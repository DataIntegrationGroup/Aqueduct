#!/usr/bin/env bash
# deploy/20_frost.sh
#
# Deploy FROST-Server on Cloud Run, wired to the private Cloud SQL instance
# created by 10_sql.sh.
#
#   ./deploy/20_frost.sh
#
# Security model: ingress=all + --no-allow-unauthenticated. FROST is isolated at
# the IAM layer — the endpoint is routable, but Cloud Run rejects every request
# that does not carry a Google-signed ID token from a principal holding
# roles/run.invoker. Only ${DAGSTER_SA} holds it (granted in 30_dagster_gcp_auth.sh).
#
# Cloud Run reaches Cloud SQL's private IP over Direct VPC egress, and the
# database itself is private-IP only.
#
# Requires: roles/run.admin, roles/iam.serviceAccountUser on the Cloud Run
# runtime service account, and permission to bind secretAccessor.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/00_config.sh"

require_apis run.googleapis.com secretmanager.googleapis.com

# The instance was created --no-assign-ip, so its only address should be the
# private one — but assert the type rather than trust the ordering, so a
# later-added public IP can't get wired into the JDBC URL silently.
IFS=$'\t' read -r SQL_IP_TYPE SQL_PRIVATE_IP < <(gcloud sql instances describe \
  "${SQL_INSTANCE}" --project="${PROJECT_ID}" \
  --format='value(ipAddresses[0].type, ipAddresses[0].ipAddress)')
if [[ "${SQL_IP_TYPE}" != "PRIVATE" || -z "${SQL_PRIVATE_IP}" ]]; then
  echo "ERROR: expected a PRIVATE Cloud SQL address on ${SQL_INSTANCE}," \
    "got type='${SQL_IP_TYPE:-none}' ip='${SQL_PRIVATE_IP:-none}'." >&2
  exit 1
fi
echo "Cloud SQL private IP: ${SQL_PRIVATE_IP}"

# The Cloud Run runtime service account must be able to read the DB password
# secret for --set-secrets to mount it.
RUNTIME_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
echo "== Grant ${RUNTIME_SA} secretAccessor on ${SECRET_FROST_DB_PW} =="
gcloud secrets add-iam-policy-binding "${SECRET_FROST_DB_PW}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/secretmanager.secretAccessor" >/dev/null

# '@'-delimited so values containing ':' and '/' (the JDBC URL) survive parsing.
# Keys mirror docker-compose.yml (already FROST v2.x-compatible).
#
# WARNING: this list is applied below with --set-env-vars, which REPLACES the
# service's entire environment rather than merging into it. Anything added by hand
# in the console is silently dropped on the next run, and persistence_db_url is
# repointed at ${SQL_INSTANCE} regardless of what the service currently uses. This
# file is therefore the single source of truth for FROST's configuration — add new
# settings here, never in the console. Before re-running against a service you did
# not deploy yourself, diff the two:
#
#   gcloud run services describe ${FROST_SERVICE} --project=${PROJECT_ID} \
#     --region=${REGION} --format='yaml(spec.template.spec.containers[0].env)'
#
# (serviceRootUrl is exempt — pass 2 re-adds it below. The DB password is mounted
# via --set-secrets, which is a separate flag and survives untouched.)
ENV_VARS="^@^persistence_db_driver=org.postgresql.Driver"
ENV_VARS="${ENV_VARS}@persistence_db_url=jdbc:postgresql://${SQL_PRIVATE_IP}:5432/${SQL_DB}"
ENV_VARS="${ENV_VARS}@persistence_db_username=${SQL_USER}"
ENV_VARS="${ENV_VARS}@persistence_autoUpdateDatabase=true"
ENV_VARS="${ENV_VARS}@defaultTop=1000"
ENV_VARS="${ENV_VARS}@maxTop=10000"
# Cloud Run only exposes port 8080, so the embedded MQTT broker (default on,
# port 1883) would just be an unreachable background thread — turn it off.
ENV_VARS="${ENV_VARS}@mqtt_Enabled=false"
ENV_VARS="${ENV_VARS}@plugins_modelLoader_enable=true"
ENV_VARS="${ENV_VARS}@plugins_multiDatastream_enable=false"
ENV_VARS="${ENV_VARS}@plugins_actuation_enable=false"
ENV_VARS="${ENV_VARS}@http_cors_enable=true"
ENV_VARS="${ENV_VARS}@http_cors_allowed_origins=*"

# Record what is serving now, so a bad deploy has an obvious way back. Empty on a
# first-ever deploy, which is fine — there is nothing to roll back to.
PREV_REVISION="$(gcloud run services describe "${FROST_SERVICE}" \
  --project="${PROJECT_ID}" --region="${REGION}" \
  --format='value(status.latestReadyRevisionName)' 2>/dev/null || true)"
[[ -n "${PREV_REVISION}" ]] && echo "Current revision (rollback target): ${PREV_REVISION}"

echo "== Deploy FROST (pass 1: bring it up) =="
# --max-instances=1: FROST is a single-writer JVM; capping at 1 avoids needing a
# shared MQTT bus for multi-instance clustering. --min-instances=1 keeps cold
# starts off the request path.
# --no-cpu-throttling (CPU always allocated): FROST opens port 8080 — passing the
# startup probe — BEFORE it finishes building its schema (Liquibase) in a
# post-deploy background thread. With default throttling, Cloud Run cuts CPU once
# the probe passes and that init freezes, so the schema is never created. Always-
# allocated CPU lets initialization run to completion.
gcloud run deploy "${FROST_SERVICE}" \
  --project="${PROJECT_ID}" --region="${REGION}" \
  --image="${FROST_IMAGE}" --port=8080 \
  --ingress=all --no-allow-unauthenticated \
  --network="${VPC_NAME}" --subnet="${SUBNET}" --vpc-egress=private-ranges-only \
  --min-instances=1 --max-instances=1 --memory=1Gi --cpu=1 --timeout=300 \
  --no-cpu-throttling \
  --set-secrets="persistence_db_password=${SECRET_FROST_DB_PW}:latest" \
  --set-env-vars="${ENV_VARS}"

FROST_URL="$(gcloud run services describe "${FROST_SERVICE}" \
  --project="${PROJECT_ID}" --region="${REGION}" --format='value(status.url)')"
echo "FROST URL: ${FROST_URL}"

echo "== Deploy FROST (pass 2: set serviceRootUrl to its own URL) =="
# FROST needs its own root for the self-links it emits in responses.
gcloud run services update "${FROST_SERVICE}" \
  --project="${PROJECT_ID}" --region="${REGION}" \
  --update-env-vars="serviceRootUrl=${FROST_URL}/FROST-Server"

cat <<EOF

FROST deployed.
  Service root : ${FROST_URL}/FROST-Server
  v1.1 API     : ${FROST_URL}/FROST-Server/v1.1
  (ingress=all, IAM-gated — every request needs an ID token with run.invoker.)

Verify from anywhere (now accessible from outside the GCP network). Only principals holding
run.invoker can call it (e.g. the Dagster service account), NOT a personal account. A personal
account token should be refused:

  # Expect 403 (no credentials) and 403 again (personal account should lack run.invoker):
  curl -si ${FROST_URL}/FROST-Server/v1.1 | head -1
  curl -si -H "Authorization: Bearer \$(gcloud auth print-identity-token)" \\
    ${FROST_URL}/FROST-Server/v1.1 | head -1

  # Expect 200 — impersonating the SA that actually holds run.invoker.
  # Needs roles/iam.serviceAccountTokenCreator on \${DAGSTER_SA}.
  # --include-email is REQUIRED; without it Cloud Run rejects the token.
  TOKEN="\$(gcloud auth print-identity-token --impersonate-service-account=${DAGSTER_SA} \\
    --audiences=${FROST_URL} --include-email)"
  curl -si -H "Authorization: Bearer \${TOKEN}" ${FROST_URL}/FROST-Server/v1.1 | head -1

Grant the Dagster service account run.invoker (idempotent, safe to re-run):
  ./deploy/30_dagster_gcp_auth.sh

Then set in Dagster+ → Deployment → Environment variables (Full deployment only):
  FROST_SERVICE_ROOT_URL = ${FROST_URL}/FROST-Server
$([[ -n "${PREV_REVISION}" ]] && printf '\nRoll back if this revision misbehaves:\n  gcloud run services update-traffic %s \\\n    --project=%s --region=%s --to-revisions=%s=100\n' \
  "${FROST_SERVICE}" "${PROJECT_ID}" "${REGION}" "${PREV_REVISION}")
EOF
