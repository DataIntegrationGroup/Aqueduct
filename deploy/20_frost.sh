#!/usr/bin/env bash
# deploy/20_frost.sh
#
# Deploy FROST-Server on Cloud Run, wired to the private Cloud SQL instance
# created by 10_sql.sh.
#
#   ./deploy/20_frost.sh
#
# Security model: ingress=internal (reachable only from inside ${VPC_NAME})
# + allow-unauthenticated. FROST is isolated at the NETWORK layer — the
# public internet cannot reach it, and in-VPC callers need no token.
# Cloud Run reaches Cloud SQL's private IP over Direct VPC egress (no
# Serverless VPC connector).
#
# TODO NOTE: this deliberately does NOT wire the Dagster+ loader to FROST. Dagster+
# Serverless runs OUTSIDE the VPC and cannot reach an internal-ingress service;
# opening that path (LB + IP allowlist, or auth) is a later story. See
# deploy/README.md.
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
  --ingress=internal --allow-unauthenticated \
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
  (ingress=internal — reachable only from inside ${VPC_NAME}.)

Verify from a VM inside ${VPC_NAME} (see deploy/README.md):
  curl -s "${FROST_URL}/FROST-Server/v1.1"

The Dagster+ loader is intentionally NOT wired to this endpoint yet — later story.
For reference, the eventual loader value would be:
  service_root_url = ${FROST_URL}/FROST-Server
EOF
