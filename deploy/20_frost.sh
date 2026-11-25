#!/usr/bin/env bash
# deploy/20_frost.sh
#
# Deploy FROST-Server on Cloud Run, wired to the private Cloud SQL instance
# created by 10_sql.sh.
#
#   ./deploy/20_frost.sh
#
# Security model: FROST's own BasicAuth with anonymous reads. GETs need no
# credentials; create/update/delete need a FROST user with those roles — in
# practice only ${FROST_WRITER_USER}, whose login is in ${SECRET_FROST_WRITER} and
# readable only by ${DAGSTER_SA}. Users live in FROST's USERS / USER_ROLES tables
# in the same database and are managed by hand (deploy/README.md, "FROST users").
#
# FROST_PUBLIC (00_config.sh) decides whether Cloud Run's IAM gate also sits in
# front: false keeps --no-allow-unauthenticated (only run.invoker holders reach
# FROST at all), true deploys --allow-unauthenticated. Keep it false until FROST's
# default admin/admin, write/write, read/read users have been removed.
#
# Cloud Run reaches Cloud SQL's private IP over Direct VPC egress, and the
# database itself is private-IP only.
#
# Requires: roles/run.admin, roles/iam.serviceAccountUser on the Cloud Run
# runtime service account, roles/secretmanager.admin (or narrower: create the
# writer secret, add versions, bind secretAccessor).

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/00_config.sh"

require_apis run.googleapis.com secretmanager.googleapis.com

case "${FROST_PUBLIC}" in
  true)
    INVOKER_FLAG="--allow-unauthenticated"
    IAM_FLAG="--no-invoker-iam-check"
    ;;
  false)
    INVOKER_FLAG="--no-allow-unauthenticated"
    IAM_FLAG="--invoker-iam-check"
    ;;
  *) echo "ERROR: FROST_PUBLIC must be 'true' or 'false', got '${FROST_PUBLIC}'." >&2; exit 2 ;;
esac

# If FROST_PUBLIC is true, then default users must not exist in an existing deployment.
# Check that each default user returns a 401 when attempting to authenticate against the deployment,
# if does not return a 401 then default user still exists, throw an error and exit.
# Reference "FROST Users" section of deploy/README.md
if [[ "${FROST_PUBLIC}" == "true" ]]; then
  FROST_URL="$(gcloud run services describe "${FROST_SERVICE}" --project="${PROJECT_ID}" --region="${REGION}" \
     --format='value(status.url)')"
  THINGS="${FROST_URL}/FROST-Server/v1.1/Things"
  admin_status_code=$(curl -w "%{http_code}" -si -u admin:admin -X POST -H 'Content-Type: application/json' -d '{}' \
    ${THINGS} | head -1)
  if [[ "$admin_status_code" -ne 401 ]]; then
    echo "ERROR: FROST_PUBLIC is true but default admin user exists in FROST Server, make sure to delete default users"
    exit 1
  fi
  read_status_code=$(curl -w "%{http_code}" -si -u read:read -X POST -H 'Content-Type: application/json' -d '{}' \
    ${THINGS} | head -1)
  if [[ "$read_status_code" -ne 401 ]]; then
    echo "ERROR: FROST_PUBLIC is true but default read user exists in FROST Server, make sure to delete default users"
    exit 1
  fi
  write_status_code=$(curl -w "%{http_code}" -si -u write:write -X POST -H 'Content-Type: application/json' -d '{}' \
    ${THINGS} | head -1)
  if [[ "$write_status_code" -ne 401 ]]; then
    echo "ERROR: FROST_PUBLIC is true but default write user exists in FROST Server, make sure to delete default users"
    exit 1
  fi
fi

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

# The login the loader writes with. FROST itself never reads this secret — it
# checks credentials against its USERS table — so the runtime SA gets no access;
# only ${DAGSTER_SA} does (30_dagster_gcp_auth.sh). First-run-only, like the DB
# password in 10_sql.sh: re-runs never rotate it, because the hash in USERS would
# no longer match. To rotate deliberately: add a new version, then update the
# user's row (deploy/README.md, "FROST users").
echo "== Secret ${SECRET_FROST_WRITER} (FROST writer login) =="
if ! gcloud secrets describe "${SECRET_FROST_WRITER}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud secrets create "${SECRET_FROST_WRITER}" \
    --project="${PROJECT_ID}" --replication-policy="automatic"
fi
WRITER_VERSION="$(gcloud secrets versions list "${SECRET_FROST_WRITER}" \
  --project="${PROJECT_ID}" --filter="state=ENABLED" --format='value(name)' --limit=1)"
if [[ -n "${WRITER_VERSION}" ]]; then
  echo "   already has an enabled version — leaving it alone."
else
  # Hex — no JSON-, shell-, or Basic-auth-hostile characters (no ':'). Assigned
  # first so an openssl failure aborts under set -e instead of storing an empty
  # password. printf is a builtin, so the value never appears in a process
  # listing; never echoed or written to disk.
  WRITER_PASSWORD="$(openssl rand -hex 24)"
  printf '{"username":"%s","password":"%s"}' "${FROST_WRITER_USER}" "${WRITER_PASSWORD}" \
    | gcloud secrets versions add "${SECRET_FROST_WRITER}" \
        --project="${PROJECT_ID}" --data-file=-
  unset WRITER_PASSWORD
  echo "   generated. ${FROST_WRITER_USER} does not exist in FROST yet — add it (deploy/README.md)."
fi

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
# via --set-secrets, which is a separate flag and REPLACES the whole secret list.)
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
# BasicAuth with anonymous reads. autoUpdateDatabase creates the USERS /
# USER_ROLES tables (and the default admin/write/read users) on the first
# authentication attempt, not at boot.
# plainTextPassword=false: USERS holds bcrypt hashes, checked with pgcrypto's
# crypt() inside Postgres — so `CREATE EXTENSION pgcrypto` must exist in ${SQL_DB}
# or every authenticated request errors (see 10_sql.sh / deploy/README.md).
# The auth layer opens its own connection pool: auth_db_* is separate from
# persistence_db_* and does not fall back to it (password via --set-secrets below).
ENV_VARS="${ENV_VARS}@auth_provider=de.fraunhofer.iosb.ilt.frostserver.auth.basic.BasicAuthProvider"
ENV_VARS="${ENV_VARS}@auth_autoUpdateDatabase=true"
ENV_VARS="${ENV_VARS}@auth_allowAnonymousRead=true"
ENV_VARS="${ENV_VARS}@auth_plainTextPassword=false"
ENV_VARS="${ENV_VARS}@auth_db_driver=org.postgresql.Driver"
ENV_VARS="${ENV_VARS}@auth_db_url=jdbc:postgresql://${SQL_PRIVATE_IP}:5432/${SQL_DB}"
ENV_VARS="${ENV_VARS}@auth_db_username=${SQL_USER}"

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
# ${INVOKER_FLAG} comes from FROST_PUBLIC
echo "   FROST_PUBLIC=${FROST_PUBLIC} → ${INVOKER_FLAG}"
gcloud run deploy "${FROST_SERVICE}" \
  --project="${PROJECT_ID}" --region="${REGION}" \
  --image="${FROST_IMAGE}" --port=8080 \
  --ingress=all "${INVOKER_FLAG}" "${IAM_FLAG}" \
  --network="${VPC_NAME}" --subnet="${SUBNET}" --vpc-egress=private-ranges-only \
  --min-instances=1 --max-instances=1 --memory=1Gi --cpu=1 --timeout=300 \
  --no-cpu-throttling \
  --set-secrets="persistence_db_password=${SECRET_FROST_DB_PW}:latest,auth_db_password=${SECRET_FROST_DB_PW}:latest" \
  --set-env-vars="${ENV_VARS}"

FROST_URL="$(gcloud run services describe "${FROST_SERVICE}" \
  --project="${PROJECT_ID}" --region="${REGION}" --format='value(status.url)')"
echo "FROST URL: ${FROST_URL}"

echo "== Deploy FROST (pass 2: set serviceRootUrl to its own URL) =="
# FROST needs its own root for the self-links it emits in responses.
gcloud run services update "${FROST_SERVICE}" \
  --project="${PROJECT_ID}" --region="${REGION}" \
  --update-env-vars="serviceRootUrl=${FROST_URL}/FROST-Server"

THINGS="${FROST_URL}/FROST-Server/v1.1/Things"
if [[ "${FROST_PUBLIC}" == "true" ]]; then
  cat <<EOF

FROST deployed — PUBLIC
  Service root : ${FROST_URL}/FROST-Server
  v1.1 API     : ${FROST_URL}/FROST-Server/v1.1
  (ingress=all, no IAM gate. FROST BasicAuth: anonymous reads; writes need ${FROST_WRITER_USER}.)

Verify from anywhere, with no credentials at all:

  # Expect 200 — anonymous read:
  curl -si ${THINGS} | head -1
  # Expect 401 — anonymous write:
  curl -si -X POST -H 'Content-Type: application/json' -d '{}' ${THINGS} | head -1
  # Expect 401 — FROST's default users must be gone:
  curl -si -u admin:admin -X POST -H 'Content-Type: application/json' -d '{}' ${THINGS} | head -1
  curl -si -u read:read -X POST -H 'Content-Type: application/json' -d '{}' ${THINGS} | head -1
  curl -si -u write:write -X POST -H 'Content-Type: application/json' -d '{}' ${THINGS} | head -1

EOF
else
  cat <<EOF

FROST deployed — PRIVATE
  Service root : ${FROST_URL}/FROST-Server
  v1.1 API     : ${FROST_URL}/FROST-Server/v1.1
  (ingress=all, IAM-gated — every request needs an ID token with run.invoker.
   Behind the gate, FROST BasicAuth: anonymous reads; writes need ${FROST_WRITER_USER}.)

Verify by impersonating the SA that holds run.invoker. The token goes in X-Serverless-Authorization,
as the loader sends it, leaving Authorization free for FROST's BasicAuth:

  TOKEN="\$(gcloud auth print-identity-token --impersonate-service-account=${DAGSTER_SA} \\
    --audiences=${FROST_URL} --include-email)"

  # Expect 403 — no token, the IAM gate is working:
  curl -si ${THINGS} | head -1
  # Expect 200 — token but no FROST login, an anonymous read:
  curl -si -H "X-Serverless-Authorization: Bearer \${TOKEN}" ${THINGS} | head -1
  # Expect 401 — a login attempt. The first one also creates FROST's user tables, so run
  # this once before adding the writer (pgcrypto must already exist):
  curl -si -H "X-Serverless-Authorization: Bearer \${TOKEN}" -u probe:probe \\
    -X POST -H 'Content-Type: application/json' -d '{}' ${THINGS} | head -1

Then add ${FROST_WRITER_USER} and remove FROST's default users: deploy/README.md, "FROST users".
Only after that: set FROST_PUBLIC=true in 00_config.sh and re-run this script.
EOF
fi

cat <<EOF

Grant the Dagster service account run.invoker and read access on ${SECRET_FROST_WRITER}
(idempotent, safe to re-run):
  ./deploy/30_dagster_gcp_auth.sh

Then set in Dagster+ → Deployment → Environment variables (Full deployment only):
  FROST_SERVICE_ROOT_URL = ${FROST_URL}/FROST-Server
$([[ -n "${PREV_REVISION}" ]] && printf '\nRoll back if this revision misbehaves:\n  gcloud run services update-traffic %s \\\n    --project=%s --region=%s --to-revisions=%s=100\n' \
  "${FROST_SERVICE}" "${PROJECT_ID}" "${REGION}" "${PREV_REVISION}")
EOF
