#!/usr/bin/env bash
# deploy/30_dagster_gcp_auth.sh
#
# Provision the GCP side of Dagster+ authentication to bucket storage.
#
#   ./deploy/30_dagster_gcp_auth.sh              # bucket + SA + IAM (safe, idempotent)
#   ./deploy/30_dagster_gcp_auth.sh --emit-key   # ...and mint a NEW key, printing base64
#
# Why a key at all: Dagster+ Serverless runs outside GCP with no metadata server and
# no way to mount a credentials file, so Application Default Credentials have nothing
# to discover. The documented approach is a base64-encoded service account key in an
# environment variable, which src/aqueduct_dagster/shared/gcp_auth.py decodes into a
# temp file and points GOOGLE_APPLICATION_CREDENTIALS at. One key therefore covers
# GCS, Secret Manager, and dlt, since all three resolve through ADC.
#
# Key creation is behind --emit-key: this script is meant to be re-run
# freely to reconcile IAM, and GCP caps user-managed keys at 10 per service account.
# The key is printed to stdout once and never written to a file.
#
# Requires: roles/storage.admin (bucket create + IAM), roles/iam.serviceAccountAdmin
# (only if the SA does not exist yet), roles/iam.serviceAccountKeyAdmin (only with
# --emit-key), and permission to bind secretAccessor on ${SECRET_HYDROVU}.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/00_config.sh"

EMIT_KEY=false
for arg in "$@"; do
  case "${arg}" in
    --emit-key) EMIT_KEY=true ;;
    *) echo "ERROR: unknown argument '${arg}' (expected --emit-key or nothing)." >&2; exit 2 ;;
  esac
done

require_apis storage.googleapis.com iam.googleapis.com secretmanager.googleapis.com

# Check if a service account approach is possible before doing any work
if [[ "${EMIT_KEY}" == true ]]; then
  if policy="$(gcloud resource-manager org-policies describe \
      constraints/iam.disableServiceAccountKeyCreation \
      --project="${PROJECT_ID}" --format='value(booleanPolicy.enforced)' 2>/dev/null)"; then
    if [[ "${policy}" == "True" ]]; then
      echo "ERROR: constraints/iam.disableServiceAccountKeyCreation is enforced on" >&2
      echo "       ${PROJECT_ID}. Service account keys cannot be created, so the" >&2
      echo "       env-var key approach will not work. See deploy/README.md." >&2
      exit 1
    fi
  else
    echo "WARN: cannot read the SA-key org policy — continuing; key creation will" >&2
    echo "      fail below if it is enforced." >&2
  fi
fi

# --- Production bucket ------------------------------------------------------
# Uniform bucket-level access so the IAM bindings below are the only access
# control in play.
echo "== Bucket gs://${BUCKET_PROD} =="
if gcloud storage buckets describe "gs://${BUCKET_PROD}" --project="${PROJECT_ID}" \
    --format='value(name)' >/dev/null 2>&1; then
  echo "   already exists — skipping create."
# Bucket names are a single global namespace across all of Google Cloud, so a create
# can fail with 409 because someone in another project holds the name. That is not a
# reason to abort: nothing in the current pipeline writes to BUCKET_PROD
# (.dlt/config.toml still points at the POC bucket), so warn and let the run finish
# provisioning the parts that are actually in use.
elif ! create_err="$(gcloud storage buckets create "gs://${BUCKET_PROD}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access \
    --public-access-prevention 2>&1)"; then
  if [[ "${create_err}" == *409* || "${create_err}" == *"not available"* ]]; then
    echo "   WARN: the name gs://${BUCKET_PROD} is taken and not visible to this" >&2
    echo "         project — it belongs to another project or organization." >&2
    echo "         Continuing; A production bucket still needs to be created" >&2
    echo "         before switching from ${BUCKET_POC}." >&2
  else
    echo "${create_err}" >&2
    exit 1
  fi
else
  echo "   created in ${REGION}."
fi

# --- Service account --------------------------------------------------------
echo "== Service account ${DAGSTER_SA} =="
if gcloud iam service-accounts describe "${DAGSTER_SA}" --project="${PROJECT_ID}" \
    --format='value(email)' >/dev/null 2>&1; then
  echo "   already exists — skipping create."
else
  gcloud iam service-accounts create "${DAGSTER_SA_NAME}" \
    --project="${PROJECT_ID}" \
    --display-name="Aqueduct dlt writer" \
    --description="Dagster+ Serverless identity for GCS raw parquet writes and Secret Manager reads"
  echo "   created."
fi

# --- IAM --------------------------------------------------------------------
# Bucket-scoped rather than project-wide, mirroring how 20_frost.sh scopes its one
# binding to a single secret. objectAdmin and not objectCreator: dlt creates
# objects, the transform assets list and read them, and the watermark JSON is
# overwritten in place — objectCreator alone cannot do the last two.
for bucket in "${BUCKET_PROD}" "${BUCKET_POC}"; do
  echo "== Grant objectAdmin on gs://${bucket} =="
  if ! gcloud storage buckets describe "gs://${bucket}" --project="${PROJECT_ID}" \
      --format='value(name)' >/dev/null 2>&1; then
    echo "   WARN: gs://${bucket} does not exist — skipping." >&2
    continue
  fi
  gcloud storage buckets add-iam-policy-binding "gs://${bucket}" \
    --project="${PROJECT_ID}" \
    --member="serviceAccount:${DAGSTER_SA}" \
    --role="roles/storage.objectAdmin" >/dev/null
  echo "   bound."
done

# The HydroVu OAuth credentials are themselves fetched through ADC at ingest time,
# so bucket access alone is not enough to run the pipeline.
echo "== Grant secretAccessor on ${SECRET_HYDROVU} =="
gcloud secrets add-iam-policy-binding "${SECRET_HYDROVU}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${DAGSTER_SA}" \
  --role="roles/secretmanager.secretAccessor" >/dev/null
echo "   bound."

# --- Key --------------------------------------------------------------------
if [[ "${EMIT_KEY}" == true ]]; then
  echo
  echo "== Minting a NEW user-managed key for ${DAGSTER_SA} =="
  existing="$(gcloud iam service-accounts keys list --iam-account="${DAGSTER_SA}" \
    --project="${PROJECT_ID}" --managed-by=user --format='value(name)' 2>/dev/null | wc -l | tr -d ' ')"
  echo "   existing user-managed keys: ${existing} (GCP allows 10)"

  KEY_TMP="$(mktemp)"
  # Delete the plaintext key on any exit path, including failure.
  trap 'rm -f "${KEY_TMP}"' EXIT
  gcloud iam service-accounts keys create "${KEY_TMP}" \
    --iam-account="${DAGSTER_SA}" --project="${PROJECT_ID}" >/dev/null

  echo
  echo "--- BEGIN GCP_SERVICE_ACCOUNT_KEY_B64 (copy the single line below) ---"
  base64 -w0 < "${KEY_TMP}" 2>/dev/null || base64 < "${KEY_TMP}" | tr -d '\n'
  echo
  echo "--- END GCP_SERVICE_ACCOUNT_KEY_B64 ---"
  rm -f "${KEY_TMP}"
  trap - EXIT

  cat <<EOF

Paste that value into Dagster+ → Deployment → Environment variables:
  Name       : GCP_SERVICE_ACCOUNT_KEY_B64
  Scope      : Full deployment AND Branch deployments
  Code loc.  : aqueduct_dagster_defs_definitions

Do NOT save it to a file or commit it. If you lose it, re-run with --emit-key and
delete the stale key:
  gcloud iam service-accounts keys list --iam-account=${DAGSTER_SA} --managed-by=user
  gcloud iam service-accounts keys delete <KEY_ID> --iam-account=${DAGSTER_SA}
EOF
fi

cat <<EOF

Done. Service account : ${DAGSTER_SA}
      Buckets         : gs://${BUCKET_PROD} (prod), gs://${BUCKET_POC} (poc/verify)
      Secret          : ${SECRET_HYDROVU}

Dagster+ → Deployment → Environment variables (code location aqueduct_dagster_defs_definitions):
  GCP_SERVICE_ACCOUNT_KEY_B64  = <the blob from --emit-key>   scope: Full + Branch deployments

Verify without minting a key (needs roles/iam.serviceAccountTokenCreator on the SA):
  gcloud storage ls gs://${BUCKET_POC} --impersonate-service-account=${DAGSTER_SA}
  echo probe | gcloud storage cp - gs://${BUCKET_POC}/_adc_probe.txt \\
    --impersonate-service-account=${DAGSTER_SA}
  gcloud storage rm gs://${BUCKET_POC}/_adc_probe.txt \\
    --impersonate-service-account=${DAGSTER_SA}

Confirm the bindings landed:
  gcloud storage buckets get-iam-policy gs://${BUCKET_PROD} --format=json
  gcloud secrets get-iam-policy ${SECRET_HYDROVU} --project=${PROJECT_ID}
EOF
