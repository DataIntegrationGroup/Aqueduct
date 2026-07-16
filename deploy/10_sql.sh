#!/usr/bin/env bash
# deploy/10_sql.sh
#
# Provision a NEW dedicated Cloud SQL for PostgreSQL instance (private IP only)
# for FROST, create its database + user, and store the generated password in
# Secret Manager. Idempotent: re-running skips resources that already exist and
# never rotates the password after first provision (the deployed FROST revision
# reads the secret's :latest version only at instance startup, so rotation
# would break a running service until redeploy). To rotate deliberately:
# add a new secret version, `gcloud sql users set-password`, then re-run
# 20_frost.sh.
#
#   ./deploy/10_sql.sh
#
# Requires: roles/cloudsql.admin + roles/secretmanager.admin (or narrower).
# The instance attaches to ${VPC_NAME}'s private-services-access range, so that
# peering must already exist.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/00_config.sh"

require_apis sqladmin.googleapis.com secretmanager.googleapis.com

echo "== Cloud SQL instance ${SQL_INSTANCE} (${SQL_DB_VERSION}, private IP) =="
if gcloud sql instances describe "${SQL_INSTANCE}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "Instance ${SQL_INSTANCE} already exists — skipping create."
else
  # Private IP only. The address is auto-drawn from an existing PSA range peered
  # to ${VPC_NAME}, so no range flag is needed on the GA command. To pin a
  # specific range, switch to
  # `gcloud beta sql instances create --allocated-ip-range-name=...` — GA has no
  # such flag.
  gcloud sql instances create "${SQL_INSTANCE}" \
    --project="${PROJECT_ID}" --region="${REGION}" \
    --database-version="${SQL_DB_VERSION}" --edition="${SQL_EDITION}" --tier="${SQL_TIER}" \
    --network="projects/${PROJECT_ID}/global/networks/${VPC_NAME}" \
    --no-assign-ip \
    --storage-auto-increase
fi

echo "== Database ${SQL_DB} =="
if gcloud sql databases describe "${SQL_DB}" --instance="${SQL_INSTANCE}" \
     --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "Database ${SQL_DB} already exists — skipping."
else
  gcloud sql databases create "${SQL_DB}" \
    --instance="${SQL_INSTANCE}" --project="${PROJECT_ID}"
fi

echo "== Secret ${SECRET_FROST_DB_PW} =="
if ! gcloud secrets describe "${SECRET_FROST_DB_PW}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud secrets create "${SECRET_FROST_DB_PW}" \
    --project="${PROJECT_ID}" --replication-policy="automatic"
fi

# First-run-only password setup. If the user and an enabled secret version both
# exist, leave them alone — rotating here would strand the running FROST
# revision on the old password (it reads :latest only at instance startup).
USER_EXISTS="$(gcloud sql users list --instance="${SQL_INSTANCE}" \
  --project="${PROJECT_ID}" --filter="name=${SQL_USER}" --format='value(name)')"
SECRET_VERSION="$(gcloud secrets versions list "${SECRET_FROST_DB_PW}" \
  --project="${PROJECT_ID}" --filter="state=ENABLED" --format='value(name)' --limit=1)"

echo "== User ${SQL_USER} =="
if [[ -n "${USER_EXISTS}" && -n "${SECRET_VERSION}" ]]; then
  echo "User ${SQL_USER} and password secret already provisioned — skipping password setup."
else
  # Generate a strong password (hex — no shell/JDBC-hostile characters), add it
  # as a new secret version, and set the DB user to match. The value is never
  # echoed or written to disk.
  DB_PASSWORD="$(openssl rand -hex 24)"
  printf '%s' "${DB_PASSWORD}" | gcloud secrets versions add "${SECRET_FROST_DB_PW}" \
    --project="${PROJECT_ID}" --data-file=-
  if [[ -n "${USER_EXISTS}" ]]; then
    # User exists but the secret had no version — re-sync them.
    gcloud sql users set-password "${SQL_USER}" \
      --instance="${SQL_INSTANCE}" --project="${PROJECT_ID}" \
      --password="${DB_PASSWORD}"
  else
    gcloud sql users create "${SQL_USER}" \
      --instance="${SQL_INSTANCE}" --project="${PROJECT_ID}" \
      --password="${DB_PASSWORD}"
  fi
  unset DB_PASSWORD
fi

cat <<EOF

Cloud SQL ready.
  Instance : ${SQL_INSTANCE} (${SQL_DB_VERSION}, private IP only)
  Database : ${SQL_DB}
  User     : ${SQL_USER}  (password in Secret Manager: ${SECRET_FROST_DB_PW}:latest)

NEXT — enable PostGIS BEFORE the first FROST boot. FROST stores Location geometry
and its schema creation fails without it. The instance has no public IP, so
connect from inside ${VPC_NAME} (a VM running cloud-sql-proxy, or a bastion) and
run against database "${SQL_DB}":

    CREATE EXTENSION IF NOT EXISTS postgis;

If necessary, also enable uuid-ossp. See: https://fraunhoferiosb.github.io/FROST-Server/deployment/postgresql.html

    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

Then run: ./deploy/20_frost.sh
EOF
