#!/usr/bin/env bash
# deploy/00_config.sh
#
# Shared configuration for the private FROST/SensorThings + PostGIS provisioning
# scripts. Sourced by 10_sql.sh and 20_frost.sh — not run directly.
#
# Contains NO secrets. The Cloud SQL password lives in Secret Manager
# (${SECRET_FROST_DB_PW}); nothing in this file is sensitive.
#
# Security model: FROST runs on Cloud Run with ingress=internal, so it is
# reachable only from inside the VPC — private until V1. See deploy/README.md.

# --- Project / region -------------------------------------------------------
export PROJECT_ID="waterdatainitiative-271000"
export REGION="us-west3"
# Derived from PROJECT_ID so the two can't drift; export PROJECT_NUMBER to skip
# the lookup.
if [[ -z "${PROJECT_NUMBER:-}" ]]; then
  PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
fi
export PROJECT_NUMBER

# --- Networking -------------------------------------------------------------
# The `default` VPC, us-west3 — where the existing FROST fleet (frostproduction,
# frost1, frost-nmed, frostdev*) and the pvacd Postgres already run. `default` has
# PSA, so private-IP Cloud SQL attaches here. Cloud Run reaches Cloud SQL over
# Direct VPC egress on this network/subnet — no Serverless VPC connector needed.
export VPC_NAME="${VPC_NAME:-default}"
export SUBNET="${SUBNET:-default}"          # auto-mode default subnet in ${REGION}

# --- Cloud SQL (new dedicated instance) -------------------------------------
# -w3 = us-west3. A new name (vs the mislocated geo-prod-vpc/us-west4 instance)
# so 10_sql.sh's "already exists" guard doesn't skip creating the correct one,
# and to avoid Cloud SQL's reuse-reservation on the old name.
export SQL_INSTANCE="frost-sensorthings-w3"
# PG17 defaults to the ENTERPRISE_PLUS edition, which only accepts dedicated
# db-perf-optimized-* tiers. Pin ENTERPRISE (what this project's other PG17
# instances use) so db-custom tiers are valid.
export SQL_EDITION="${SQL_EDITION:-ENTERPRISE}"
export SQL_TIER="db-custom-1-3840"     # 1 vCPU / 3.75 GB — smallest custom size used in-project
export SQL_DB="sensorthings"
export SQL_USER="frost"
export SQL_DB_VERSION="POSTGRES_17"
# The instance's private IP is auto-drawn from an existing Private Services
# Access range on ${VPC_NAME} (google-managed-services-default-22 or -28;
# peering already established). The GA create command has no flag to pin a
# specific range — see the note in 10_sql.sh.

# --- Secret Manager ---------------------------------------------------------
export SECRET_FROST_DB_PW="frost-db-password"

# --- FROST on Cloud Run -----------------------------------------------------
export FROST_SERVICE="frost-sensorthings"
# Pinned to match the tested docker-compose.yml.
export FROST_IMAGE="docker.io/fraunhoferiosb/frost-server:2.6"

# --- Preflight helpers --------------------------------------------------------
# require_apis <api>... — fail fast with a clear message if a required Google
# API is not enabled on ${PROJECT_ID}. If the caller lacks permission to list
# services, warn and continue (the real command will surface the error anyway).
require_apis() {
  local enabled missing="" api
  if ! enabled="$(gcloud services list --enabled --project="${PROJECT_ID}" \
      --format='value(config.name)' 2>/dev/null)"; then
    echo "WARN: cannot list enabled APIs (missing serviceusage.services.list?) — skipping preflight." >&2
    return 0
  fi
  for api in "$@"; do
    grep -qx "${api}" <<<"${enabled}" || missing="${missing} ${api}"
  done
  if [[ -n "${missing}" ]]; then
    echo "ERROR: required APIs not enabled on ${PROJECT_ID}:${missing}" >&2
    echo "Enable with: gcloud services enable${missing} --project=${PROJECT_ID}" >&2
    exit 1
  fi
}
