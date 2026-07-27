# deploy/ — private FROST/SensorThings + PostGIS provisioning

Scripts to stand up a **private** FROST-Server v1.1 endpoint backed by PostGIS in
GCP, not reachable from the public internet until V1.

- **FROST** → Cloud Run, `ingress=internal` (in-VPC only), image pinned to
  `fraunhoferiosb/frost-server:2.6` (matches the repo's `docker-compose.yml`).
  Reaches Cloud SQL over **Direct VPC egress** (no Serverless VPC connector).
- **PostGIS** → a new dedicated **Cloud SQL for PostgreSQL** instance, private IP
  only, on the **`default`** VPC in **us-west3**
- **Project:** `waterdatainitiative-271000` · **Region:** `us-west3` ·
  **VPC/subnet:** `default` / `default` (auto-mode).

The Dagster+ loader is **not** wired to this endpoint here — Dagster+ Serverless
runs outside the VPC and can't reach an internal-ingress service. Connecting it
(LB + IP allowlist, or auth) is a separate, later story.

## Files

| File | Role |
|------|------|
| `00_config.sh` | Shared variables (no secrets). Sourced by the others. |
| `10_sql.sh` | Create the dedicated Cloud SQL instance + DB + user; store the password in Secret Manager. |
| `20_frost.sh` | Deploy FROST on Cloud Run, wired to that instance's private IP via Direct VPC egress. |

## Prerequisites (admin-owned — "PM provisions")

Networking is already in place: the `default` VPC has Private Services Access
(`servicenetworking-googleapis-com`, ranges `google-managed-services-default-22`
and `-28`), so a new private-IP Cloud SQL instance attaches with no extra setup.
What an admin still needs to grant the deploy identity:

1. `roles/cloudsql.admin`, `roles/secretmanager.admin`, `roles/run.admin`, and
   `roles/iam.serviceAccountUser` on the Cloud Run runtime service account
   (`95715287188-compute@developer.gserviceaccount.com`).
2. `roles/compute.networkUser` on subnet `default` (us-west3) — required for Cloud
   Run **Direct VPC egress** to use the subnet. The Cloud Run Service Agent
   (`service-95715287188@serverless-robot-prod.iam.gserviceaccount.com`) needs the
   same on the subnet.

No Serverless VPC connector is needed (Direct VPC egress), and no PSA setup is
needed (it already exists).

## Run order

```bash
# 1. Cloud SQL instance + DB + user + password secret
./deploy/10_sql.sh

# 2. Enable PostGIS BEFORE first FROST boot (private IP → connect from in-VPC).
#    From a VM in the default VPC's us-west3 subnet running cloud-sql-proxy, or a bastion:
#      psql "host=<sql-private-ip> dbname=sensorthings user=frost" \
#        -c "CREATE EXTENSION IF NOT EXISTS postgis;" \
#        -c 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'   # optional, per FROST docs

# 3. FROST on Cloud Run
./deploy/20_frost.sh
```

Every step is safe to re-run: `10_sql.sh` skips existing resources and sets the
DB password only on first provision (re-runs never rotate it — the running FROST
revision reads the secret's `:latest` version only at instance startup), the
`psql` step uses `IF NOT EXISTS`, and `20_frost.sh` just deploys a new Cloud Run
revision. To rotate the password deliberately: add a new secret version, run
`gcloud sql users set-password`, then re-run `20_frost.sh`.

## Verify (satisfies the acceptance criterion)

`ingress=internal` means you **cannot** curl from a laptop — that's the point.
Verify from **inside the `default` VPC** (us-west3 subnet):

```bash
# One-off e2-micro VM in the default subnet (Private Google Access on):
gcloud compute instances create frost-verify \
  --project=waterdatainitiative-271000 --zone=us-west3-a \
  --machine-type=e2-micro --network=default --subnet=default \
  --no-address

# From the VM — expect the SensorThings service document (Things, Locations,
# Datastreams, Observations, ...):
gcloud compute ssh frost-verify --project=waterdatainitiative-271000 --zone=us-west3-a \
--command='curl -sS -m 15 -w "\nHTTP %{http_code}\n" https://<frost-run-url>/FROST-Server/v1.1'

gcloud compute ssh frost-verify --project=waterdatainitiative-271000 --zone=us-west3-a \
--command='curl -sS -m 15 -w "\nHTTP %{http_code}\n" https://<frost-run-url>/FROST-Server/v1.1/Things'

# Tear down:
gcloud compute instances delete frost-verify --zone=us-west3-a --quiet
```

A 200 with the entity-set list confirms "a private SensorThings v1.1 endpoint
responds to queries" — while proving it is not reachable from the public internet
(a `curl` from a laptop to the run.app URL should be refused).

## When Dagster+ connects (later story)

- Move ingress to `internal-and-cloud-load-balancing` + Cloud Armor allowlisting
  Dagster+ Serverless's static egress IPs, **or** add auth (Cloud Run IAM/OIDC or
  FROST BasicAuth — the latter needs a loader change).
- Make the FROST URL configurable per-deployment. Today
  `src/aqueduct_dagster/defs/assets/load.py` reads `service_root_url` from the
  committed `.dlt/config.toml` (hardcoded to localhost); add an env-var override
  so the prod deployment can point at this endpoint.
