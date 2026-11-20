# deploy/ — FROST/SensorThings + PostGIS provisioning

Scripts to stand up a FROST-Server v1.1 endpoint backed by PostGIS in GCP: anyone
can read it, only the Dagster pipeline can write to it.

- **FROST** → Cloud Run, `ingress=all`, image pinned to
  `fraunhoferiosb/frost-server:2.6` (matches the repo's `docker-compose.yml`).
  Reaches Cloud SQL over **Direct VPC egress** (no Serverless VPC connector).
  FROST's own **BasicAuth** allows anonymous reads; writes need the `aqueduct_writer`
  login (secret `frost_writer`, readable only by the Dagster SA). `FROST_PUBLIC` in
  `00_config.sh` decides whether Cloud Run's IAM gate also sits in front.
- **PostGIS** → a new dedicated **Cloud SQL for PostgreSQL** instance, private IP
  only, on the **`default`** VPC in **us-west3**
- **Project:** `waterdatainitiative-271000` · **Region:** `us-west3` ·
  **VPC/subnet:** `default` / `default` (auto-mode).


## Files

| File | Role |
|------|------|
| `00_config.sh` | Shared variables (no secrets). Sourced by the others. |
| `10_sql.sh` | Create the dedicated Cloud SQL instance + DB + user; store the password in Secret Manager. |
| `20_frost.sh` | Deploy FROST on Cloud Run, wired to that instance's private IP via Direct VPC egress, with BasicAuth on; generate the `frost_writer` login secret on first run. |
| `30_dagster_gcp_auth.sh` | Bucket + service account + IAM so Dagster+ can authenticate to GCS, Secret Manager, and FROST via ADC. |
| `31_verify_dagster_auth.py` | Verify a minted key end to end, locally, before pasting it into Dagster+. |

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

# 2. Enable extensions BEFORE first FROST boot (see "Running SQL" below):
#      CREATE EXTENSION IF NOT EXISTS postgis;
#      CREATE EXTENSION IF NOT EXISTS pgcrypto;      -- BasicAuth's hashed passwords
#      CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- optional, per FROST docs

# 3. FROST on Cloud Run (FROST_PUBLIC=false) + the frost_writer secret
./deploy/20_frost.sh

# 4. Dagster+ authentication. Run AFTER step 3: the run.invoker grant and the
#    frost_writer secret need it (it warns and skips them otherwise).
./deploy/30_dagster_gcp_auth.sh              # bucket + SA + IAM, safe to re-run
./deploy/30_dagster_gcp_auth.sh --emit-key   # ...and mint a key to paste into Dagster+

# 5. Add the writer user, delete FROST's defaults — see "FROST users" below.

# 6. Verify the key before pasting it into Dagster+
export GCP_SERVICE_ACCOUNT_KEY_B64=<blob from step 4>
export FROST_SERVICE_ROOT_URL=https://<frost-run-url>/FROST-Server
unset GOOGLE_APPLICATION_CREDENTIALS
uv run python deploy/31_verify_dagster_auth.py

# 7. Only after step 5: set FROST_PUBLIC=true in 00_config.sh, re-run 20_frost.sh,
#    and commit it. 20_frost.sh prints the checks for each mode.
```

Every step is safe to re-run: `10_sql.sh` skips existing resources and sets the
DB password only on first provision (re-runs never rotate it — the running FROST
revision reads the secret's `:latest` version only at instance startup), the
extension step uses `IF NOT EXISTS`, and `20_frost.sh` just deploys a new Cloud Run
revision (it generates `frost_writer` only once). To rotate the DB password
deliberately: add a new secret version, run `gcloud sql users set-password`, then
re-run `20_frost.sh`.

## Verify (while `FROST_PUBLIC=false`)

**Expect 403 for both of these.** Only `aqueduct-dlt-writer` holds `run.invoker`; the
policy deliberately contains no human members, so your own token is refused exactly
like an anonymous one. Two 403s here is the gate working, not a misconfiguration:

```bash
ORIGIN=https://<frost-run-url>
FROST=$ORIGIN/FROST-Server

curl -si "$FROST/v1.1" | head -1                                                  # anonymous
curl -si -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "$FROST/v1.1" | head -1                                                         # you
```

**Expect 200 from the service account** — the identity that actually holds the role,
and the one Dagster+ uses. Needs `roles/iam.serviceAccountTokenCreator` on the SA. The
token goes in `X-Serverless-Authorization`, as the loader sends it, leaving
`Authorization` free for FROST's BasicAuth:

```bash
SA=aqueduct-dlt-writer@waterdatainitiative-271000.iam.gserviceaccount.com
TOKEN="$(gcloud auth print-identity-token --impersonate-service-account="$SA" \
  --audiences="$ORIGIN" --include-email)"
GATE=(-H "X-Serverless-Authorization: Bearer $TOKEN")   # zsh: must be set in this shell

curl -si "${GATE[@]}" "$FROST/v1.1" | head -1
curl -s  "${GATE[@]}" "$FROST/v1.1/Things"
```

Two things to get right here, both of which produce a 403 that looks like a missing
IAM binding:

- **`--include-email` is required.** Impersonated ID tokens omit the service account
  email by default and Cloud Run rejects them without it.
- **`--audiences` is the service origin**, not the `/FROST-Server/v1.1` path. The
  loader derives this the same way (`_audience()` in `loader/frost_auth.py`).

## FROST users

FROST's BasicAuth users live in the `sensorthings` database (`USERS`, `USER_ROLES`)
and are managed with SQL — there is no API. The tables are created on the first login 
attempt, seeded with `admin`, `write`, and `read` default users, which must be deleted
before going public. The writer needs `delete` as well as `read`/`create`/`update`
roles, because backfills delete observations before reloading.

**1. Create the tables**: make a login attempt through the gate (`GATE` from
[Verify](#verify-while-frost_publicfalse)); expect 401, or 500 if `pgcrypto` is missing:

```bash
curl -si "${GATE[@]}" -u probe:probe -X POST -H 'Content-Type: application/json' \
  -d '{}' "$FROST/v1.1/Things" | head -1
```

**2. Hash the writer password locally**, so the plaintext never leaves your machine
(Apache's `$2y$` bcrypt is pgcrypto's `$2a$`):

```bash
gcloud secrets versions access latest --secret=frost_writer --project=waterdatainitiative-271000 \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["password"], end="")' \
  | htpasswd -niBC 12 aqueduct_writer | cut -d: -f2 | sed 's/^\$2y\$/$2a$/'
```

**3. Run the SQL** in `sensorthings`:

```sql
BEGIN;
INSERT INTO "USERS" ("USER_NAME", "USER_PASS")
  VALUES ('aqueduct_writer', '<the $2a$12$... hash from step 2>');
INSERT INTO "USER_ROLES" ("USER_NAME", "ROLE_NAME") VALUES
  ('aqueduct_writer', 'read'), ('aqueduct_writer', 'create'),
  ('aqueduct_writer', 'update'), ('aqueduct_writer', 'delete');
DELETE FROM "USER_ROLES" WHERE "USER_NAME" IN ('admin', 'write', 'read');
DELETE FROM "USERS"      WHERE "USER_NAME" IN ('admin', 'write', 'read');
COMMIT;
```

**Rotating the writer login:** add a new `frost_writer` version (same JSON shape), hash
it as in step 2, then `UPDATE "USERS" SET "USER_PASS" = '<hash>' WHERE "USER_NAME" =
'aqueduct_writer';`. Pause the `frost_load_*` schedules meanwhile.

### Running SQL

The instance has no public IP, so either:

- **Cloud SQL Studio** (Console -> SQL -> `frost-sensorthings-w3` -> Cloud SQL Studio):
  database `sensorthings`, user `frost`, password from the `frost-db-password` secret.
- **psql from a VM in the `default` VPC** (e.g. `frost-verify`), straight to the private
  IP. `gcloud compute ssh --tunnel-through-iap` needs `roles/iap.tunnelResourceAccessor`.

## `30_dagster_gcp_auth.sh` — Dagster+ → GCP authentication

The documented approach for Serverless is a base64-encoded service account key in
an environment variable; `src/aqueduct_dagster/shared/gcp_auth.py` decodes it to a
private temp file and points `GOOGLE_APPLICATION_CREDENTIALS` at it. One key covers
GCS, Secret Manager, and dlt together.

What the script does, all idempotent:

1. Creates `gs://nmwdi-aqueduct-production` if absent (`us-west3`, uniform bucket-level
   access, public access prevention).
2. Creates the `aqueduct-dlt-writer` service account if absent.
3. Grants **bucket-scoped** `roles/storage.objectAdmin` and
   `roles/storage.legacyBucketReader` on `gs://nmwdi-aqueduct-production` and
   `gs://aqueduct-poc-bravo-pvacd` — scoped to the bucket rather than the project,
   the same way `20_frost.sh` scopes its binding to a single secret. `objectAdmin` and
   not `objectCreator`: dlt creates objects, the transform assets list and read them,
   and the watermark JSON is overwritten in place.
4. Grants `roles/secretmanager.secretAccessor` on every secret in `SECRETS_DAGSTER`
   (`00_config.sh`). A secret that does not exist yet is warned about and skipped.
5. Grants `roles/run.invoker` on the `frost-sensorthings` Cloud Run service, so the
   same identity can call the IAM-gated FROST endpoint. Warns and skips if FROST is
   not deployed yet, so the script still runs before `20_frost.sh`.
6. With `--emit-key` only: mints a key and prints it base64-encoded to stdout, never
   to a file. Gated behind the flag so routine IAM reconciliation re-runs cannot
   silently accumulate keys against GCP's 10-per-account cap.

### Prerequisites (admin-owned — "PM provisions")

- `roles/storage.admin` (bucket create + IAM), `roles/secretmanager.admin`.
- `roles/iam.serviceAccountAdmin` — only if `aqueduct-dlt-writer` does not exist yet.
- `roles/iam.serviceAccountKeyAdmin` — only for `--emit-key`.
- `roles/iam.serviceAccountTokenCreator` on the SA — only for the impersonation
  verification below.
- The org policy `constraints/iam.disableServiceAccountKeyCreation` must **not** be
  enforced.

#### Checking your permissions

```bash
PROJECT_ID=waterdatainitiative-271000
TOKEN="$(gcloud auth print-access-token)"

for p in \
  storage.buckets.create \
  storage.buckets.get \
  storage.buckets.getIamPolicy \
  storage.buckets.setIamPolicy \
  iam.serviceAccounts.create \
  iam.serviceAccountKeys.create \
  secretmanager.secrets.getIamPolicy \
  secretmanager.secrets.setIamPolicy \
  run.services.update \
  run.services.getIamPolicy \
  run.services.setIamPolicy
do
  resp="$(curl -s -X POST \
    -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
    "https://cloudresourcemanager.googleapis.com/v1/projects/${PROJECT_ID}:testIamPermissions" \
    -d "{\"permissions\":[\"${p}\"]}")"
  case "$resp" in
    *'"permissions"'*)   echo "GRANTED  $p" ;;
    *INVALID_ARGUMENT*)  echo "INVALID  $p  (not testable at project scope — ignore)" ;;
    *)                   echo "MISSING  $p" ;;
  esac
done
```

Only `MISSING` matters. Mapping gaps to roles:

| Missing | Ask for | Blocks |
|---|---|---|
| `storage.buckets.*` | `roles/storage.admin` (bucket-scoped is enough) | bucket create + `objectAdmin` grant |
| `iam.serviceAccounts.create` | `roles/iam.serviceAccountAdmin` | first run only |
| `iam.serviceAccountKeys.create` | `roles/iam.serviceAccountKeyAdmin` | `--emit-key` |
| `secretmanager.secrets.*IamPolicy` | `roles/secretmanager.admin` | `secretAccessor` grant |
| `run.services.*` | `roles/run.admin` | ingress change + `run.invoker` grant |

Project-level `testIamPermissions` does not see grants made directly on a bucket, so
a `MISSING` for `storage.buckets.*` may still be granted at bucket scope:

```bash
curl -s -H "Authorization: Bearer ${TOKEN}" \
  "https://storage.googleapis.com/storage/v1/b/aqueduct-poc-bravo-pvacd/iam/testPermissions?permissions=storage.buckets.setIamPolicy&permissions=storage.buckets.getIamPolicy"
```

### Then configure Dagster+

Deployment → **Environment variables** → add these, each attached to code location
`aqueduct_dagster_defs_definitions`:

| Name | Value | Scope | Secret? |
|---|---|---|---|
| `GCP_SERVICE_ACCOUNT_KEY_B64` | the base64 blob from `--emit-key` | Full **and** Branch deployments | yes |
| `FROST_SERVICE_ROOT_URL` | `https://<frost-run-url>/FROST-Server` | **Full deployment only** | no |
| `GCS_BUCKET_URL` | `gs://nmwdi-aqueduct-production` | **Full deployment only** | no |

`FROST_SERVICE_ROOT_URL` and `GCS_BUCKET_URL` are deliberately withheld from branch
deployments: PR runs fall back to the committed `.dlt/config.toml`, so they cannot write
into production storage or the production SensorThings store.

A variable must be attached to at least one code location to take effect. Reload the
code location afterwards.

### Verify

**1. Did the bindings land?**

```bash
gcloud storage buckets get-iam-policy gs://nmwdi-aqueduct-production --format=json
for SECRET in hydrovu_pvacd hydrovu_bernco ckan_cf_bypass ebid_onerain frost_writer; do
  gcloud secrets get-iam-policy "$SECRET" --project=waterdatainitiative-271000
done
gcloud run services get-iam-policy frost-sensorthings \
  --project=waterdatainitiative-271000 --region=us-west3
```

**2. Does the identity work?** Impersonation, so no key is minted. Needs
`roles/iam.serviceAccountTokenCreator` on the SA.

```bash
SA=aqueduct-dlt-writer@waterdatainitiative-271000.iam.gserviceaccount.com
gcloud storage ls gs://aqueduct-poc-bravo-pvacd --impersonate-service-account="$SA"
echo probe | gcloud storage cp - gs://aqueduct-poc-bravo-pvacd/_adc_probe.txt \
  --impersonate-service-account="$SA"
gcloud storage rm gs://aqueduct-poc-bravo-pvacd/_adc_probe.txt \
  --impersonate-service-account="$SA"
for SECRET in hydrovu_pvacd hydrovu_bernco ckan_cf_bypass ebid_onerain frost_writer; do
  gcloud secrets versions access latest --secret="$SECRET" \
    --impersonate-service-account="$SA" >/dev/null && echo "secret $SECRET OK"
done
```

**3. Does the key work?**

```bash
unset GOOGLE_APPLICATION_CREDENTIALS
export GCP_SERVICE_ACCOUNT_KEY_B64=<blob from --emit-key>
export FROST_SERVICE_ROOT_URL=https://<frost-run-url>/FROST-Server
uv run python deploy/31_verify_dagster_auth.py
```

This checks four layers independently: ADC bootstrap, GCS write/read/delete, Secret
Manager access, and an ID-token GET against FROST. `VERBOSE=1` adds
tracebacks. For a full end-to-end run instead, `uv run dagster dev` with the same two
variables set and materialize `pvacd_hydrovu_pipeline`.

## How Dagster+ connects to FROST

`loader/frost_auth.py` resolves the FROST URL and decides whether to authenticate:

| Resolved host | Behaviour |
|---|---|
| `localhost` / `127.0.0.1` / `::1` | No auth — a developer's `docker compose` FROST |
| anything else | ID token in `X-Serverless-Authorization` (Cloud Run's gate) + `Authorization: Basic` with the login from `[destination.frost] gcp_secret` (FROST's BasicAuth) |

Deriving the decision from the host rather than a separate flag means the URL and the
auth mode cannot drift apart. `FROST_SERVICE_ROOT_URL` overrides the
`.dlt/config.toml` default, which stays pointed at localhost so local development
needs no configuration at all.

Both credentials resolve through ADC, so the service account key that already reaches
GCS and Secret Manager reaches FROST too — the extra provisioning is the `run.invoker`
binding and the `frost_writer` grant in `30_dagster_gcp_auth.sh`. Without `gcp_secret`,
reads still work but FROST rejects every write with a 401.

**Branch deployments do not get `FROST_SERVICE_ROOT_URL`.** Development against FROST
happens locally via `docker compose`, so a Dagster+ branch deployment falls back to the
localhost default and its `frost_load_*` assets fail on connection — deliberately, so
PR runs cannot write into the production SensorThings store. Verify branch deployments
against the ingest and transform assets.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Reauthentication failed` on any `gcloud` call | Session expired — `gcloud auth login` |
| 409 `bucket name is not available` on create | GCS bucket names are one global namespace — the name belongs to another project or organization. Pick a different name. |
| 403 on FROST with **no** token | Correct while `FROST_PUBLIC=false` — the gate is working. |
| 403 on FROST with **your own** token | Also correct. The policy grants `run.invoker` only to the service account; no human is a member. Test by impersonating the SA. |
| 403 on FROST with an **impersonated SA** token | Missing `run.invoker`, or `--include-email` omitted, or `--audiences` included a path. |
| **200** on FROST with no token | Expected once `FROST_PUBLIC=true`. While `false`, the gate is not live — check for an `allUsers` binding, or wait 30–60s for IAM propagation. |
| **401** on a pipeline write | Wrong or missing BasicAuth login: check `gcp_secret`, that `frost_writer` matches the hash in `"USERS"`, and the writer's roles. |
| **500** on any request with a login | `pgcrypto` not enabled, or `auth_db_password` not mounted — see the Cloud Run logs. |
| `curl: option : blank argument` | zsh: `GATE` isn't set in this shell. Re-run the `TOKEN=`/`GATE=` lines. |
| `FROST_PUBLIC=true` deploy fails on an org policy rejecting `allUsers` | Use `--no-invoker-iam-check` in place of `--allow-unauthenticated` in `20_frost.sh`. |
| `FrostAuthError: Cannot mint an ID token` | Using user ADC. ID tokens require a service account key. |
| `DefaultCredentialsError` in the verify script | Key is malformed or revoked — re-mint with `--emit-key`. |
| Verify script layer 1 passes, 2 or 3 fails | Key is valid but an IAM binding is missing — re-run `30_dagster_gcp_auth.sh`. |
| 404 from FROST on a path that should exist | Note a 404 means the request *authenticated* — Cloud Run accepted the token and routed it. Check `FROST_SERVICE_ROOT_URL` for typos. |
