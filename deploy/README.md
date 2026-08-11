# deploy/ — private FROST/SensorThings + PostGIS provisioning

Scripts to stand up a FROST-Server v1.1 endpoint backed by PostGIS in GCP,
reachable only by authenticated callers.

- **FROST** → Cloud Run, `ingress=all` + `--no-allow-unauthenticated` (IAM-gated),
  image pinned to `fraunhoferiosb/frost-server:2.6` (matches the repo's
  `docker-compose.yml`). Reaches Cloud SQL over **Direct VPC egress** (no
  Serverless VPC connector).
- **PostGIS** → a new dedicated **Cloud SQL for PostgreSQL** instance, private IP
  only, on the **`default`** VPC in **us-west3**
- **Project:** `waterdatainitiative-271000` · **Region:** `us-west3` ·
  **VPC/subnet:** `default` / `default` (auto-mode).


## Files

| File | Role |
|------|------|
| `00_config.sh` | Shared variables (no secrets). Sourced by the others. |
| `10_sql.sh` | Create the dedicated Cloud SQL instance + DB + user; store the password in Secret Manager. |
| `20_frost.sh` | Deploy FROST on Cloud Run, wired to that instance's private IP via Direct VPC egress. |
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

# 2. Enable PostGIS BEFORE first FROST boot (private IP → connect from in-VPC).
#    From a VM in the default VPC's us-west3 subnet running cloud-sql-proxy, or a bastion:
#      psql "host=<sql-private-ip> dbname=sensorthings user=frost" \
#        -c "CREATE EXTENSION IF NOT EXISTS postgis;" \
#        -c 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'   # optional, per FROST docs

# 3. FROST on Cloud Run
./deploy/20_frost.sh

# 4. Dagster+ authentication. The bucket/SA/secret parts are independent of 1–3,
#    but the run.invoker grant needs FROST to exist — so run this AFTER step 3
#    (it warns and skips that one binding otherwise, and is safe to re-run).
./deploy/30_dagster_gcp_auth.sh              # bucket + SA + IAM, safe to re-run
./deploy/30_dagster_gcp_auth.sh --emit-key   # ...and mint a key to paste into Dagster+

# 5. Verify the key before pasting it into Dagster+
export GCP_SERVICE_ACCOUNT_KEY_B64=<blob from step 4>
export FROST_SERVICE_ROOT_URL=https://<frost-run-url>/FROST-Server
unset GOOGLE_APPLICATION_CREDENTIALS
uv run python deploy/31_verify_dagster_auth.py
```

Every step is safe to re-run: `10_sql.sh` skips existing resources and sets the
DB password only on first provision (re-runs never rotate it — the running FROST
revision reads the secret's `:latest` version only at instance startup), the
`psql` step uses `IF NOT EXISTS`, and `20_frost.sh` just deploys a new Cloud Run
revision. To rotate the password deliberately: add a new secret version, run
`gcloud sql users set-password`, then re-run `20_frost.sh`.

## Verify (satisfies the acceptance criterion)

**Expect 403 for both of these.** Only `aqueduct-dlt-writer` holds `run.invoker`; the
policy deliberately contains no human members, so your own token is refused exactly
like an anonymous one. Two 403s here is the gate working, not a misconfiguration:

```bash
FROST=https://<frost-run-url>/FROST-Server

curl -si "$FROST/v1.1" | head -1                                                  # anonymous
curl -si -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "$FROST/v1.1" | head -1                                                         # you
```

**Expect 200 from the service account** — the identity that actually holds the role,
and the one Dagster+ uses. Needs `roles/iam.serviceAccountTokenCreator` on the SA:

```bash
SA=aqueduct-dlt-writer@waterdatainitiative-271000.iam.gserviceaccount.com
TOKEN="$(gcloud auth print-identity-token --impersonate-service-account="$SA" \
  --audiences=https://<frost-run-url> --include-email)"

curl -si -H "Authorization: Bearer $TOKEN" "$FROST/v1.1" | head -1
curl -s  -H "Authorization: Bearer $TOKEN" "$FROST/v1.1"          # service document
curl -s  -H "Authorization: Bearer $TOKEN" "$FROST/v1.1/Things"
```

Two things to get right here, both of which produce a 403 that looks like a missing
IAM binding:

- **`--include-email` is required.** Impersonated ID tokens omit the service account
  email by default and Cloud Run rejects them without it.
- **`--audiences` is the service origin**, not the `/FROST-Server/v1.1` path. The
  loader derives this the same way (`_audience()` in `loader/frost_auth.py`).

## `30_dagster_gcp_auth.sh` — Dagster+ → GCP authentication

The documented approach for Serverless is a base64-encoded service account key in
an environment variable; `src/aqueduct_dagster/shared/gcp_auth.py` decodes it to a
private temp file and points `GOOGLE_APPLICATION_CREDENTIALS` at it. One key covers
GCS, Secret Manager, and dlt together.

What the script does, all idempotent:

1. Creates `gs://aqueduct-production` if absent (`us-west3`, uniform bucket-level
   access, public access prevention).
2. Creates the `aqueduct-dlt-writer` service account if absent.
3. Grants **bucket-scoped** `roles/storage.objectAdmin` on `gs://aqueduct-production`
   and `gs://aqueduct-poc-bravo-pvacd` — scoped to the bucket rather than the project,
   the same way `20_frost.sh` scopes its binding to a single secret. `objectAdmin` and
   not `objectCreator`: dlt creates objects, the transform assets list and read them,
   and the watermark JSON is overwritten in place.
4. Grants `roles/secretmanager.secretAccessor` on `hydrovu_pvacd`.
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

Deployment → **Environment variables** → add both, each attached to code location
`aqueduct_dagster_defs_definitions`:

| Name | Value | Scope | Secret? |
|---|---|---|---|
| `GCP_SERVICE_ACCOUNT_KEY_B64` | the base64 blob from `--emit-key` | Full **and** Branch deployments | yes |
| `FROST_SERVICE_ROOT_URL` | `https://<frost-run-url>/FROST-Server` | **Full deployment only** | no |

`FROST_SERVICE_ROOT_URL` is deliberately withheld from branch deployments — see
"How Dagster+ connects to FROST" above.

A variable must be attached to at least one code location to take effect. Reload the
code location afterwards.

### Verify

**1. Did the bindings land?**

```bash
gcloud storage buckets get-iam-policy gs://aqueduct-production --format=json
gcloud secrets get-iam-policy hydrovu_pvacd --project=waterdatainitiative-271000
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
gcloud secrets versions access latest --secret=hydrovu_pvacd \
  --impersonate-service-account="$SA" >/dev/null && echo "secret OK"
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
variables set and materialize `hydrovu_pipeline`.

## How Dagster+ connects to FROST

`loader/frost_auth.py` resolves the FROST URL and decides whether to authenticate:

| Resolved host | Behaviour |
|---|---|
| `localhost` / `127.0.0.1` / `::1` | No auth — a developer's `docker compose` FROST |
| anything else | Mint a Google ID token from ADC and send it as a Bearer header |

Deriving the decision from the host rather than a separate flag means the URL and the
auth mode cannot drift apart. `FROST_SERVICE_ROOT_URL` overrides the
`.dlt/config.toml` default, which stays pointed at localhost so local development
needs no configuration at all.

Because the token comes from ADC, the service account key that already reaches GCS and
Secret Manager reaches FROST too — the only extra provisioning is the `run.invoker`
binding in `30_dagster_gcp_auth.sh`.

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
| 403 on FROST with **no** token | Correct — the gate is working. |
| 403 on FROST with **your own** token | Also correct. The policy grants `run.invoker` only to the service account; no human is a member. Test by impersonating the SA. |
| 403 on FROST with an **impersonated SA** token | Missing `run.invoker`, or `--include-email` omitted, or `--audiences` included a path. |
| **200** on FROST with no token | Gate not live. Check for an `allUsers` binding on the service, or wait 30–60s for IAM propagation after a deploy. |
| `FrostAuthError: Cannot mint an ID token` | Using user ADC. ID tokens require a service account key. |
| `DefaultCredentialsError` in the verify script | Key is malformed or revoked — re-mint with `--emit-key`. |
| Verify script layer 1 passes, 2 or 3 fails | Key is valid but an IAM binding is missing — re-run `30_dagster_gcp_auth.sh`. |
| 404 from FROST on a path that should exist | Note a 404 means the request *authenticated* — Cloud Run accepted the token and routed it. Check `FROST_SERVICE_ROOT_URL` for typos. |
