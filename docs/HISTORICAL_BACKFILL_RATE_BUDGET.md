# BernCo HydroVu Historical Backfill — Rate Budget

Status: implemented.

## Request volume

Using each of the 36 DTW-carrying locations' actual earliest-reading date
(`docs/sources/bernco_hydrovu_location_survey.md`), a full historical load
is about 36,173 requests total.

## No documented daily cap

- HydroVu's own OpenAPI spec (`GET /public-api/docs/spec`): only rate-limit
  statement is 1,000 requests/minute. No daily or monthly figure.
- Live response headers: only `x-isi-requests-this-minute` and
  `x-isi-requests-timeout`, both per-minute. No daily-scoped header.

Since the real cap is unknown, the backfill should handle actually hitting a
rate limit gracefully — stop the run cleanly and resume later, not crash —
rather than assuming the per-minute limit is the only one that can ever fire.

## What already exists (no changes needed)

Calendar-month chunking, `BackfillCheckpointStore` (resume from last
completed chunk), `dry_run: true` by default, and per-minute 429 handling
already cover this backfill.

## Decided: use the shared factory as-is

Implemented as `sources/bernco_hydrovu/backfill.py`, reusing
`shared/backfill.py`'s existing month-chunking exactly like PVACD/CABQ — one
`run_backfill_chunk()` call per month, covering all requested locations
(processed one at a time inside that call, still one month per location per
API call, so the memory bound §4.3 relies on is unchanged).

An earlier version of this doc recommended a location-outer, bespoke-op
design instead, mainly to avoid wasting requests if a per-run budget stopped
a run mid-month. Since there's no request budget (see below) and no
confirmed daily cap, that reason no longer applies — matching the existing,
tested factory pattern is simpler and lower-risk.

**Rate-limit hardening done as part of this:** `fetch_location_data`
(`hydrovu_common.py`) used to only handle 404/429/5xx explicitly and crash
uncaught on anything else. It now treats any non-2xx status the same
graceful way — log it, return an error, fail just the current chunk. Earlier
completed chunks stay checkpointed either way; re-launching resumes.

## Open

Confirm with BernCo/In-Situ whether the account has any daily or monthly
cap beyond the documented per-minute limit.
