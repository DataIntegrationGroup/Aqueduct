# BernCo HydroVu Historical Backfill — Rate Budget

Status: proposal.

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

## Recommended change

Loop one location at a time (not all requested locations together), and for
each location, walk its history month by month, same month-sized calls as
today. Finish one location's full history before moving to the next.
`BackfillCheckpointStore` marks each `(month, location)` pair complete
independently — a run interrupted at any point won't re-fetch anything
already finished.

Each individual API call still covers one month for one location, same
size as today's calls — so this doesn't reopen the memory concern month
chunking exists for. That only applies to a single call spanning a whole
location's history at once, which this design never does.

No changes to `shared/backfill.py`: `BackfillCheckpointStore` already accepts
any `location_ids` list, including a single location. New code needed:
`sources/bernco_hydrovu/backfill.py` (doesn't exist yet) and a
BernCo-specific op — not the shared job factory CABQ/PVACD use, since their
factory only loops over months, not locations.

## Open

Confirm with BernCo/In-Situ whether the account has any daily or monthly
cap beyond the documented per-minute limit.
