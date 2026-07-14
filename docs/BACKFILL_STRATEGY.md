# Backfill & Initial Start Date Strategy

This document describes how Aqueduct should handle the dlt initial start date
and backfill in production, as the pipeline scales from 2 sources to 10+.
This is a **design proposal**, not yet implemented — written for team review
before implementation begins.

- **Status:** proposal, open for feedback
- **Last updated:** 2026-07-14

---

## 1. Why this needs a formal design

Today, `initial_start_date` is a static value per source in `.dlt/config.toml`,
and no backfill mechanism exists at all — the only way to re-fetch older data
is to manually clear an entity's cursor in dlt's internal state. This is
manageable at 2 sources. It will not scale to 10+: the pipeline requires one
uniform mechanism that any new source inherits automatically, and backfill
must be treated as a normal, recurring operational need — not a one-time
action that only occurs when a source is first onboarded.

This document answers two questions:

1. What does `initial_start_date` mean, and when should it — and should it
   not — be modified?
2. What is the mechanism for backfill, and how does it remain safe and
   uniform as sources are added?

---

## 2. Initial start date: definition and scope

`initial_start_date` (per source, in `.dlt/config.toml`) is a **floor**, not
a trigger. It applies only the first time a given entity has no cursor yet
in dlt's state — once an entity has run once, its own persisted cursor
permanently overrides the config value.

This has a consequence worth stating explicitly, since it is easy to
misjudge: **editing `initial_start_date` in a pull request does not
retroactively affect entities that have already run.** It changes behavior
only for genuinely new entities. A reviewer skimming a diff could reasonably
assume that changing this date reruns history for every entity; it does not.

**Recommendation:** retain the current design (a floor for new entities,
checked into config, reviewed via pull request), but never use it as the
backfill lever. Temporarily editing a shared config value to move backward
in time, running the job, and remembering to revert it is precisely the
kind of manual step that fails at scale, and it affects every entity in that
source, not only the one requiring repair. Backfill should always proceed
through its own explicit, scoped mechanism (§4), never through mutating this
value.

---

## 3. Situations that require backfill

Backfill is often assumed to be a one-time action performed only when a
source is first onboarded. In practice, at 10+ sources, most backfill demand
will come from ongoing operation, not onboarding. Two distinct *kinds* of
situation arise, and they require different mechanisms (see §4):

**A. Data was never ingested — requires a re-fetch from the source's API:**
1. Initial onboarding of a new source.
2. A new entity added to an existing source (e.g. a new location ID added to
   an allowlist) that was already reporting data before it was added — its
   history predates when the pipeline began requesting it.
3. An upstream API or pipeline outage longer than the retry budget — a run
   errors out for several days, or Dagster itself is unavailable for a
   stretch, and the resulting gap exceeds what the normal incremental cursor
   can self-heal.
4. The vendor corrects historical readings after the fact (recalibration,
   QA/QC revision) and now serves different values for a past window than
   what was already ingested. **Also requires Mode B** — see §4.4.
5. A deliberate decision to extend history further back than the original
   `initial_start_date` (a report requires more years than were originally
   loaded). **Also requires Mode B** — see §4.4.
6. Raw GCS parquet is accidentally deleted or corrupted, with no other copy
   of the raw data anywhere — recovery requires Mode A, since the source API
   is the only remaining source of truth.

**B. Data already exists in GCS raw storage — requires reprocessing, no API
calls:**
7. A bug in adapter or transform mapping is discovered after the fact (an
   incorrect unit conversion, wrong timezone handling, a mis-mapped
   parameter code) — fixing the code does not retroactively fix data already
   loaded into FROST.
8. A decision is made to start mapping a canonical property that was not
   previously mapped, for data already fetched (e.g. the source already
   returns a parameter that was not yet turned into a datastream).
9. Storage or bucket migration, or a layout change.
10. An entity is renamed, merged, or split on the source side and requires
    remapping.
11. FROST data itself is lost or corrupted, but the raw GCS parquet remains
    intact — no API calls are required; the existing archive is simply
    replayed back into FROST.


## 4. Backfill mechanism

### 4.1 Options considered

**Option 1 — Dagster asset partitioning** (date-partitioned assets, backfill
via Dagster's native partition-backfill UI).

This is the conventional Dagster approach, and it has genuine merits:
per-partition retry, a built-in audit trail, and no bespoke tooling. This
direction is **not recommended** — Dagster+ credits are billed per
materialization or step execution, and daily partitions across 10+ sources
and potentially years of history would substantially multiply the number of
billed runs compared to the current design. It is also a larger structural
change, since ingest assets would need to move from "process all entities in
one run" to "one run per day per source."

**Option 2 — Two shared jobs, one per mode, parameterized by a `source_name`
field in run configuration**, dispatching internally to the relevant
source's logic.

This option is appealing because it keeps the Dagster Jobs list short
regardless of source count. It is also **not recommended**:
- "Which source" becomes a free-text configuration field rather than "which
  job was selected" — a typo, or a copied configuration from a previous run,
  would silently target the wrong source instead of being structurally
  impossible.
- The configuration schema must be loosely typed enough to fit every source
  (for example, `entity_ids: list[str]` even where one source's IDs are
  integers), which forgoes Dagster's own configuration validation.
- A runtime dispatch table (source name to fetch function) would still be
  required, and would need to be kept in sync with the source registry.
  Static type checking cannot verify a dynamically dispatched call the way
  it verifies a directly generated object, so per-source code is not
  actually avoided — only the point at which it is checked moves from
  definition time to runtime.

This is confirmed not to be a cost-driven decision: Dagster+ bills on
*executed* runs, not on the number of job definitions in code, so idle job
definitions with no schedule attached, launched manually only, incur no cost
regardless of count.

**Option 3 — Per-source generated jobs, two per source (recommended).**

This follows the pattern already used in this codebase for
`frost_load_hydrovu` / `frost_load_cabq` and `hydrovu_pipeline` /
`cabq_pipeline`: one shared factory function per job type, looped over the
source registry, so that adding a third source requires one registry entry
and no new job-wiring code. "Which source" is determined by which job is
selected in the Dagster UI, which cannot be mistyped. Each job's
configuration schema can be typed precisely for that source. The cost of
this option is purely cosmetic — additional entries in the Jobs list,
searchable and filterable in the UI — and does not increase the amount of
code, since the factory function is written once.

### 4.2 Two modes, and how each executes end to end

Because the two categories of backfill in §3 require genuinely different
mechanics, each source is generated **two** jobs, not one.

**Mode A — Refetch** (category A situations: new or late entity, outage gap,
vendor correction, extended history)

Mode A is a single, self-contained job that performs ingest, transform, and
load together:

1. **Ingest** — contacts the source's own API, in the same manner as normal
   ingest, but for an explicit date range and entity list rather than the
   persisted cursor. This runs under its own isolated pipeline state,
   separate from the production pipeline's cursors, so a backfill run can
   never roll back or interfere with the next normal scheduled run. The
   HTTP and pagination logic here is inherently source-specific — every
   vendor's API differs — so this stage requires one new function per
   source, mirroring that source's existing ingest code. It cannot be made
   fully generic, in the same way today's per-source ingest logic is not
   generic.
2. **Transform** — reuses the same transform logic as the normal pipeline
   (DTW filtering, location join, grouping, adapter mapping), factored into
   a plain function callable from both the normal transform asset and this
   job, mirroring how `_frost_load()` is already factored out separately
   from the `frost_load_hydrovu` asset wrapper.
3. **Load** — always uses the window-scoped, delete-then-repost mechanism
   described in §4.4, rather than the normal `frost_load` asset's
   single-watermark filter. This mechanism is safe for every situation in
   category A, not only items 4 and 5: deleting observations in a window
   where none yet exist (a brand-new entity, a gap fill) is simply a no-op
   delete followed by a normal post. Using one uniform, always-safe load
   path avoids the need to detect whether a given window falls behind an
   existing watermark, and removes any dependency on the next scheduled
   run's behavior.

**Mode B — Replay** (category B situations: bug fix, retroactive property,
storage migration, FROST-only data loss)

- Never contacts the source API. Reads already-ingested raw GCS parquet for
  an explicit date range, filtered by event time, and re-runs that source's
  *existing* adapter — so fixing an adapter bug and running replay picks up
  the fix automatically.
- Almost entirely generic and shared: unlike Mode A, none of this logic is
  source-specific, so the same code drives every source's replay job as soon
  as that source has an adapter and a registry entry. No new per-source code
  is required for Mode B.
- Uses the same window-scoped, delete-then-repost load mechanism as Mode A's
  load stage (§4.4), since it is deliberately reprocessing a window that has
  already been loaded.

Because both modes now share the same load mechanism, the substantive
difference between them is simply whether the job needs to contact the
source's API first (Mode A) or can operate entirely on data already present
in GCS (Mode B).

### 4.3 Chunking — why a wide date range cannot be one API call

A naive "fetch the entire requested range in one pass" approach breaks down
for anything beyond a few weeks:

- **Run duration.** A year at HydroVu's pagination rate (approximately 2
  days per page) is roughly 180 pages per entity — with 10 entities, that is
  approximately 1,800 sequential calls in one run, risking multi-hour single
  steps that most orchestration environments do not tolerate well.
- **Memory.** The current per-location fetch accumulates the entire
  requested range in memory before returning it — a year of data in memory
  per entity is a genuine concern independent of duration.
- **Blast radius of partial failure.** A single long-running job that fails
  90% of the way through loses all progress with no visibility into what
  had already succeeded.

**Design:** the backfill job splits the requested range into chunks
(default: calendar month — January 1 to February 1, February 1 to March 1,
and so on — easier to reason about in logs than an arbitrary rolling window)
and processes them **sequentially within the same Dagster run**, not as
separate launched runs. This preserves the same cost-neutrality established
in §4.1: one Dagster run regardless of chunk count.

A chunk is considered complete, and checkpointed accordingly, only once
ingest, transform, and load have all succeeded for that chunk — not ingest
alone. Checkpointing uses the backfill's own isolated pipeline state, which
is already isolated from production cursors, so a crash partway through
resumes from the last fully completed chunk on re-launch rather than
restarting the entire range. If a crash occurs during the load stage of a
chunk, the entire chunk (ingest, transform, and load) is retried on the next
attempt, which is safe because all three stages are idempotent to repeat.

One nuance worth noting so it does not cause confusion later: chunking by
calendar month does **not** mean the resulting GCS files land in folders
matching the historical period (for example, `year=2026/month=01/...` for
January data). The GCS `year=/month=/day=` layout is stamped by *when the
pipeline runs*, not by the event time contained in the rows — a backfill run
executed today places every chunk's file under today's date folder, the
same as any normal run. This is not a problem in practice, since transform
locates files by globbing all folders and filtering on the load timestamp
embedded in the filename rather than the folder path — but it means
calendar-month chunking is a matter of chunk sizing and log readability, not
of mirroring storage layout.

### 4.4 The watermark problem, and how Mode A and Mode B both address it

The FROST loader tracks a single, monotonic "highest timestamp loaded"
watermark per datastream, and — unlike the metadata entities (locations,
things, sensors), which are looked up by external key before creation —
individual observations have no dedup key at all. Rewinding the watermark
to allow old data back in would therefore also reopen everything between the
rewind point and the present, with no way to avoid duplicating observations
that were never actually incorrect.

Both modes instead use the same mechanism: for each affected datastream and
chunk, existing observations in that exact window are deleted, and the
corrected or newly loaded ones are posted in their place. The persisted
watermark is never rewound, and is only extended forward if the processed
window happens to reach past it.

**Known trade-off:** if a job crashes between the delete and the completed
repost, that chunk's window has a temporary hole in FROST until the job is
re-run. This is acceptable because the operation is idempotent — re-running
the same chunk resolves the hole correctly. This ordering was chosen
deliberately: the alternative (repost first, delete the old values second)
trades a visible, self-healing hole for a silent one — a crash between post
and delete would leave both the outdated and the corrected data coexisting,
double-counting in any aggregate query, with no mechanism to detect or
correct it. A hole is easy to notice (a gap or an unexpectedly low count)
and easy to resolve (re-run); silent duplication is not. This is also why
chunk size specifically matters here — smaller chunks bound how much data
can ever be temporarily missing if a crash occurs mid-chunk.

### 4.5 Safety defaults

Every backfill job's run configuration defaults to `dry_run: true` —
launching it logs what *would* happen (entities, date range, resolved chunk
plan, expected row and observation counts) without writing anything.
Executing the job for real requires explicitly setting `dry_run: false`,
which is itself part of the Dagster run's logged configuration — consistent
with this repository's existing rule that a production backfill is a
reviewed, deliberate action, never a default.

---

## 5. Execution flow — how these pipelines run in practice

### 5.1 Normal daily incremental runs (unchanged)

Each source's existing schedule fires its existing pipeline job
automatically, with no operator involvement:

- `hydrovu_schedule` (`0 6 * * *`) triggers `hydrovu_pipeline`, which runs
  `raw_hydrovu_readings` → `canonical_bundles_hydrovu` → `frost_load_hydrovu`
  in sequence.
- `cabq_schedule` (`0 8 * * *`) triggers `cabq_pipeline` independently, with
  its own three assets. Running one source's pipeline never triggers or
  blocks the other.

### 5.2 Launching a backfill job

Two additional jobs are generated per source and appear in the Dagster
UI's Jobs list alongside the existing pipeline jobs: `<source>_backfill_refetch`
and `<source>_backfill_replay`. Neither has a schedule attached — both are
launched manually, on demand:

1. Open the Dagster UI and select **Jobs**.
2. Select the relevant job (for example, `hydrovu_backfill_refetch`).
3. Open the **Launchpad** and provide the run configuration — entity list,
   start and end dates, and (for Mode A) an isolated pipeline name — as YAML
   or JSON directly in the browser. No command-line access is required.
4. Launch with the default `dry_run: true` and review the logged summary of
   what the run would do.
5. Re-launch the same configuration with `dry_run: false` to execute.
6. Monitor the run in the **Runs** tab. If it fails partway through a
   multi-chunk backfill, re-launching the same job with the same
   configuration resumes from the last completed chunk rather than
   restarting the full range.

---

## 6. Open questions — feedback wanted

- Default `chunk_days` (or calendar-month chunking) — is a month the
  appropriate default across sources of substantially different volume, or
  should this vary per source?
- The delete-then-repost mechanism in Mode A's load stage and Mode B — is
  the team comfortable with FROST calls performing destructive deletes as
  part of an automated job, even when gated behind `dry_run`? An
  alternative would be a diff-and-post-only-missing approach: more complex
  to implement, but avoids destructive calls entirely.
- Should anything in `shared/source_registry.py`'s `SourceConfig` grow to
  support this design (for example, a per-source default chunk size), or is
  a source-agnostic default sufficient for now?
- The bookkeeping-loss case (lost cursor or watermark state with
  underlying data intact) is not addressed by this design — does it warrant
  its own mechanism, or is manual recovery acceptable given how rarely it is
  expected to occur?
