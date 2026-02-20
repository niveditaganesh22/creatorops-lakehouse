# Silver Transform Runbook (Bronze → Silver)

This document defines the authoritative transformation from raw events (Bronze) into the canonical event log (Silver).

## Inputs / Outputs

**Input**
- `bronze.creatorops_events_raw` (raw_json, append-only)

**Outputs**
- `silver.creatorops_events` (canonical, typed, deduped)
- `silver.creatorops_events_rejects` (quarantined bad events)

## Transform Principles

- Bronze is never mutated; it is replayable.
- Silver is the canonical record used by analytics and KPIs.
- Events are immutable; corrections arrive as new events.
- Schema evolution is handled through `eventVersion` and metadata expansion.

## Step-by-step Transform

### 1) Parse raw JSON
Attempt to parse `raw_json` into a structured object using the canonical contract.

If JSON parsing fails:
- write record to rejects with:
  - `reject_stage = "parsing"`
  - `reject_reason = "invalid_json"`

### 2) Extract canonical fields
Flatten and cast to Silver columns:

- `event_id` ← `eventId`
- `event_type` ← `eventType`
- `event_version` ← `eventVersion`
- `occurred_at` ← `occurredAt` (timestamp)
- `tenant_id` ← `tenant.tenantId`
- `author_id` ← `tenant.authorId`
- `plan` ← `tenant.plan`
- `series_id` ← `entity.seriesId`
- `story_id` ← `entity.storyId`
- `chapter_id` ← `entity.chapterId`
- `scene_id` ← `entity.sceneId`
- producer fields from `producer.*`
- metrics from `metrics.*`

If any required field is missing or cannot be cast:
- write to rejects with:
  - `reject_stage = "validation"`
  - `reject_reason = "missing_required"` or `cast_failed`
  - include details in `validation_errors`

### 3) Enforce enum validity
Reject if `event_type` is not in allowed enum list.

Reject if `stage` is present but not in allowed enum list.

Write to rejects with:
- `reject_stage = "enum_check"`
- `reject_reason = "invalid_event_type"` or `invalid_stage`

### 4) Stage derivation (authoritative mapping)
Stage is enforced by event_type, regardless of producer-supplied stage.

Mapping:
- draft_created → DRAFT
- chapter_written → WRITE
- scene_revised → REVISION
- beta_feedback_received → BETA
- submission_sent → SUBMISSION
- editor_comment → EDIT
- publish_scheduled → SCHEDULED
- publish_released → RELEASED
- reader_engagement → ENGAGEMENT
- reader_dropoff → ENGAGEMENT

If the producer stage conflicts, keep:
- canonical `stage` = derived stage
- store the producer stage (if present) in metadata_map as `producerStage`

### 5) Metric validation
Rules:
- `revision_count` must be >= 0 if present
- `word_count` must be >= 0 if present
- `engagement_score` must be between 0 and 100, but only for reader_* events

Invalid metrics → rejects with:
- `reject_stage = "metric_check"`
- `reject_reason = "invalid_metric"`

### 6) Late-event detection
`is_late_event = true` when:
- `occurred_at < current_timestamp() - interval 7 days`
(7 days is configurable; used to monitor backfills / delayed ingestion)

### 7) Event hash for dedupe
Compute `event_hash` as SHA-256 of stable fields:

`sha2(concat_ws('||',
  event_type,
  cast(occurred_at as string),
  tenant_id,
  author_id,
  coalesce(series_id,''),
  story_id,
  coalesce(chapter_id,''),
  coalesce(scene_id,''),
  coalesce(cast(revision_count as string),''),
  coalesce(cast(word_count as string),''),
  coalesce(cast(engagement_score as string),'')
), 256)`

### 8) Deduplication strategy (Silver upsert)
Preferred:
- If `event_id` is present: dedupe by `event_id` (keep latest by ingested_at)
Fallback:
- If generator produces occasional duplicate event_ids: dedupe by `(event_hash, occurred_at, story_id)`.

Silver write pattern:
- Use MERGE INTO (upsert) to enforce uniqueness.
- For duplicates, keep record with max(ingested_at).

## Reject Table Contract

Write rejects with:
- `ingest_id`
- `raw_json`
- `reject_reason`
- `reject_stage`
- `validation_errors` (JSON string)
- `rejected_at = current_timestamp()`
- `p_reject_date = current_date()`

## Operational Notes

- Batch runs are partition-aware (process only recent ingest dates unless backfill).
- Backfill uses parameters `start_date` and `end_date`.
- OPTIMIZE/ZORDER is weekly; avoid daily cost.