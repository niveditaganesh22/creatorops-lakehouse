# CreatorOps Data Contracts

This repository treats events as a product contract.  
Bronze stores raw payloads for replay. Silver enforces the canonical contract. Gold derives KPIs.

## Canonical Contract

Source of truth: `contracts/event_schema.json`

### Required fields (must be present in every event)
- `eventId` (ULID string)
- `eventType` (enum)
- `eventVersion` (int >= 1)
- `occurredAt` (ISO-8601 timestamp)
- `tenant.tenantId`
- `tenant.authorId`
- `entity.storyId`

### Optional but recommended fields
- `producer.service`, `producer.env`, `producer.region`, `producer.traceId`
- `entity.seriesId`, `entity.chapterId`, `entity.sceneId`
- `stage` (if missing, Silver derives it from `eventType`)
- `metrics` (event-specific)
- `metadata` (flexible payload for product evolution)

## Event Identity & Idempotency

### Uniqueness
- `eventId` is the primary key.
- Events are treated as immutable.

### Deduplication rule (Silver)
If duplicates arrive, Silver keeps the most recent record by:
1) `eventId` exact match
2) else `hash(eventType + occurredAt + tenantId + storyId + chapterId + sceneId + metrics + metadata)` match

## Stage Mapping (Authoritative)

Even if producers send `stage`, Silver will enforce stage by `eventType`:

| eventType | stage |
|---|---|
| draft_created | DRAFT |
| chapter_written | WRITE |
| scene_revised | REVISION |
| beta_feedback_received | BETA |
| submission_sent | SUBMISSION |
| editor_comment | EDIT |
| publish_scheduled | SCHEDULED |
| publish_released | RELEASED |
| reader_engagement | ENGAGEMENT |
| reader_dropoff | ENGAGEMENT |

## Schema Evolution Policy (Non-negotiable)

This project follows strict-but-practical evolution rules.

### Allowed changes (backward compatible)
- Add new optional fields to `metadata`
- Add new keys inside `metadata`
- Add new optional fields under `producer`
- Add new optional metrics fields (Silver will null-fill for older events)

### Breaking changes (require bumping `eventVersion`)
- Renaming fields
- Changing field types (e.g., int -> string)
- Changing required fields
- Changing the meaning of an existing enum value

### Enum expansion
- Adding a new `eventType` is allowed only with:
  1) `eventVersion` bump
  2) stage mapping update
  3) KPI impact note in `docs/kpis.md`

## Data Quality Guarantees (Silver)

Silver enforces:
- non-null required fields
- valid timestamp parsing
- `eventType` must be one of the allowed enums
- `occurredAt` cannot be > 5 minutes in the future (clock-skew guardrail)
- `wordCount` and `revisionCount` cannot be negative
- `engagementScore` must be between 0 and 100 (only for reader events)

Events failing validation:
- remain in Bronze for audit/replay
- are quarantined into a Silver `_rejects` table with reason codes

## Storage Model

- Bronze: raw append-only, replayable
- Silver: canonical typed event log + rejects
- Gold: KPI tables / aggregates for product analytics