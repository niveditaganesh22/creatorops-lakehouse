import json
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import yaml

# ----------------------------
# Utilities
# ----------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def weighted_choice(items: List[Tuple[str, float]], rng: random.Random) -> str:
    total = sum(w for _, w in items)
    r = rng.random() * total
    upto = 0.0
    for k, w in items:
        upto += w
        if upto >= r:
            return k
    return items[-1][0]

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def ulid_like(dt: datetime, rng: random.Random) -> str:
    # Not a real ULID implementation; good enough for ordering + uniqueness in this portfolio.
    # Format: YYYYMMDDHHMMSS + random hex
    ts = dt.strftime("%Y%m%d%H%M%S")
    suffix = "".join(rng.choice("0123456789ABCDEF") for _ in range(16))
    return f"{ts}{suffix}"

# ----------------------------
# Domain modeling
# ----------------------------

EVENT_TYPES = [
    "draft_created",
    "chapter_written",
    "scene_revised",
    "beta_feedback_received",
    "submission_sent",
    "editor_comment",
    "publish_scheduled",
    "publish_released",
    "reader_engagement",
    "reader_dropoff",
]

STAGE_BY_EVENT = {
    "draft_created": "DRAFT",
    "chapter_written": "WRITE",
    "scene_revised": "REVISION",
    "beta_feedback_received": "BETA",
    "submission_sent": "SUBMISSION",
    "editor_comment": "EDIT",
    "publish_scheduled": "SCHEDULED",
    "publish_released": "RELEASED",
    "reader_engagement": "ENGAGEMENT",
    "reader_dropoff": "ENGAGEMENT",
}

@dataclass
class Persona:
    name: str
    weight: float
    active_days_per_week: Tuple[int, int]
    words_per_active_day: Tuple[int, int]
    burst_probability: float
    avg_revisions_per_scene: Tuple[int, int]
    revision_bursts: str
    days_draft_to_release: Tuple[int, int]
    scheduling_buffer_days: Tuple[int, int]
    initial_score_range: Tuple[int, int]
    decay_curve: str
    dropout_probability: float

def parse_range(v: Any) -> Tuple[int, int]:
    return int(v[0]), int(v[1])

def parse_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    return float(v)

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def build_personas(cfg: Dict[str, Any]) -> List[Persona]:
    personas = []
    for name, p in cfg["personas"].items():
        cadence = p.get("cadence", {})
        revisions = p.get("revisions", {})
        publishing = p.get("publishing", {})
        engagement = p.get("engagement", {})

        personas.append(
            Persona(
                name=name,
                weight=float(p.get("weight", 0.0)),
                active_days_per_week=parse_range(cadence.get("active_days_per_week", [3, 5])),
                words_per_active_day=parse_range(cadence.get("words_per_active_day", [600, 1200])),
                burst_probability=parse_float(cadence.get("burst_probability", 0.0), 0.0),
                avg_revisions_per_scene=parse_range(revisions.get("avg_revisions_per_scene", [0, 2])),
                revision_bursts=str(revisions.get("revision_bursts", "low")),
                days_draft_to_release=parse_range(publishing.get("days_draft_to_release", [20, 45])),
                scheduling_buffer_days=parse_range(publishing.get("scheduling_buffer_days", [2, 10])),
                initial_score_range=parse_range(engagement.get("initial_score_range", [50, 80])),
                decay_curve=str(engagement.get("decay_curve", "medium")),
                dropout_probability=parse_float(cadence.get("dropout_probability", 0.0), 0.0),
            )
        )
    return personas

# ----------------------------
# Event generation
# ----------------------------

def rand_date_range(start: date, end: date, rng: random.Random) -> date:
    delta = (end - start).days
    return start + timedelta(days=rng.randint(0, max(delta, 0)))

def rand_time_on_day(d: date, rng: random.Random) -> datetime:
    # writing tends to happen evening/night; bias a bit
    hour = rng.choice([7, 8, 9, 18, 19, 20, 21, 22, 23])
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=timezone.utc)

def engagement_decay_multiplier(curve: str, day_offset: int) -> float:
    # simple deterministic curves; no ML
    if curve == "slow":
        return max(0.25, 1.0 - 0.02 * day_offset)
    if curve == "fast":
        return max(0.10, 1.0 - 0.08 * day_offset)
    if curve == "none":
        return 0.0
    # medium
    return max(0.15, 1.0 - 0.05 * day_offset)

def generate_story_flow(
    tenant_id: str,
    author_id: str,
    plan: str,
    series_id: str,
    story_id: str,
    start_day: date,
    end_day: date,
    persona: Persona,
    meta_templates: Dict[str, Any],
    rng: random.Random,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    # Draft created
    draft_day = rand_date_range(start_day, end_day, rng)
    draft_at = rand_time_on_day(draft_day, rng)

    def emit(event_type: str, occurred_at: datetime, chapter_id: Optional[str]=None, scene_id: Optional[str]=None,
             revision_count: Optional[int]=None, word_count: Optional[int]=None, engagement_score: Optional[float]=None,
             metadata: Optional[Dict[str, Any]]=None) -> None:
        e = {
            "eventId": ulid_like(occurred_at, rng),
            "eventType": event_type,
            "eventVersion": 1,
            "occurredAt": iso(occurred_at),
            "tenant": {"tenantId": tenant_id, "authorId": author_id, "plan": plan},
            "entity": {"seriesId": series_id, "storyId": story_id},
            "stage": STAGE_BY_EVENT[event_type],
            "metrics": {
                "revisionCount": revision_count,
                "wordCount": word_count,
                "engagementScore": engagement_score
            },
            "metadata": metadata or {}
        }
        if chapter_id:
            e["entity"]["chapterId"] = chapter_id
        if scene_id:
            e["entity"]["sceneId"] = scene_id
        # producer fields are optional in contract; include for realism
        e["producer"] = {"service": "creatorops-sim", "env": "dev", "region": "canada-central", "traceId": str(uuid.uuid4())}
        events.append(e)

    emit("draft_created", draft_at, metadata={"draftTool": rng.choice(["scrivener", "docs", "notion"]), "draftMode": rng.choice(["outline", "discovery", "hybrid"])})

    # Decide if this story will be abandoned (persona-dependent)
    abandoned = rng.random() < persona.dropout_probability or persona.days_draft_to_release[0] >= 999
    if abandoned:
        # still allow some writing/revision activity then stop
        active_weeks = rng.randint(1, 3)
        total_days = active_weeks * 7
        end_active = min(end_day, draft_day + timedelta(days=total_days))
        # generate some chapter_written + scene_revised then stop
        chapter_count = rng.randint(1, 3)
        for ch in range(1, chapter_count + 1):
            ch_id = f"ch_{ch:02d}"
            write_day = rand_date_range(draft_day, end_active, rng)
            words = rng.randint(*persona.words_per_active_day)
            emit("chapter_written", rand_time_on_day(write_day, rng), chapter_id=ch_id, word_count=words)

            scene_revs = rng.randint(*persona.avg_revisions_per_scene)
            for s in range(1, rng.randint(1, 4) + 1):
                sc_id = f"sc_{s:02d}"
                for r in range(scene_revs):
                    rev_day = rand_date_range(write_day, end_active, rng)
                    reason = rng.choice(meta_templates["scene_revised"]["revision_reasons"])
                    emit(
                        "scene_revised",
                        rand_time_on_day(rev_day, rng),
                        chapter_id=ch_id,
                        scene_id=sc_id,
                        revision_count=r + 1,
                        word_count=max(0, words + rng.randint(-200, 300)),
                        metadata={"revisionReason": reason}
                    )
        return events

    # Publishing window
    days_to_release = rng.randint(*persona.days_draft_to_release)
    release_day = min(end_day, draft_day + timedelta(days=days_to_release))
    schedule_buffer = rng.randint(*persona.scheduling_buffer_days)
    scheduled_day = max(draft_day, release_day - timedelta(days=schedule_buffer))

    # Writing / revision cadence
    # We generate chapters across the window between draft_day and scheduled_day
    chapter_total = rng.randint(3, 12)
    for ch in range(1, chapter_total + 1):
        ch_id = f"ch_{ch:02d}"
        write_day = rand_date_range(draft_day, scheduled_day, rng)

        # burst writing may pack more work into same day
        words = rng.randint(*persona.words_per_active_day)
        if rng.random() < persona.burst_probability:
            words = int(words * rng.uniform(1.4, 2.3))

        emit("chapter_written", rand_time_on_day(write_day, rng), chapter_id=ch_id, word_count=words)

        # Revisions per scene
        scenes = rng.randint(1, 5)
        base_revs = rng.randint(*persona.avg_revisions_per_scene)

        for s in range(1, scenes + 1):
            sc_id = f"sc_{s:02d}"
            revs = base_revs
            if persona.revision_bursts == "high" and rng.random() < 0.35:
                revs += rng.randint(2, 6)
            elif persona.revision_bursts == "medium" and rng.random() < 0.25:
                revs += rng.randint(1, 3)

            for r in range(1, revs + 1):
                rev_day = rand_date_range(write_day, scheduled_day, rng)
                reason = rng.choice(meta_templates["scene_revised"]["revision_reasons"])
                emit(
                    "scene_revised",
                    rand_time_on_day(rev_day, rng),
                    chapter_id=ch_id,
                    scene_id=sc_id,
                    revision_count=r,
                    word_count=max(0, words + rng.randint(-300, 500)),
                    metadata={"revisionReason": reason}
                )

    # Beta + submission + editor comments
    beta_day = rand_date_range(draft_day + timedelta(days=3), scheduled_day, rng)
    beta_meta = {
        "source": rng.choice(meta_templates["beta_feedback_received"]["sources"]),
        "sentiment": rng.choice(meta_templates["beta_feedback_received"]["sentiment"])
    }
    emit("beta_feedback_received", rand_time_on_day(beta_day, rng), metadata=beta_meta)

    submission_day = rand_date_range(beta_day, scheduled_day, rng)
    emit("submission_sent", rand_time_on_day(submission_day, rng), metadata={"channel": rng.choice(["agent", "direct", "platform"])})

    # 1–4 editor comments
    editor_comments = rng.randint(1, 4)
    for _ in range(editor_comments):
        ed_day = rand_date_range(submission_day, scheduled_day, rng)
        ed_meta = {
            "commentType": rng.choice(meta_templates["editor_comment"]["comment_types"]),
            "severity": rng.choice(meta_templates["editor_comment"]["severity"])
        }
        emit("editor_comment", rand_time_on_day(ed_day, rng), metadata=ed_meta)

    # Schedule + release
    emit("publish_scheduled", rand_time_on_day(scheduled_day, rng), metadata={"releaseChannel": rng.choice(["serial", "full_drop"])})

    emit("publish_released", rand_time_on_day(release_day, rng), metadata={"storefront": rng.choice(["galatea_like", "kindle_like", "web_serial"])})

    # Engagement events after release (simple curve)
    base_score = rng.randint(*persona.initial_score_range)
    for offset in range(0, rng.randint(10, 28)):
        d = release_day + timedelta(days=offset)
        if d > end_day:
            break
        mult = engagement_decay_multiplier(persona.decay_curve, offset)
        score = base_score * mult
        if score <= 0:
            continue

        # engagement
        emit(
            "reader_engagement",
            rand_time_on_day(d, rng),
            engagement_score=round(clamp(score + rng.uniform(-5, 5), 0, 100), 2),
            metadata={"surface": rng.choice(["feed", "search", "recommendation"]), "device": rng.choice(["mobile", "web"])}
        )

        # dropoff chance increases as score decays
        drop_prob = clamp(0.05 + (1.0 - mult) * 0.6, 0.05, 0.70)
        if rng.random() < drop_prob:
            emit(
                "reader_dropoff",
                rand_time_on_day(d, rng),
                engagement_score=round(clamp(score + rng.uniform(-10, 2), 0, 100), 2),
                metadata={"reason": rng.choice(["pacing", "tone", "lost_interest", "life_interrupt"]) }
            )

    return events

def main() -> None:
    cfg = load_config(os.path.join("data_generator", "profiles.yml"))
    gen = cfg["generator"]
    seed = int(gen.get("seed", 22))
    rng = random.Random(seed)

    tenants = int(gen["tenants"])
    days = int(gen["timeline_days"])
    target_events = int(gen["target_total_events"])
    authors_rng = tuple(gen["authors_per_tenant_range"])
    stories_rng = tuple(gen["stories_per_author_range"])

    personas = build_personas(cfg)
    persona_weights = [(p.name, p.weight) for p in personas]
    persona_by_name = {p.name: p for p in personas}

    meta_templates = cfg.get("metadata_templates", {})

    end_day = utc_now().date()
    start_day = end_day - timedelta(days=days)

    all_events: List[Dict[str, Any]] = []

    for t in range(1, tenants + 1):
        tenant_id = f"tnt_{t:03d}"
        # minimal SaaS realism: some tenants on pro
        plan = "pro" if rng.random() < 0.25 else "free"

        author_count = rng.randint(int(authors_rng[0]), int(authors_rng[1]))
        for a in range(1, author_count + 1):
            author_id = f"auth_{t:03d}_{a:03d}"
            series_id = f"ser_{t:03d}_{a:03d}"

            persona_name = weighted_choice(persona_weights, rng)
            persona = persona_by_name[persona_name]

            story_count = rng.randint(int(stories_rng[0]), int(stories_rng[1]))
            for s in range(1, story_count + 1):
                story_id = f"sto_{t:03d}_{a:03d}_{s:02d}"
                story_events = generate_story_flow(
                    tenant_id=tenant_id,
                    author_id=author_id,
                    plan=plan,
                    series_id=series_id,
                    story_id=story_id,
                    start_day=start_day,
                    end_day=end_day,
                    persona=persona,
                    meta_templates=meta_templates,
                    rng=rng,
                )
                # stamp persona for transparency (but keep it in metadata)
                for e in story_events:
                    e.setdefault("metadata", {})
                    e["metadata"]["persona"] = persona_name
                all_events.extend(story_events)

    # Downsample / upsample lightly to hit target without changing realism too much
    rng.shuffle(all_events)
    if len(all_events) > target_events:
        all_events = all_events[:target_events]

    # Partitioned NDJSON output
    out_root = os.path.join("out", "events")
    ensure_dir(out_root)

    # group by event date (occurredAt)
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for e in all_events:
        d = e["occurredAt"][:10]  # YYYY-MM-DD
        buckets.setdefault(d, []).append(e)

    for d, events in buckets.items():
        path = os.path.join(out_root, f"p_event_date={d}")
        ensure_dir(path)
        fp = os.path.join(path, "events.ndjson")
        with open(fp, "w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")

    print(f"Generated events: {len(all_events)}")
    print(f"Output folder: {out_root}")
    print(f"Partitions: {len(buckets)}")

if __name__ == "__main__":
    main()