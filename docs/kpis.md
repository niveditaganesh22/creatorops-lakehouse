# KPI Definitions — CreatorOps Lakehouse

This document defines the key operational KPIs computed in the Gold layer of the CreatorOps Lakehouse.

These KPIs measure creator workflow efficiency, content quality signals, and reader engagement patterns.

---

## 1. Writing Velocity (Daily)

**Definition**  
Measures how quickly creators move content through the writing stage.

**Formula**  
Number of completed writing-stage events per day.

**Source**  
Silver events (`creatorops_events`)

**Why it matters**  
- Indicates creator productivity
- Helps identify slowdowns in early content development
- Useful for forecasting content output

---

## 2. Revision Churn

**Definition**  
Measures how often content cycles through revision stages.

**Formula**  
Number of revision events per content item or per day.

**Source**  
Silver events

**Why it matters**  
- High churn may indicate unclear requirements or quality issues
- Helps identify inefficiencies in editing workflows
- Signals rework cost

---

## 3. Engagement Band Distribution

**Definition**  
Categorizes content based on engagement score into bands (e.g., low, medium, high).

**Formula**  
Derived from `metrics.engagementScore` in reader engagement events.

**Source**  
Silver → Gold transformation

**Why it matters**  
- Helps evaluate content performance
- Enables segmentation of successful vs underperforming content
- Supports editorial decision-making

---

## 4. Dropoff Rate

**Definition**  
Measures how often readers disengage from content.

**Formula**  
Dropoff events / (Engagement events + Dropoff events)

**Source**  
Reader interaction events

**Why it matters**  
- Indicates content quality or pacing issues
- Helps identify where readers lose interest
- Useful for optimizing content structure

---

## 5. Bottleneck Detection

**Definition**  
Identifies stages in the workflow where content accumulates or slows down.

**Formula**  
Time spent per stage or count of events per stage over time.

**Source**  
Silver events

**Why it matters**  
- Highlights operational inefficiencies
- Helps teams focus optimization efforts
- Improves end-to-end workflow speed

---

## 6. Post-Release Engagement

**Definition**  
Measures reader interaction after content is released.

**Formula**  
Aggregate engagement metrics for released content.

**Source**  
Reader engagement events (post-release)

**Why it matters**  
- Evaluates content success after publication
- Helps refine future content strategies
- Links operational pipeline to business outcomes

---

## Data Model Notes

- All KPIs are computed in the **Gold layer**
- Source data is validated and standardized in **Silver**
- Raw ingestion occurs in **Bronze**

---

## Design Principles

- Batch-first KPI computation
- Deterministic transformations
- No ML — focus on correctness and interpretability
- Clear mapping from raw events → business metrics
