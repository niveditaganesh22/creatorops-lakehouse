# CreatorOps Lakehouse (Product Brief)

CreatorOps Lakehouse is a SaaS-style operational analytics platform for creative publishing workflows.

It tracks the full creator pipeline using realistic workflow events (draft → writing → revision → beta → submission → edit → scheduled → released → engagement),
then computes operational KPIs that reveal velocity, churn, bottlenecks, and post-release retention.

This project is intentionally batch-first and cost-sensitive:
- Azure Databricks + Delta Lake
- Bronze → Silver → Gold
- Small job clusters with 10-minute auto-termination
- No unnecessary ML — focus is data modeling, quality, and KPI correctness

Primary outputs:
- Gold KPI tables for CreatorOps operational metrics
- A reproducible synthetic event generator that mimics real publishing behavior
- Infrastructure-as-Code for a minimal Azure Databricks lakehouse setup