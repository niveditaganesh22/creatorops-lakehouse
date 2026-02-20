# CreatorOps Lakehouse (V1 — Local Delta Lakehouse)

CreatorOps Lakehouse is a SaaS-style operational analytics platform that models creative publishing workflows and computes operational KPIs across the full creator lifecycle.

This version (V1) runs locally using **PySpark + Delta Lake** and demonstrates:

- Bronze → Silver → Gold lakehouse modeling
- Event contract enforcement
- Reject handling for invalid records
- Operational KPI computation (velocity, churn, retention, bottlenecks)
- A realistic synthetic event generator

This is not a notebook demo.  
It is a structured analytics system.

---

## Architecture (V1 Local)

Bronze → Raw ingested events  
Silver → Contract-enforced, typed, validated events  
Gold → Business KPIs  

Delta tables are written locally under:
out/delta/
bronze/
silver/
gold/


---

## Workflow Modeled

Events simulate a real publishing lifecycle:


draft → writing → revision → beta → submission → edit → scheduled → released → engagement


Reader interaction events:

- `reader_engagement`
- `reader_dropoff`

---

## KPIs Implemented (Gold Layer)

- Writing velocity (daily)
- Revision churn (daily)
- Engagement band distribution
- Dropoff rate
- Bottleneck detection by stage
- Post-release engagement metrics

---

## Run Locally (5 Minutes)

### 1. Create Virtual Environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r local/requirements.txt
```
### 2. Ensure Java + Winutils (Windows Only)

Install JDK 17

Set JAVA_HOME

Place winutils.exe in C:\hadoop\bin

Set HADOOP_HOME=C:\hadoop

### 3. Run Full Pipeline

.\local\run_all.ps1

You should see Gold tables written under:

out/delta/gold

## Data Contract Enforcement

### Schema and expectations:

contracts/event_schema.json
contracts/expectations.yml

Invalid events are written to:

out/delta/silver/rejects

This models real production data quality patterns.
```text
Repository Structure
contracts/
data_generator/
pipelines/
  bronze/
  silver/
  gold/
sql/
docs/
local/
```
### Design Philosophy

Batch-first

Cost-sensitive

No unnecessary ML

Focus on correctness and modeling clarity

Clear separation of concerns per layer

## Roadmap
## V2

Azure Databricks implementation

Cluster configuration + job orchestration

Unity Catalog-style governance

## V3

CI/CD via GitHub Actions

Cloud object storage backend (ADLS/S3)

Production-grade environment promotion

## License

MIT


---
