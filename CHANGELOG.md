# Changelog

All notable changes to this project will be documented in this file.

---

## [v1.0.0-local] - 2026-02-20

### Added
- Initial local Delta Lakehouse implementation
- Bronze → Silver → Gold layered architecture
- Synthetic event generator for publishing workflows
- Data contract enforcement using JSON schema and expectations
- Reject handling for invalid events (silver/rejects)

### Gold KPIs
- Writing velocity (daily)
- Revision churn
- Engagement band distribution
- Dropoff rate
- Bottleneck detection by stage
- Post-release engagement metrics

### Infrastructure
- Local PySpark + Delta setup
- PowerShell pipeline runner (local/run_all.ps1)
- Repository structure with contracts, pipelines, sql, docs

### Notes
- Windows setup requires JDK 17, winutils, and HADOOP_HOME
- Spark temp directory warnings observed but do not affect execution

