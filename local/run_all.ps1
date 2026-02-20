$ErrorActionPreference = "Stop"

Write-Host "CreatorOps Lakehouse (Local) — Running Bronze → Silver → Gold" -ForegroundColor Cyan

# Activate venv if not already
if (-not $env:VIRTUAL_ENV) {
    if (Test-Path ".\.venv\Scripts\Activate.ps1") {
        . .\.venv\Scripts\Activate.ps1
    }
    else {
        throw "No .venv found. Create it first: python -m venv .venv"
    }
}

# Minimal Windows Spark deps (optional; safe if already set)
if (-not $env:JAVA_HOME) { Write-Host "WARN: JAVA_HOME not set in this shell." -ForegroundColor Yellow }
if (-not $env:HADOOP_HOME) { Write-Host "WARN: HADOOP_HOME not set in this shell." -ForegroundColor Yellow }

python .\pipelines\bronze\ingest_local.py
python .\pipelines\silver\transform_local.py
python .\pipelines\gold\kpis_local.py
python .\pipelines\gold\retention_local.py
python .\pipelines\gold\bottlenecks_local.py

Write-Host "`n✅ Done. Delta tables are under out\delta\{bronze|silver|gold}" -ForegroundColor Green