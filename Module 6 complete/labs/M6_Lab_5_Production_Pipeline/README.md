# M6 · Lab 5 — Production Monitoring Pipeline

**Module 6 — Monitoring, Testing & Drift Detection | Spine Project: Truck Delay Classification**

This is the **production** form of the monitor you explored interactively in Labs 2–4. The notebook was for *seeing*
each piece; this package is for *running* it on a schedule. It composes Great Expectations (schema) + Evidently (drift)
into one decision with a clean **exit-code contract**, and publishes a structured alert to the **SNS topic from Lab 1**.

> **Notebook → script.** Every function here is lifted, almost verbatim, from the Lab 4 notebook — same logic, now
> packaged with a CLI, env-driven config, and an exit code so cron / Airflow / EventBridge / SageMaker can run it.

---

## What it does

```
GE validate (cheap, fail-fast)   ──fail──▶  publish CRITICAL alert ──▶ exit 1
        │ pass
        ▼
Evidently drift (expensive)      ──drift─▶  publish INFO|WARNING|CRITICAL ──▶ exit 1
        │ no drift
        ▼
                                  log success ──▶ exit 0
```

**Why GE first:** it's ~50 ms vs Evidently's ~3–5 s, and a drift report on structurally broken data is meaningless.
Fail fast on the cheap check.

## Files (all functional — no classes)

| File | Responsibility |
|------|----------------|
| `run_monitoring.py` | CLI entry point: `argparse` + `main()` → exit code |
| `config.py` | env-driven config (paths, `TOPIC_ARN`, region, severity thresholds) |
| `data_sources.py` | load the **real** reference frame + produce a current batch (file / simulate / simulate-corrupt) |
| `checks.py` | `run_ge_validation()`, `run_drift_detection()` |
| `alerting.py` | `route_severity()`, `build_alert_payload()` (a dict), `publish_alert()` |
| `requirements.txt` | pinned deps (Python 3.12.10) |

## Prerequisites (real artifacts — nothing synthetic)

- **Reference frame + model**: `../data/reference/final_features.csv` + `../data/artifacts/` — the genuine M3 Lab B/C
  outputs that ship with Module 6. The pipeline **errors out** (it does not fabricate) if they're missing.
- **GE suite**: `../great_expectations/expectations/truck_delay_features.json` — created by running **Lab 3** in
  `Module 6/labs/`.
- **SNS topic** (optional): from Lab 1. Without it, alerts print instead of paging.

## Setup

```bash
# Python 3.12.10
python -m venv .venv
# 🪟 Windows PowerShell:  .venv\Scripts\Activate.ps1
# 🍎 macOS / 🐧 Linux:     source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
# 1) Dry-run on a synthetic drifted batch — prints the alert payload, publishes nothing
python run_monitoring.py --simulate --dry-run

# 2) Synthetic schema-corruption batch — GE fails first (critical), drift never runs
python run_monitoring.py --simulate-corrupt --dry-run

# 3) A real production dump
python run_monitoring.py --current production_batch.parquet

# 4) For real alerts: point at the Lab 1 topic, then drop --dry-run
export TOPIC_ARN=arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts   # 🪟 $env:TOPIC_ARN="..."
python run_monitoring.py --simulate
```

Check the exit code: `echo $?` (bash) / `echo $LASTEXITCODE` (PowerShell). `0` = healthy, `1` = alert, `2` = config error.

## Schedule it — three options (same script, different wrapper)

| Option | Best for | How |
|--------|----------|-----|
| **cron** | solo / throwaway | `0 * * * * cd /path && python run_monitoring.py --simulate >> monitor.log 2>&1` |
| **Apache Airflow** | real ops (retries, history, UI) | the **M6 Branch project** does exactly this for a Banking-Churn model — see `../M6_Branch_Airflow_Monitoring/` |
| **EventBridge + Lambda** | all-in-AWS, no scheduler box | wrap the call in a Lambda (suite + reference in S3) + a cron-like rule — **M8** wires this up |

## Where this goes next

- **Branch project (Airflow)** — same monitor, scheduled, different domain (Banking Churn).
- **M8 capstone** — this becomes a **SageMaker Processing step**; the drift/GE result gates whether the pipeline retrains.

## Conventions
- Python 3.12.10 · functional style (no classes) · env-driven config · exit-code contract · `--dry-run` safe by default.
