# Module 6 — Student Manual

**Monitoring, Testing & Drift Detection** | 7 hours total

> This is the **deep-dive manual** for Module 6. It explains what you'll build, the lab sequence, how M6 picks up from M5, and how to run each lab end-to-end. Read this before class. The conceptual "why this tool, why this AWS service" KT is in **[M6_Module_Reference_Guide.md](M6_Module_Reference_Guide.md)** (generated after all labs are content-locked).

> For a one-page repo overview see [README.md](README.md). The branch project briefing lives in [labs/M6_Branch_Airflow_Monitoring/README.md](labs/M6_Branch_Airflow_Monitoring/README.md).

> **Joining at Module 6?** The labs are self-contained — the **real** M3 artifacts ship with this module in [labs/data/](labs/): `reference/final_features.csv` (the M3 Lab B training distribution, 12,308 × 37, regenerated from the committed raw CSVs and round-trip-validated against the model at 0.93 accuracy) and `artifacts/` (the M3 Lab C model + encoder + scaler + metadata). You only need to bring **an AWS account with SNS permissions** (for Lab 1). The M5 ECS service is *nice-to-have*, not required — the labs use a synthetic drifted batch in place of live inference logs. (If you want to rebuild the reference frame yourself, run `Module 3/labs/regenerate_final_features.py`.)

---

## Table of contents

1. [What you'll build](#1-what-youll-build)
2. [Before you start — prerequisites](#2-before-you-start--prerequisites)
3. [Module 6 lab roadmap](#3-module-6-lab-roadmap)
4. [How M6 picks up from M5 (and where it hands off to M7)](#4-how-m6-picks-up-from-m5-and-where-it-hands-off-to-m7)
5. [Lab 1 — SNS topic + alerting setup](#5-lab-1--sns-topic--alerting-setup)
6. [Lab 2 — Evidently AI drift detection](#6-lab-2--evidently-ai-drift-detection)
7. [Lab 3 — Great Expectations data validation](#7-lab-3--great-expectations-data-validation)
8. [Lab 4 — Combined monitoring (exploration notebook)](#8-lab-4--combined-monitoring-exploration-notebook)
8b. [Lab 5 — Production monitoring pipeline (script)](#8b-lab-5--production-monitoring-pipeline-script)
9. [Branch project — Airflow + Docker monitoring](#9-branch-project--airflow--docker-monitoring)
10. [Learning outcomes](#10-learning-outcomes)
11. [Teardown](#11-teardown)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. What you'll build

By the end of Module 6 you'll have:

- An **Amazon SNS topic** (`truck-delay-alerts`) with your email subscribed, ready to fan out to SMS / Lambda / HTTPS endpoints later.
- A **Great Expectations expectation suite** over the Truck Delay feature contract — domain rules on the 28 numeric + 6 categorical columns (within the 128-column frame), which incoming inference batches must satisfy.
- An **Evidently AI drift report** that compares last-week's production features against the M3 training distribution and flags numeric drift (Wasserstein), categorical drift (Jensen-Shannon), and target drift.
- A single **runnable monitoring script** (`run_monitoring.py`) that ingests a batch of production inferences, runs Great Expectations validation, runs Evidently drift, and publishes a structured SNS message whenever either check fails.
- (Branch take-home) A working **Apache Airflow DAG** that runs the same monitoring logic on a schedule — hourly, with retries, with the DAG-level UI showing run history.

This is **spine phase 4**: Truck Delay went from notebook (M3) → containerised (M4) → production-deployed with CI/CD (M5) → **monitored for drift + data quality (M6)** → feature-store-driven (M7) → fully automated SageMaker Pipeline (M8).

The M5 ECS service stays running. M6 doesn't change it — M6 *watches* it.

---

## 2. Before you start — prerequisites

### From earlier modules

| What | Why | Where it lives |
|---|---|---|
| **M3 reference frame** (`final_features.csv`) | The reference distribution for the Evidently drift report — the real M3 Lab B feature frame (12,308 × 37). | **Ships with M6**: `labs/data/reference/final_features.csv` (regenerated via `Module 3/labs/regenerate_final_features.py`, round-trip-validated at 0.93 acc). |
| **M3 trained artifacts** (`xgboost_model.pkl`, `scaler.pkl`, `encoder.pkl`, `model_metadata.json`) | The 128-feature model used in **Lab 2 for prediction drift** + the column contract. | **Ships with M6**: `labs/data/artifacts/` (128 features, F1=0.6697, training_rows=8615). |
| **M5 ECS service running** (optional) | The M5 Streamlit container is self-contained and **does not emit structured per-inference logs**, so the in-class path uses the synthetic drifted batch. Wiring inference logging is an M7/M8 enhancement. | Not required — the simulate path is the default in Labs 2/4/5. |
| **AWS CLI v2 + SNS permissions** | Lab 1 creates the topic and subscription via CLI / Console. | `aws sts get-caller-identity` works; user has `AmazonSNSFullAccess` (or `AdministratorAccess`). |

> **Skipped M3?** No problem — the real reference frame already ships in `labs/data/reference/final_features.csv`. If you ever want to rebuild it from scratch, run `Module 3/labs/regenerate_final_features.py` (it replays M3 Lab B's exact feature engineering on the committed raw CSVs and writes the identical 12,308 × 37 frame). We do **not** use a simplified synthetic substitute.

### Local tooling

| Tool | Why | Install |
|---|---|---|
| Python 3.12.10 | Course standard | https://www.python.org/ |
| `boto3` | SNS publish, optional ECS Logs read | `pip install boto3` |
| `evidently>=0.4,<0.5` | Drift reports. **Pinned** — Evidently's API changed in 0.5; this course teaches the 0.4 API. | `pip install "evidently>=0.4,<0.5"` |
| `great-expectations>=0.18,<1.0` | Data validation. **Pinned** — Great Expectations v1.0 (released late 2024) is a different API surface; we use 0.18 which is the last stable line of the original API. | `pip install "great-expectations>=0.18,<1.0"` |
| Docker Desktop | Branch project (Airflow Compose stack) | https://www.docker.com/products/docker-desktop/ |

The Python pins matter — drift-detection libraries have moved fast and we don't want you debugging API differences. Stick to the pins.

### AWS permissions

For the M6 spine labs you need (in addition to what you already have from M3–M5):

- `AmazonSNSFullAccess` (Lab 1)
- `CloudWatchLogsReadOnlyAccess` (Lab 4 optional ECS log reading)

If you used `AdministratorAccess` for M3–M5 you already have both.

### Estimated cost per session

| Resource | Rate (ap-south-1) | Cost for 4-hour session |
|---|---|---|
| SNS topic | Free (1M publishes/month free) | ₹0 |
| SNS email deliveries | First 1,000/month free | ₹0 for this lab |
| CloudWatch Logs reads | First 5 GB/month free | ~₹0 |
| **Total M6 spine** | | **~₹0** |
| (Optional) keep M5 ECS service running | ~₹3-5/hour | ~₹15-20 |

M6 is essentially **free** if you tear down the M5 ECS service at the end of the M5 session.

---

## 3. Module 6 lab roadmap

| Lab | Title | Format | Duration | What you do |
|---|---|---|---|---|
| **1** | SNS topic + alerting setup | Hands-on (AWS Console + Python) | 45 min | Create the topic, subscribe your email, confirm the subscription, publish a test message from `boto3`. |
| **2** | Evidently AI drift detection | Hands-on (local Python) | 75 min | Load M3 reference features, build a synthetic-but-realistic "production" batch, run Evidently's `Report` with `DataDriftPreset`, inspect the HTML, extract the boolean `dataset_drift` flag for downstream use. |
| **3** | Great Expectations data validation | Hands-on (local Python) | 60 min | Profile the M3 reference features into an expectation suite. Validate a clean batch (passes), validate a corrupted batch (fails). Inspect Data Docs HTML. |
| **4** | Combined monitoring pipeline | Hands-on (local Python) | 60 min | Write `run_monitoring.py` that loads a production batch, runs GE then Evidently, publishes a structured SNS message whenever either check fails. Verify the email arrives. |
| **Branch** | Airflow + Docker monitoring | Take-home (Docker Compose) | 3-4 hours | Spin up Airflow + Postgres via Compose, write a DAG that runs the same checks on the M5 banking churn model every hour, view the DAG in the Airflow UI. |

Total in-class time: ~4 hours of labs + ~1 hour conceptual drift discussion + ~1 hour branch briefing + 1 hour Q&A/buffer = 7 hours.

### 3-hour fast-track (compressed delivery)

If the session is a flat 3 hours, run the labs as **"execute the provided cells + interpret"** rather than "type everything", and move the optional/advanced blocks to take-home. All five spine labs ship complete, runnable code, so this works without losing the core learning:

| Block | Time | What's in / what's cut |
|---|---|---|
| **Lab 1 — SNS** | 30 min | Topic + email subscribe + confirm + one `boto3` publish. **Cut to take-home:** Slack-via-Lambda, the alert-hygiene deep dive. |
| **Lab 2 — Evidently (notebook)** | 50 min | Run the drift report inline + read the `dataset_drift` boolean. **Cut:** prediction-drift (Step 7), threshold-tuning. Fold the conceptual drift lecture into Step 1 here. |
| **Lab 3 — Great Expectations (notebook)** | 35 min | Profile → validate clean → validate corrupt. **Demo (don't have everyone build):** Data Docs HTML. |
| **Lab 4 — Combined (notebook)** | 35 min | Compose GE → Evidently → SNS; run all three paths in `--dry-run`. |
| **Lab 5 — Production script** | 15 min | Run `run_monitoring.py --simulate --dry-run` + `--simulate-corrupt`. **Take-home:** scheduling (cron/Airflow/EventBridge). |
| **Buffer / Q&A** | 5 min | |

The **conceptual drift lecture** merges into Lab 2 Step 1; the **Airflow branch**, the **Slack Lambda**, **Data Docs authoring**, and **prediction-drift** all become take-home. Hard prerequisite for 3 hours: students arrive with a Python 3.12.10 venv created, the `labs/data/` folder in place (it ships with the module — the real `final_features.csv` + model artifacts), and the SNS email subscription confirmed — chasing confirmation links eats 10 minutes otherwise. The per-lab "Prerequisites" blocks list exactly what to pre-stage.

---

## 4. How M6 picks up from M5 (and where it hands off to M7)

```
M5 endpoint:
    ECS service running the Truck Delay container behind an ALB
    Every push to main → rolling deploy
        └── Predictions flowing; no one watching them

M6 spine progression:
    Lab 1:  SNS topic for drift alerts; on-call email subscribed
    Lab 2:  Evidently drift against M3 baseline → "is the input data still in-distribution?"  (notebook)
    Lab 3:  Great Expectations → "is each batch schema-correct?"  (notebook)
    Lab 4:  Compose them interactively: GE → Evidently → SNS publish on either failure  (notebook)
    Lab 5:  Ship it: run_monitoring.py — exit-code CLI for cron / Airflow / EventBridge / SageMaker  (script)
                                                                            ↓
M7 will:
    Move the M3 feature engineering into Hopsworks Feature Store. The same
    monitoring scripts from M6 will then read FEATURE VECTORS from Hopsworks
    instead of from the raw CSV joins, so drift detection runs against
    feature-store-served data instead of ad-hoc preprocessing.
```

The spine is **continuous** — the same Truck Delay model + ECS service runs through M5, M6, M7, M8. M6 is where it *gets observed* for the first time.

The Branch project does NOT change the spine state. It demonstrates the same monitoring pattern under Airflow + Docker on a different domain (banking churn).

---

## 5. Lab 1 — SNS topic + alerting setup

**File:** [labs/M6_Lab_1_SNS_Alerting_Setup.md](labs/M6_Lab_1_SNS_Alerting_Setup.md)

SNS is the AWS pub-sub bus. You publish a message to a *topic*; AWS fans it out to every *subscriber* (email, SMS, Lambda, SQS, HTTPS).

You'll:
1. Create the `truck-delay-alerts` topic
2. Subscribe your email; confirm via the verification link AWS emails you
3. Publish a hand-rolled test message from the Console
4. Publish a structured JSON message from a `boto3` Python script
5. (Optional) Add a second subscription: a Lambda function that posts to Slack

**Output:** a working SNS topic + confirmed email subscription. Every subsequent lab in M6 publishes to this topic.

> **Why we do this first:** the alerter is the most "dumb" piece — no ML, no statistics. Getting it working in isolation means when Lab 4 fails-to-alert, we know the bug is in the drift logic, not the plumbing.

---

## 6. Lab 2 — Evidently AI drift detection (notebook)

**File:** [labs/M6_Lab_2_Evidently_Drift_Detection.ipynb](labs/M6_Lab_2_Evidently_Drift_Detection.ipynb)

**Evidently** computes statistical distance between two distributions (a "reference" and a "current"). For numeric features it uses the **Wasserstein distance**; for categorical features, **Jensen-Shannon divergence**. The output is per-column drift flags + a dataset-level boolean. This is an **exploration notebook** — you render the report inline, tune thresholds, and re-run.

You'll:
1. Load the **real** M3 reference frame — `data/reference/final_features.csv` (**12,308 × 37**, the M3 Lab B output that ships with this module). No synthetic stand-in.
2. Synthesise a "current" batch with realistic monsoon drift (precip/humidity up, fleet aging, more heavy-rain routes, more delays) — the *only* synthetic step.
3. Run an Evidently `Report` with `DataDriftPreset` + `TargetDriftPreset` and render it inline.
4. Load the **real** M3 XGBoost model + encoder + scaler (`data/artifacts/`) and detect **prediction drift**.
5. Extract `dataset_drift: true/false` + `drift_share` — the signal Lab 4's alerter consumes. Save `drift_report.html` + `drift_metrics.json`.

**Output:** an inline + saved drift report (HTML) + the machine-readable JSON metrics.

---

## 7. Lab 3 — Great Expectations data validation (notebook)

**File:** [labs/M6_Lab_3_Great_Expectations_Validation.ipynb](labs/M6_Lab_3_Great_Expectations_Validation.ipynb)

**Great Expectations** ≠ Evidently. Evidently asks "is the distribution shifting?" — *statistical*. GE asks "does this batch satisfy the schema contract?" — *declarative*. You need both: drift is subtle and statistical; corruption (NULLs, out-of-range, wrong types) is binary and schema-level.

You'll:
1. Auto-profile the **real** reference frame into an expectation suite (`truck_delay_features.json`), then hand-add domain rules:
   - `truck_age` between 1 and 30, `age` 18–75, `experience` 0–60
   - `delay`, `accident`, `is_midnight` in {0, 1}
   - `fuel_type` / `gender` / `driving_style` enums (incl. the legal `'Unknown'` NaN-fill value)
   - cross-column: `age ≥ experience`
2. Validate a clean batch (the reference) — ~100% pass.
3. Validate a deliberately corrupted batch (negative truck age, NULL ratings, `'hydrogen'` fuel, `delay=2`) — fails with row-level errors.
4. Generate **Data Docs** (the HTML schema contract) + the structured failure payload Lab 4/5 publish.

**Output:** the JSON expectation suite (in `./great_expectations/`) + a Data Docs HTML browser.

---

## 8. Lab 4 — Combined monitoring (exploration notebook)

**File:** [labs/M6_Lab_4_Combined_Monitoring_Pipeline.ipynb](labs/M6_Lab_4_Combined_Monitoring_Pipeline.ipynb)

The glue lab — but **interactive**. You compose the two checks (GE → Evidently) and the SNS alerting into one decision and watch each piece fire, all in functional, no-class style:

1. `run_ge_validation(df)` — validate against the Lab 3 suite (cheap, fail-fast).
2. `run_drift_detection(ref, cur)` — Evidently data + target drift.
3. `route_severity()` + `build_alert_payload()` — severity routing + a structured **dict** payload (no classes).
4. `publish_alert(payload, dry_run)` — SNS publish, dry-run by default.
5. `run_monitoring(current)` — orchestrate with an exit-code contract; you exercise all three paths (drift, schema-failure, healthy).

**Output:** a proven monitoring flow you understand piece-by-piece — ready to ship as a script in Lab 5.

---

## 8b. Lab 5 — Production monitoring pipeline (script)

**Folder:** [labs/M6_Lab_5_Production_Pipeline/](labs/M6_Lab_5_Production_Pipeline/)

Lab 4's functions, lifted into a runnable **production `.py` package** — `run_monitoring.py` with an `argparse` CLI, env-driven `config.py`, and an exit-code contract (0 healthy / 1 alert / 2 config-error) so the same code runs under cron, Airflow, EventBridge, or a SageMaker Processing step.

```bash
python run_monitoring.py --simulate --dry-run       # synthetic drift, prints payload
python run_monitoring.py --simulate-corrupt         # GE fails first (critical)
python run_monitoring.py --current batch.parquet    # a real production dump
```

The reference frame + model are **required** (the real M3 artifacts in `data/`) — the script errors out rather than fabricating them.

**Output:** `run_monitoring.py` — the single artifact the Airflow branch, M7, and M8 reuse and schedule.

---

## 9. Branch project — Airflow + Docker monitoring

**Folder:** [labs/M6_Branch_Airflow_Monitoring/](labs/M6_Branch_Airflow_Monitoring/)

**Format:** Take-home (3-4 hours). Self-paced.

You'll re-implement the M6 spine monitoring on a **different model** (banking churn from M5's Branch) using a **workflow scheduler** (Airflow) instead of "run the script manually". Airflow gives you scheduling, retries, the UI, and a real production-grade orchestration story.

**What's included in the branch folder:**
- A trained Customer Churn classifier (from the M5 Branch — Random Forest, 8 features)
- A simulated stream of "new daily customer records" (CSV slices)
- A `docker-compose.yml` that spins up Airflow webserver + scheduler + Postgres
- A DAG (`monitoring_dag.py`) with three tasks: GE validate → Evidently drift → publish to SNS

**Why Airflow?** When monitoring becomes "run this every hour, retry twice on transient failures, page the on-call if it fails three times in a row, keep 90 days of run history" — that's exactly what Airflow exists for. The `run_monitoring.py` script from **Lab 5** is fine for a `cron` job. Airflow is what you reach for when scheduling becomes its own engineering problem.

Full briefing in [labs/M6_Branch_Airflow_Monitoring/README.md](labs/M6_Branch_Airflow_Monitoring/README.md).

---

## 10. Learning outcomes

By the end of M6 you can:

**Drift theory:**
1. Distinguish **data drift**, **concept drift**, and **label drift** with one-line definitions and a real example of each
2. Explain when a Wasserstein test is preferable to a KS test, and when Jensen-Shannon beats chi-squared
3. State two reasons drift detection is *not* the same as data validation, and why production needs both

**Evidently AI:**
4. Build an Evidently `Report` with the standard ML presets (`DataDriftPreset`, `TargetDriftPreset`, `RegressionPreset` / `ClassificationPreset`)
5. Extract machine-readable drift metrics from a report and use them in a downstream `if/else`
6. Tune drift thresholds for noisy features so you don't get alert fatigue

**Great Expectations:**
7. Auto-profile a reference frame into an expectation suite as a starting point
8. Hand-edit the suite to add domain rules (`expect_column_values_to_be_in_set`, custom expectations) the profiler can't infer
9. Generate Data Docs and serve them as the team's "data contract" reference

**SNS + Python alerting:**
10. Create + subscribe to an SNS topic from Console and CLI
11. Publish a structured JSON message from boto3 with `MessageAttributes` for filtering
12. Reason about alert hygiene: severity levels, dedup windows, alert routing

**End-to-end:**
13. Run a monitoring script standalone, then under Airflow scheduling
14. Compare scheduling options (cron, Airflow, EventBridge Scheduler) and pick one for a given team

---

## 11. Teardown

M6 spine: essentially free. The SNS topic costs ₹0 idle. Tear down only if you want a clean account.

```bash
# 1. (Optional) Delete the SNS subscription
aws sns list-subscriptions \
    --query "Subscriptions[?TopicArn=='arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts'].SubscriptionArn" \
    --output text \
    | xargs -I {} aws sns unsubscribe --subscription-arn {}

# 2. (Optional) Delete the SNS topic
aws sns delete-topic --topic-arn arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts

# 3. (Branch only) Stop the Airflow Compose stack
cd labs/M6_Branch_Airflow_Monitoring
docker compose down -v          # -v also removes the volumes
```

**M5 ECS service**: independent of M6. Tear it down whenever you're done with M5/M6 (it costs ~₹3-5/hour while running). M7 doesn't need it.

> **🪟 Windows users:** the `xargs -I {}` patterns are bash-only. On native PowerShell, capture the ARN into a variable first: `$arn = aws ...; aws sns unsubscribe --subscription-arn $arn`.

---

## 12. Troubleshooting

| Symptom | Diagnosis | Fix |
|---|---|---|
| SNS subscription says "Pending confirmation" forever | You haven't clicked the AWS confirmation link in your email | Check spam folder; the email is from `no-reply@sns.amazonaws.com` |
| `boto3.client("sns").publish(...)` returns 403 | IAM user lacks `sns:Publish` | Attach `AmazonSNSFullAccess` (or scoped `sns:Publish` on this topic only) |
| Evidently `Report.run()` errors: "AttributeError: 'DataFrame' has no attribute 'iteritems'" | pandas 2.x vs Evidently 0.3.x | Upgrade Evidently to `>=0.4` (we pin to `>=0.4,<0.5`) |
| Evidently HTML report is blank | Pandas categorical columns with empty-string levels confuse it | Ensure `df[cat_cols].astype(str).replace("", "unknown")` before passing |
| Great Expectations `create_expectation_suite` errors: "no module named `great_expectations.cli`" | You installed `great-expectations>=1.0` (different API) | `pip install "great-expectations>=0.18,<1.0"` |
| Great Expectations Data Docs page is 404 | Default `data_docs_sites` not configured | Run `great_expectations docs build` after validating |
| Airflow scheduler container exits immediately | The container can't reach the Postgres metadata DB | Check `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` in `docker-compose.yml`; default port is 5432 |
| Airflow DAG shows `broken DAG` | DAG import error (path / dependency) | `docker compose logs scheduler | tail -50` shows the Python traceback |

For deeper issues, the lab files have per-lab troubleshooting tables at the bottom.
