# Module 6 — Module Reference Guide

**The AWS services and OSS tools in M6: what they are, why they're there, and how they fit together as one observability + alerting system for a deployed ML model.**

> This is the **conceptual KT document** for Module 6. [M6_Student_Manual.md](M6_Student_Manual.md) tells you what to do step by step. The [lab files](labs/) tell you exactly how to click and what to type. **This document tells you why** — so when an interviewer asks "Why do you need both Evidently and Great Expectations?" or "Why is drift detection a real problem and not over-engineering?", you can answer from first principles.

> Read this once before Lab 1. Refer back to the **decision tree** (§5) when you can't remember which tool catches which class of failure. Refer to **interview justifications** (§6) before any monitoring / observability interview.

---

## Table of contents

1. [The one-paragraph problem M6 solves](#1-the-one-paragraph-problem-m6-solves)
2. [The monitoring lifecycle as a mental model](#2-the-monitoring-lifecycle-as-a-mental-model)
3. [Tool-by-tool KT (in pipeline order)](#3-tool-by-tool-kt-in-pipeline-order)
4. [The detection sequence — what runs when, in order](#4-the-detection-sequence--what-runs-when-in-order)
5. [The decision tree — when to use what](#5-the-decision-tree--when-to-use-what)
6. [Interview-ready justifications](#6-interview-ready-justifications)
7. [Tools NOT in M6 (and which module they arrive in)](#7-tools-not-in-m6-and-which-module-they-arrive-in)
8. [15 interview questions (with hints)](#8-15-interview-questions-with-hints)

---

## 1. The one-paragraph problem M6 solves

Coming out of M5, the Truck Delay model is in production behind an ALB. It serves predictions hour after hour. **Nobody knows whether those predictions are still any good.** The model could be making wildly off predictions and the only feedback channel is "ops staff complain". By the time you hear the complaint, three weeks of bad scheduling decisions have already happened. Module 6 closes that gap: it gives the deployed model a continuous health check that compares incoming inference data against the training distribution (drift) and the schema contract (validation), and pages on-call within minutes of either check failing. Every tool introduced in M6 — Evidently, Great Expectations, SNS, Airflow — solves one specific kind of failure in that loop, and they only make sense if you can name which kind.

---

## 2. The monitoring lifecycle as a mental model

A production ML monitoring system has **five phases**. Every tool in M6 belongs to one of them. If you can name a tool's phase, you can defend its presence.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│   Phase 1            Phase 2            Phase 3            Phase 4           │
│   ───────            ───────            ───────            ───────           │
│   CAPTURE      ─►    VALIDATE     ─►    DETECT       ─►    DECIDE            │
│                                                                              │
│   Production         Great             Evidently AI       Severity logic     │
│   inference logs     Expectations      (drift            (drift_share        │
│   from M5 ECS        (schema           statistical        thresholds         │
│   (CloudWatch)       contract)         distances)        + alert routing)    │
│                                                                              │
│                                                              │               │
│                                                              ▼               │
│                                                                              │
│   Phase 5                                                                    │
│   ───────                                                                    │
│   ALERT                                                                      │
│                                                                              │
│   Amazon SNS topic                                                           │
│   → email                                                                    │
│   → SMS                                                                      │
│   → Lambda → Slack                                                           │
│   → SQS → downstream system                                                  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

Above all phases (cross-cutting):
   Orchestration — cron / Airflow (Branch) / EventBridge Scheduler (M8)
```

Every M6 lab is "fill in one phase":

| Lab | Fills phase | Result |
|---|---|---|
| **Lab 1** | Phase 5 — Alert | SNS topic + confirmed email subscription |
| **Lab 2** | Phase 3 — Detect (drift) | Evidently `Report` produces `dataset_drift: bool` + `drift_share: float` |
| **Lab 3** | Phase 2 — Validate (schema) | GE suite `truck_delay_features` produces `success: bool` + row-level failures |
| **Lab 4** | Phases 1-5 wired end-to-end | `run_monitoring.py` script that ingests a batch, runs GE → Evidently → SNS |
| **Branch** | All phases under Airflow orchestration | Hourly DAG monitoring the M5 Branch Churn model |

---

## 3. Tool-by-tool KT (in pipeline order)

Each section follows the same format as the M5 guide:

- **What it is** — one-line definition
- **Problem it solves in M6** — the specific failure it prevents
- **What hurts without it** — the failure mode you'd hit if you skipped it
- **Why this tool vs alternatives** — the choice you're defending
- **Where in M6** — exact lab reference
- **What it depends on / what depends on it**

### 3.1 CloudWatch Logs (carried over from M5, used as input)

| Field | Value |
|---|---|
| **Phase** | 1 — Capture |
| **What it is** | AWS managed log aggregation — the ECS service writes container stdout/stderr here automatically |
| **Problem it solves in M6** | "Where do production inference inputs live so we can monitor them?" |
| **Without it** | You'd have to add a separate logging sink to your container, or read from a database, or buffer in S3 — all extra plumbing for what is essentially "tail stdout from a running container" |
| **Why CloudWatch Logs vs S3 vs Kinesis** | Already enabled in M5 for free. Logs Insights queries are good enough at this volume (~1000 inferences/hour). At higher volume (millions of inferences/day), you'd shift to Kinesis Firehose → S3 → Athena. |
| **Where in M6** | Lab 4 Step 7 (optional) shows the boto3 CWL query pattern; Airflow Branch uses a local simulator instead because not everyone has the live M5 ECS service running |
| **Depended on by** | The detection layer (Evidently, GE) needs *something* to monitor |

### 3.2 Great Expectations (GE)

| Field | Value |
|---|---|
| **Phase** | 2 — Validate |
| **What it is** | A declarative tabular-data validation framework. You write "this column must satisfy expectation X" once; every incoming batch is checked. |
| **Problem it solves in M6** | "Has the schema broken? NULLs spiked? Types changed? Values gone out of range?" |
| **Without it** | A schema break (e.g., upstream service now emits `route_id` as string instead of int) silently passes garbage to the model. The model doesn't crash — it returns low-confidence predictions that look fine. By the time you notice, a week of bad decisions are on the books. |
| **Why GE vs Pydantic vs pandera vs custom validators** | GE has 50+ built-in expectations (you'd reinvent them with Pydantic). GE has Data Docs (auto-generated HTML schema-contract pages). GE has profilers that auto-derive starting suites from reference data. pandera is great for type-checking dataframes inline; GE is great for *batch-level acceptance gates* with persistent results. Use both in different layers. |
| **Where in M6** | Lab 3 (full walkthrough); Lab 4 (called as a function); Branch (the same suite under Airflow) |
| **Depends on** | A reference frame to profile from |
| **Depended on by** | Lab 4's pipeline (GE runs first, fail-fast) |

**The key conceptual distinction**: GE answers "**Does this batch satisfy the contract?**" — a binary, point-in-time, declarative question. It catches *schema-level* failures. It does **not** catch slow statistical drift — that's Evidently's job.

### 3.3 Evidently AI

| Field | Value |
|---|---|
| **Phase** | 3 — Detect (drift) |
| **What it is** | A statistical-drift report generator. Compares a "reference" distribution against a "current" distribution and reports per-feature drift flags + a dataset-level boolean. |
| **Problem it solves in M6** | "Has the production data distribution slowly shifted away from the training distribution?" |
| **Without it** | Subtle drifts go undetected for months. Monsoon shifts `route_avg_precip` by 30% but no individual value is "wrong" — GE doesn't fire. Model accuracy degrades silently until ground-truth labels arrive weeks later and the F1 dashboard shows the slide. |
| **Why Evidently vs WhyLabs vs Arize vs nannyML vs a custom KS-test loop** | Evidently is open-source, runs locally (no SaaS lock-in), has sensible presets (`DataDriftPreset` picks the right stat test per feature type), produces nice HTML + JSON. WhyLabs / Arize are SaaS-hosted (better for large teams, more expensive). nannyML adds estimated-performance-without-labels (cutting-edge but newer). A custom KS-test loop is what you'd write if you needed to monitor 5 features — once you hit 30+, use a library. |
| **Where in M6** | Lab 2 (full walkthrough); Lab 4 (called as a function); Branch (same logic under Airflow) |
| **Depends on** | A reference frame; a current batch; both with the same columns |
| **Depended on by** | Lab 4's pipeline (Evidently runs after GE passes) |

**The key conceptual distinction**: Evidently answers "**Is the distribution drifting?**" — a statistical, continuous, threshold-driven question. It catches *distribution-level* shifts. It does **not** catch a single corrupted row — that's GE's job.

### 3.4 Amazon SNS (Simple Notification Service)

| Field | Value |
|---|---|
| **Phase** | 5 — Alert |
| **What it is** | AWS managed pub-sub. You publish a message to a *topic*; AWS fans it out to every confirmed *subscriber* (email, SMS, Lambda, SQS, HTTPS). |
| **Problem it solves in M6** | "When a drift / validation check fails, how does the message reach the human (or system) who needs to act?" |
| **Without it** | The detector logs a warning to stdout. Nobody reads stdout. The alert dies in the log. (This was exactly Priya's team's failure mode pre-M6.) |
| **Why SNS vs sending email directly vs PagerDuty vs Slack webhooks** | SNS decouples the *publisher* from the *subscribers*. The detector publishes once; downstream you can add/remove/swap subscribers (email → SMS → PagerDuty → Slack via Lambda) without touching the detector code. Direct email is rigid; PagerDuty/Slack are great endpoints — SNS gives you the *fanout layer in front of them*. |
| **Where in M6** | Lab 1 (full walkthrough); Lab 4 (publisher); Branch (same topic shared) |
| **Depends on** | IAM permissions to publish; at least one confirmed subscriber |
| **Depended on by** | The alerter pattern is the binding contract between the detector and the on-call rotation |

**The key SNS concept**: it's a **bus**, not a queue. Once a message is published, all confirmed subscribers receive it. There's no "consumer offset" — every subscriber sees every message. For queue semantics (one consumer, ordered, replayable), use SQS or Kinesis instead.

### 3.5 boto3 (the publisher SDK)

| Field | Value |
|---|---|
| **Phase** | 5 — Alert (the wire between detector and SNS) |
| **What it is** | The AWS Python SDK. `boto3.client("sns").publish(...)` is two lines of code. |
| **Problem it solves in M6** | "How does Python code in the monitoring pipeline call SNS?" |
| **Without it** | You'd write your own AWS SigV4 request signer (don't). |
| **Where in M6** | Lab 1 Step 4 (`sns_publish_test.py`); Lab 4 (`publish_alert(...)`); Branch (`plugins/monitoring_utils.publish_alert`) |
| **Tradeoffs vs aws-cli** | Same auth backend (`~/.aws/credentials` or env vars); boto3 wins in Python apps, aws-cli wins in shell scripts. |

### 3.6 Three forward-references the labs mention but don't deeply use

| Tool | What it is | Why mentioned now | First real use |
|---|---|---|---|
| **AWS Lambda** | Serverless function runtime | Lab 1 Step 5 (optional) shows an SNS → Lambda → Slack fanout sketch — to expose the *idea* that SNS subscribers can be code, not just humans | **M8 capstone** — Lambda wraps the M6 monitoring script as a SageMaker Pipeline trigger |
| **CloudWatch Logs Insights** | A query language for log groups | Lab 4 Step 7 shows how to pull production batches from the ECS log stream via Logs Insights | M8 uses Insights queries inside Lambda |
| **EventBridge Scheduler** | Serverless cron — the AWS-native alternative to Airflow | Lab 4 Step 8 + Branch comparison — mentioned as the M8 scheduling path | M8 capstone |

---

## 4. The detection sequence — what runs when, in order

This is the chronological narrative for one run of the M6 monitoring pipeline. Memorise it.

```
0. PREREQUISITE (one-time setup, before any scheduling)
   - SNS topic 'truck-delay-alerts' created (Lab 1)
   - Reference frame 'final_features.csv' — the real M3 Lab B frame, shipped in labs/data/reference/ (Lab 2)
   - Great Expectations suite 'truck_delay_features.json' authored (Lab 3)
   - Monitoring script 'run_monitoring.py' packaged (Lab 5)

1. INPUT (each hour)
   - Production batch arrives -- 500-5000 rows from one of three sources:
       (a) CloudWatch Logs Insights query against /ecs/truck-delay-service
       (b) S3 dump written by the M5 ECS container

2. GE VALIDATE (fast -- ~50ms on 500 rows)
   - Each row checked against ~142 expectations from the suite
   - Result: success: bool + per-failure row-level details
   - If FAIL -> jump to step 5 with alert_type='ge_validation', severity='critical'

3. EVIDENTLY DRIFT (slow -- ~3-5 sec on 1000 rows x 30 features)
   - Compares each numeric feature using Wasserstein distance
   - Compares each categorical feature using Jensen-Shannon divergence
   - Computes target-drift (P(y) shift) if labels are available
   - Result: dataset_drift: bool + drift_share: float + per-column scores
   - If DRIFT -> jump to step 4 with alert_type='drift'
   - If NO DRIFT -> exit 0, no alert

4. SEVERITY ROUTING (for drift alerts)
   - drift_share > 50%  ->  severity = 'critical'
   - drift_share > 30%  ->  severity = 'warning'
   - otherwise          ->  severity = 'info'
   (GE failures are always 'critical' -- a broken contract is never informational)

5. SNS PUBLISH
   - Build structured JSON payload (schema_version, alert_type, severity, service,
     environment, detected_at, summary, details, runbook_url)
   - boto3.client('sns').publish(TopicArn=..., Subject=..., Message=..., MessageAttributes=...)
   - Subscribers fan out:
       email     -> Priya gets the JSON body
       Lambda    -> formats to Slack blocks -> #ml-monitoring
       (optional SMS, SQS, HTTPS)

6. EXIT CODE
   - 0 = healthy (no publish)
   - 1 = alert published (Airflow / EventBridge marks the run successful nonetheless --
         the *check* ran fine; it just found a problem)
   - 2 = pipeline error (missing env var, can't reach SNS, etc.) -- Airflow retries

7. WHEN SCHEDULED
   - Run every hour via Airflow @hourly (Branch) or cron (Lab 4 standalone)
   - 90 days of run history visible in Airflow UI / CloudWatch / log files
   - Failure of the *scheduler itself* triggers a meta-alert (DAG-level on_failure_callback
     or EventBridge dead-letter queue)
```

If you can recite this without looking, you understand M6.

---

## 5. The decision tree — when to use what

These are the forks you'll hit when an interviewer asks you to design monitoring for a deployed ML model.

### Fork 1: Do I need drift monitoring at all?

```
Is the model deployed in production with non-stationary inputs?
├─ NO (e.g., batch reports on historical data, fixed-distribution synthetic env)
│   → No drift monitoring needed. Standard validation + unit tests cover you.
└─ YES (real-world data feeding live predictions)
    → You need drift monitoring. The only question is which tool.
```

### Fork 2: GE alone, Evidently alone, or both?

```
What kind of failure modes do you fear most?
├─ Schema breaks (upstream service changes; NULL spikes; type changes)
│   → Great Expectations -- alone is enough
├─ Distribution shifts (seasonal trends; population drift; market changes)
│   → Evidently -- alone is enough
└─ Both kinds (the realistic answer for any non-trivial production system)
    → Both -- in series, GE first (fail-fast, cheap), Evidently second
```

### Fork 3: Evidently vs WhyLabs vs Arize vs custom

```
Is it a solo / small-team project where SaaS cost is a real concern?
├─ YES -> Evidently (open-source, self-hosted)
└─ NO
    Do you have >10 models to monitor and want a unified observability platform?
    ├─ YES -> WhyLabs or Arize (SaaS dashboards across all models)
    └─ NO  -> Evidently is still fine; revisit at scale
```

### Fork 4: SNS vs PagerDuty vs Slack-webhook-direct

```
Do you need on-call paging escalation (SMS at 2 AM, PagerDuty schedules)?
├─ YES -> SNS -> PagerDuty integration (SNS as the AWS-side bus; PD as the on-call layer)
└─ NO
    Slack-only suffices?
    ├─ YES -> Slack webhook directly from the script (simplest); OR SNS -> Lambda -> Slack
    └─ Email suffices?
        └─ SNS -> email (Lab 1's setup)
```

### Fork 5: cron vs Airflow vs EventBridge Scheduler

```
Single laptop / dev sandbox?
├─ cron is fine

Real team with multiple pipelines, retries, audit history?
├─ Airflow (self-hosted, MWAA, Astronomer)

All-in-AWS team that doesn't want to maintain a scheduler?
├─ EventBridge Scheduler + Lambda (M8 path)
```

### Fork 6: Where does the reference distribution live?

```
M6 path  -> A parquet file on disk (lightweight, manual)
M7 path  -> Hopsworks Feature Store reference window (durable, versioned)
M8 path  -> Same as M7 plus SageMaker Pipelines triggers retraining when drift exceeds threshold
```

This is the spine progression — same monitoring tools, increasingly durable upstream of them.

---

## 6. Interview-ready justifications

Sample answers to questions you *will* get. Adapt them to your project but keep the structure: **problem → choice → reason → tradeoff acknowledged**.

### Q: "Why do you need both Evidently and Great Expectations? Isn't that redundant?"

> "They answer different questions. Great Expectations answers 'does this batch satisfy the schema contract?' — a binary, declarative check that catches NULL spikes, type breaks, out-of-range values. Evidently answers 'has the distribution shifted statistically?' — a continuous threshold-driven check that catches monsoon-style population drift. GE catches what would *break* my code; Evidently catches what would *degrade* my model. A real system needs both because the failure modes are different in kind. I run GE first (cheap, ~50ms, fail-fast on a single bad row) and Evidently second (expensive, ~3 sec on a thousand rows, only meaningful if the batch is structurally valid)."

### Q: "Walk me through what happens when drift is detected."

> *Recite section 4 verbatim*. "Production batch lands (CloudWatch Logs / S3 dump / synthetic). GE validates — passes. Evidently runs DataDriftPreset, Wasserstein per numeric feature, Jensen-Shannon per categorical. dataset_drift comes back True with drift_share = 0.42. Severity router sees 42% > 30% warning threshold → severity = 'warning'. boto3 publishes a structured JSON message to the SNS topic with Subject = '[WARNING] truck-delay-classifier drift'. Email subscribers see the body; a Lambda subscriber formats it for Slack; an optional SMS subscriber pages the on-call. Script exits 1 — alert was sent, but the *check* ran fine, so Airflow records a green run."

### Q: "Why SNS rather than just calling Slack webhook directly?"

> "Decoupling. The detector publishes to one topic — that's it. Subscribers can be added, removed, or swapped without touching the detector code. Tomorrow when finance asks for an alert via email but engineering wants it in #ml-monitoring on Slack and the SRE team wants it via PagerDuty for any critical alert — that's three subscribers behind the same publish. If I had hardcoded the Slack webhook, every new consumer requires a detector change + a redeploy. SNS is the *fanout layer*. The downside: one extra hop, one extra service to learn. For one consumer it's overkill; for any production system it's worth it."

### Q: "What's the difference between data drift, concept drift, and label drift?"

> "Data drift means P(X) — the input distribution — has changed, while P(y|X) is unchanged. Example: monsoon brings more cold-weather routes, but the relationship between weather and delay is the same. Caught by Evidently's DataDriftPreset without needing labels. Label drift means P(y) shifts — overall positive rate goes from 35% to 60%. Caught by Evidently's TargetDriftPreset, also label-free. Concept drift is the dangerous one: P(y|X) has changed — same inputs now correspond to different outputs. Example: new highway tolls change route timing, so 'low traffic + short distance' no longer reliably means 'on-time'. You can't detect concept drift without fresh labels — there's nothing in the input distribution that tells you the *mapping* has changed. Practical pattern: catch data + label drift in real time as a *warning*, then schedule a periodic label-aware evaluation (M8) to confirm concept drift."

### Q: "Your monitoring pipeline runs hourly. What happens if SNS is down at 2 AM?"

> "Three lines of defence. First, boto3's default retry policy retries idempotent operations 3 times with exponential backoff — that handles transient blips. Second, the monitoring script exits with code 2 on publish failure, distinct from exit 1 (alert sent). The scheduler (Airflow in the Branch, EventBridge in M8) treats exit 2 as a task failure and retries the *whole* run twice with 5-min backoff. Third, if all retries fail, Airflow's `on_failure_callback` triggers a meta-alert via a separate channel (e.g., direct email from the scheduler). The point is: a single-point-of-failure in alerting is unacceptable. Multi-channel redundancy at the routing layer is the only safe design."

### Q: "Why Airflow over EventBridge Scheduler for this branch project?"

> "Airflow is what the data engineering team in most companies is already running. Walking out of this course knowing Airflow is more job-market-portable than only knowing EventBridge. The branch project deliberately picks Airflow for that reason. For a greenfield AWS-only ML team, I'd actually pick EventBridge Scheduler + Lambda — no scheduler infrastructure to maintain, IAM-native auth, scales to zero. The M8 capstone does that. The tradeoff is: Airflow gives you DAG-level alerting, manual triggers, lineage views, and a UI that data scientists can use without AWS console access. EventBridge gives you near-zero ops burden. Different tools for different team contexts."

---

## 7. Tools NOT in M6 (and which module they arrive in)

Things people commonly expect in a production monitoring system that aren't here yet — and where they come in:

| Tool | Why not in M6 | Where it arrives |
|---|---|---|
| **Hopsworks Feature Store** | Reference data is just a parquet for now | **M7** — reference window served from feature store |
| **MLflow Model Registry stage tracking** | M6 just monitors; doesn't retrain | **M7** + **M8** — drift triggers a retraining run that registers a new model version |
| **AWS Lambda** (real use) | Lab 1 sketches a Slack-fanout Lambda but doesn't deploy it | **M8** — Lambda wraps the M6 monitoring script and acts as the SageMaker Pipeline trigger |
| **EventBridge Scheduler** | Branch deliberately uses Airflow for portability | **M8** capstone |
| **SageMaker Pipelines** | M6 alerts a human; doesn't trigger retraining | **M8** — drift → SageMaker Pipeline → retrain → register → redeploy, all automated |
| **AWS CloudWatch Alarms** | We use SNS but don't wire alarm-thresholds against CW metrics | **M8** stretch — alarm on model latency p99, ECS task count, etc. |
| **W&B / nannyML / Aporia** | One observability platform per module is enough | **M7** has W&B for experiment tracking; nannyML / Aporia are stretch goals after the course |
| **AWS X-Ray / OpenTelemetry** | Distributed tracing is a separate observability domain | Outside the course — recommended for microservices courses |
| **Data Quality DAGs in Dagster / Prefect** | Airflow is the industry default; one tool is enough | Outside the course |

The point: M6 is the *minimum viable monitoring story*. Real production teams add label-aware evaluation, retraining triggers, multi-environment monitoring, dashboards — all of those build on the foundation Module 6 lays.

---

## 8. 15 interview questions (with hints)

These map to the depth-progression interviewers use. **Hints, not answers** — work them out yourself before re-reading §3-§6.

1. **Conceptual.** What's the difference between data drift, concept drift, and label drift? Give one Truck Delay example of each. *(Hint: §3.3, Lab 2 Step 1.)*
2. **Conceptual.** Why does GE run before Evidently in the combined pipeline? *(Hint: cost + meaningfulness of distance metrics on corrupted data.)*
3. **Conceptual.** Name two failure modes Evidently catches that GE misses, and two failure modes GE catches that Evidently misses. *(Hint: §3.2 + §3.3.)*
4. **Architecture.** Walk me through your monitoring pipeline from production batch arrival to alert email. *(Hint: §4.)*
5. **Architecture.** Where would you put the reference distribution in a real production system, and why? *(Hint: §5 Fork 6, M7 spine progression.)*
6. **Trade-offs.** SNS vs PagerDuty for paging — when do you pick which? *(Hint: §5 Fork 4 and §6 SNS justification.)*
7. **Trade-offs.** Argue both sides: should drift detection trigger automatic retraining, or always page a human first? *(Hint: M6 pages humans; M8 automates. The answer depends on retraining cost + audit requirements.)*
8. **Statistics.** Why does Evidently use Wasserstein for numeric features and Jensen-Shannon for categorical? Why not KS for everything? *(Hint: KS is sensitive to support; Wasserstein handles different supports more gracefully; JS is symmetric and bounded for categoricals.)*
9. **Operations.** Your monitoring DAG runs every hour. After 30 days you have 720 runs. How do you avoid drowning in alerts when the same drift fires repeatedly? *(Hint: alert dedup — Lab 1 Step 6 + Branch Stretch Goal 1.)*
10. **Security.** Your boto3 SNS publisher needs AWS creds. What's the threat model and how do you handle it? *(Hint: IAM user with scoped policy `sns:Publish` on this topic only; or OIDC for CI/CD; or task role on ECS/Lambda.)*
11. **Cost.** Estimate the monthly AWS cost of running the M6 spine monitoring (excluding the M5 ECS service). *(Hint: SNS publishes free <1M/mo; email deliveries free <1k/mo; CloudWatch Logs read free <5GB/mo — total ≈ ₹0.)*
12. **Failure modes.** What happens if the reference data is corrupted (e.g., labelled wrong)? *(Hint: garbage in, garbage out — your drift detector becomes a drift accuser. Reference data needs its own version control + review.)*
13. **Scheduling.** Compare Airflow, cron, and EventBridge Scheduler on the dimensions of setup time, retries, scaling, and cost. *(Hint: Branch project Phase 5 comparison table.)*
14. **Schema evolution.** A new feature is added to the model in M7. How do you update the GE suite without losing history? *(Hint: version the suite as `truck_delay_features_v2`; keep `v1` as documentation for older batches; tag every validation run with its suite version.)*
15. **Forward-looking.** M8 will turn this monitoring pipeline into a SageMaker Pipeline trigger. Predict which AWS services M8 will add to M6's stack. *(Hint: Lambda, EventBridge, SageMaker Pipelines, possibly a second SNS topic for pipeline notifications.)*

---

## TL;DR — the one-page mental model

```
M5 ended with:    a live ECS service serving predictions, no observability

M6 ends with:     every hour, a script ingests the latest production batch,
                  runs GE + Evidently, and pages on-call via SNS if anything's wrong

The journey:
    Production batch (CloudWatch Logs / S3 / simulator)
        ↓
    Great Expectations    ← cheap, fail-fast schema check
        │  (success: bool + per-row failures)
        ↓
    Evidently            ← expensive, statistical drift check
        │  (dataset_drift: bool + drift_share: float)
        ↓
    Severity router      ← critical / warning / info
        ↓
    Amazon SNS topic     ← publish if either check fired
        │
        ├─► email          (immediate)
        ├─► Lambda → Slack (#ml-monitoring)
        └─► (future) PagerDuty, SMS, SQS

Orchestration layer:
    Lab 4: cron-runnable script
    Branch: same logic under Airflow DAG (@hourly, with retries + UI)
    M8 (preview): same logic under EventBridge Scheduler + Lambda, with
                  drift-triggered SageMaker Pipeline retraining
```

If you can draw that diagram from memory and name what failure mode each component prevents, you can defend M6 in any interview.
