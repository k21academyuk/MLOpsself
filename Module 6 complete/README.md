# MLOps Module 6 — Monitoring, Testing & Drift Detection

**Evidently AI drift + Great Expectations data validation + Amazon SNS alerting + Airflow branch project** — taking the M5 ECS deployment and giving it eyes, ears, and a paging system, so the team finds out *before* a customer does when the model starts misbehaving.

> **New to this module?** Start with **[M6_Student_Manual.md](M6_Student_Manual.md)** — the end-to-end walkthrough.
>
> **Want the *why* — what each tool solves, why drift detection is a real production need, and how to defend the architecture in an interview?** Read **[M6_Module_Reference_Guide.md](M6_Module_Reference_Guide.md)**.
>
> **Joining at M6?** You need the M5 ECS service still running (or its artifacts on disk). Skim §2 of the Student Manual for the on-ramp.

---

## What you'll build

Take the **live Truck Delay ECS service** from M5 and:

1. Assemble a **production batch** of recent inference inputs to monitor. In class you use a simulated / parquet batch, because the M5 Streamlit container is **self-contained and doesn't yet emit structured per-inference logs** — wiring the ECS service to log each inference so you can read it back from CloudWatch is an M7/M8 enhancement that Lab 4 Step 7 previews.
2. Use **Great Expectations** to validate every batch of inputs against a schema — catch malformed data *before* it reaches the model.
3. Use **Evidently AI** to compute drift against the M3 training distribution — catch slow shifts in the feature/label distribution that degrade accuracy over time.
4. Wire both checks into an **Amazon SNS topic** — when a drift report fails or a Great Expectations validation breaks, the on-call gets an email/SMS within 60 seconds.
5. (Branch take-home) Re-implement the whole pattern using **Apache Airflow + Docker Compose** on a banking-domain churn model — the deliberate "what does this look like under a real workflow scheduler?" moment.

---

## Repo map

```
.
├── README.md                                          ← you're here
├── M6_Student_Manual.md                               ← THE manual — read this first
├── M6_Module_Reference_Guide.md                       ← the conceptual KT (why each tool, how it fits)
│
├── instructor_setup/                                  ← CDK: auto-stopping SageMaker notebook (Tier 1, same VPC as M3-M8)
│   ├── app.py · cdk.json · requirements.txt
│   ├── mlops_m6/m6_stack.py                              notebook + lifecycle config + IAM role
│   └── scripts/{on-create.sh, on-start.sh}              bake deps + idle auto-stop (45 min)
│
└── labs/
    ├── data/                                          REAL M3 artifacts shipped for the labs
    │   ├── reference/final_features.csv               M3 Lab B reference frame (12,308×37, validated)
    │   └── artifacts/                                 M3 Lab C model + encoder + scaler + metadata
    ├── M6_Lab_1_SNS_Alerting_Setup.md                 SNS topic + subscription + boto3 publish (first encounter)
    ├── M6_Lab_2_Evidently_Drift_Detection.ipynb       Evidently report vs M3 baseline; HTML + JSON (exploration notebook)
    ├── M6_Lab_3_Great_Expectations_Validation.ipynb   Expectation suite for incoming inference data (exploration notebook)
    ├── M6_Lab_4_Combined_Monitoring_Pipeline.ipynb    Compose GE → Evidently → SNS interactively (exploration notebook)
    ├── M6_Lab_5_Production_Pipeline/                  The production .py package the notebook logic ships as
    └── M6_Branch_Airflow_Monitoring/                  Take-home: Airflow DAG + Docker Compose
        ├── README.md                                     Branch briefing
        ├── dags/                                         The monitoring DAG
        ├── data/                                         Reference + production CSV slices
        ├── docker-compose.yml                            Airflow + Postgres
        └── plugins/                                      Custom operators / utility code
```

---

## After M5 — run the 5 spine labs + branch

The spine labs build on each other. **Lab 1** gives you the alerting channel. **Labs 2 and 3** are exploration **notebooks** for the two complementary checks (drift vs validation). **Lab 4** is a notebook where you compose them interactively. **Lab 5** ships that exact logic as the production `.py` script. *Notebooks to explore → script to operate.*

> **Reference data is real, not synthetic.** Labs 2–5 load the genuine M3 outputs in `labs/data/` — `final_features.csv` (the M3 Lab B training distribution, regenerated from the committed raw CSVs and round-trip-validated against the model at 0.93 accuracy) and the M3 Lab C model artifacts. The *only* synthetic data is the drifted/corrupted "current" batch, because there's no live production traffic in class.

| Lab | What | Format | Output |
|---|---|---|---|
| **1** | [SNS topic + subscription + boto3 alerter](labs/M6_Lab_1_SNS_Alerting_Setup.md) | AWS Console + Python | `truck-delay-alerts` topic, your email subscribed and confirmed |
| **2** | [Evidently drift detection vs M3 baseline](labs/M6_Lab_2_Evidently_Drift_Detection.ipynb) | Jupyter notebook | `drift_report.html` + `drift_metrics.json` + a "drift detected" boolean |
| **3** | [Great Expectations validation suite](labs/M6_Lab_3_Great_Expectations_Validation.ipynb) | Jupyter notebook | `truck_delay_features.json` expectation suite + Data Docs HTML |
| **4** | [Combined monitoring (exploration)](labs/M6_Lab_4_Combined_Monitoring_Pipeline.ipynb) | Jupyter notebook | GE + Evidently + SNS composed interactively; all three paths exercised |
| **5** | [Production monitoring pipeline](labs/M6_Lab_5_Production_Pipeline/) | Python package | `run_monitoring.py` — exit-code CLI for cron / Airflow / EventBridge / SageMaker |
| **Branch** | [Airflow + Docker monitoring](labs/M6_Branch_Airflow_Monitoring/) | Local Docker Compose | Scheduled DAG that runs the same checks on a banking churn model |

Full per-lab walkthrough is in **[M6_Student_Manual.md](M6_Student_Manual.md)**.

---

## AWS services introduced in M6

The only **first-encounter** AWS service this module is **SNS**:

| Service | What it does | First introduced in lab |
|---|---|---|
| **SNS (Simple Notification Service)** | Pub-sub topic + email/SMS/Lambda/HTTPS subscribers — the alerting bus for drift events | Lab 1 |

The rest of the AWS surface (ECS service, ECR image, IAM, CloudWatch Logs) is reused from M3–M5 with no new provisioning required.

Per the **learn-then-automate** rule in [PLANNING.md](../PLANNING.md), SNS is hands-on here (Console + boto3) because this is the first encounter. In M8, the SageMaker Pipeline's second SNS topic will be CDK-provisioned silently.

---

## Tools (non-AWS) introduced in M6

| Tool | What it does | First introduced in lab |
|---|---|---|
| **Evidently AI** | Statistical drift + data quality reports for ML inputs, predictions, and (when available) ground-truth labels | Lab 2 |
| **Great Expectations** | Declarative "this column must be int, between 0 and 100, never null" rule engine for tabular data | Lab 3 |
| **Apache Airflow** | Workflow scheduler — DAGs of tasks running on cron schedules with retries, alerting, and a web UI | Branch project |

---

## Teardown

SNS topics have no hourly cost (you only pay per published message + per delivered email/SMS), so it's safe to keep the topic around. The teardown below is the minimal one-line cleanup if you want a fully empty account.

```bash
# 1. Delete the SNS subscription (Console: SNS → Subscriptions → select → Delete)
#    Or CLI:
aws sns list-subscriptions --query "Subscriptions[?TopicArn=='arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts'].SubscriptionArn" \
    --output text | xargs -I {} aws sns unsubscribe --subscription-arn {}

# 2. Delete the topic
aws sns delete-topic --topic-arn arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts

# 3. (Branch only) Stop the Airflow Compose stack
cd labs/M6_Branch_Airflow_Monitoring
docker compose down -v
```

Full teardown procedure in **[M6_Student_Manual.md §11](M6_Student_Manual.md)**.

> **The M5 ECS service:** keep it running through M6 if you want to monitor the *live* service. Otherwise re-use the M4 artifacts on disk and tear ECS down — see M5_Student_Manual §13. M7 and M8 do not require the M5 ECS service to be running.

---

## What you'll learn

- The three kinds of drift (**data drift**, **concept drift**, **label drift**) and which statistical tests catch which kind
- When to use **Evidently** (continuous, statistical) vs **Great Expectations** (point-in-time, declarative) — and why you usually need both
- Wire a Python alerter to **Amazon SNS** with `boto3` — and reason about who subscribes, what message format, and how to avoid alert fatigue
- Build a **runnable monitoring pipeline** that turns raw inference logs into a "page the on-call" decision
- Run the same logic under **Apache Airflow** scheduling (Branch project) so you understand the orchestration story when monitoring becomes "run this every hour, retry on failure, page if it fails twice in a row"

Full learning outcomes are in **[M6_Student_Manual.md §10](M6_Student_Manual.md)**.

---

## License + credits

Course content built for the **AWS MLOps Master Course** (48 hours, 8 modules). Module 6 is **spine phase 4** — Truck Delay Classification monitoring continues into M7 (feature store) and M8 (full SageMaker Pipeline automation, where drift detection becomes a pipeline trigger).

Branch project ("End-to-End ML Model Monitoring using Airflow and Docker") based on the source materials in `Projects Repo/End-to-End ML Model Monitoring using Airflow and Docker/`.
