# Module 8 — Full Automation with SageMaker Pipelines (Capstone)

**SageMaker Pipelines + Lambda + EventBridge + SNS** — the capstone. Everything from M3–M7 (features, model, registry,
monitoring, explainability) becomes **one triggered, self-healing pipeline**: data arrives → the pipeline runs → it
processes, trains, evaluates, and *conditionally* registers + deploys a new model — and pages you via SNS.

> **New here?** Read **[M8_Guide.md](M8_Guide.md)** — the single guide combining the walkthrough, the *why* / interview prep
> (15 Qs), and a consolidated **AWS CLI quick reference**. **Assemble your portfolio:**
> **[M8_Course_Completion_Packaging_Guide.md](M8_Course_Completion_Packaging_Guide.md)**.

---

## The capstone arc — the whole course in one picture

```
M3 features+model · M4 Docker · M5 ECS+CI/CD · M6 monitoring · M7 feature store+registry+SHAP
                                          │
                                          ▼
M8:   EventBridge (schedule)  ──▶  Lambda (new data lands)  ──▶  SageMaker Pipeline:
        Processing ─▶ Training ─▶ Evaluation ─▶ Condition(f1 ≥ threshold?)
                                                   ├─ yes ─▶ RegisterModel ─▶ (Deploy / approve)
                                                   └─ no  ─▶ SNS "model below bar — needs attention"
        Every run: SNS notification.  Registry: a new versioned model.  No human in the loop.
```

This is the **learn-then-automate** payoff: every service you met hands-on (ECS, SNS, the monitoring script) now runs
*itself*. **First encounters in M8:** SageMaker Pipelines, Lambda, EventBridge. **Reused (CDK pre-provisions):** SNS (first
taught M6), S3 (since M3).

Trains on the **real** Truck Delay data (`labs/data/final_features.csv`) — the same 12,308 × 37 frame from M3 Lab B.

---

## Module structure

```
Module 8/
├── README.md · M8_Guide.md (walkthrough + concepts + 15 Qs + CLI ref) · M8_Instructor_Manual.md
├── M8_Course_Completion_Packaging_Guide.md     ← assemble all 5 projects into a portfolio
│
├── instructor_setup/                           ← CDK: SNS topic + S3 artifact bucket + IAM (same VPC, ap-south-1)
│   ├── app.py · cdk.json · requirements.txt
│   └── mlops_m8/m8_stack.py
│
└── labs/
    ├── data/                                   REAL M3 reference frame + model (shipped)
    ├── M8_Lab_1_Pipeline_Steps/                the pipeline-as-code package (ships ready to read + run)
    │   ├── pipeline.py                            orchestrates the 6 steps
    │   ├── code/processing.py                     Processing step: load → encode/scale → split
    │   ├── code/training.py                       Training step: XGBoost
    │   ├── code/evaluation.py                     Evaluation step: f1 → evaluation.json
    │   └── requirements.txt
    ├── M8_Lab_1_Build_And_Run_Pipeline.ipynb   build the Pipeline object, upsert, start, watch
    ├── M8_Lab_2_Lambda_Streaming.md            Lambda that lands new streaming data (first encounter)
    └── M8_Lab_3_EventBridge_Schedule.md        EventBridge rule that triggers the pipeline (first encounter)
```

---

## Labs

| Lab | What | Tier | First encounter |
|---|---|---|---|
| **1 (steps)** | [The 6 pipeline steps](labs/M8_Lab_1_Pipeline_Steps/) — `processing.py`, `training.py`, `evaluation.py`, composed in `pipeline.py` (Processing → Training → Evaluation → Condition → Register/Fail). Ships ready to read | 3 | **SageMaker Pipelines** |
| **1 (run)** | [Build + run the pipeline](labs/M8_Lab_1_Build_And_Run_Pipeline.ipynb) — create the package group, `upsert()`, `start()`, watch in Studio, approve the model, **prove the gate** (boto3 *and* CLI) | 3 | |
| **2** | [Lambda streaming](labs/M8_Lab_2_Lambda_Streaming.md) — deploy a **pandas-free** Lambda (no layer) that drops a batch into S3; full copy-paste CLI | 3 | **Lambda** |
| **3** | [EventBridge schedule](labs/M8_Lab_3_EventBridge_Schedule.md) — a cron schedule that starts the pipeline automatically; full CLI + prove-it-now | 3 | **EventBridge** |

---

## Environment — same SageMaker instance, same VPC, ap-south-1
Author and run from the **same `m6-truck-delay-monitoring` SageMaker instance** (now on its 3rd day; auto-stops overnight).
The pipeline's compute (`ml.m5.large` Processing/Training jobs) is **ephemeral** — SageMaker spins it up per step and tears
it down, so there's nothing to leave running. New deps: `sagemaker` SDK. Region/VPC unchanged from M3–M7.

## Cost
| Item | Cost |
|---|---|
| Notebook (reused, auto-stop) | ~₹4/h running, ~₹0 stopped |
| Pipeline Processing/Training jobs (`ml.m5.large`, per-run, ~10 min) | ~₹10–15 per full pipeline run |
| Lambda + EventBridge + SNS | effectively ₹0 (free tier) |
| S3 artifact bucket | negligible |

> **Final teardown is here.** After M8, run `cdk destroy` in **every** `instructor_setup/` (M3-style infra, M4 ECR, M5,
> M6 notebook, M7 MLflow, M8 SNS/S3) — the [Course Completion guide](M8_Course_Completion_Packaging_Guide.md) has the full
> checklist so nothing is left billing.

---

## What you'll have built by the end
A production MLOps system for Truck Delay: **deployed** (M5), **monitored** (M6), **governed** (M7), and **fully automated**
(M8) — plus four branch projects. The packaging guide turns it into a portfolio you can show in interviews.
