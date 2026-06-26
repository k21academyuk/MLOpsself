# Module 7 — Feature Stores, Experiment Management & Explainability

**Hopsworks Feature Store + MLflow Model Registry + Weights & Biases + SHAP** — taking the monitored M6 system and giving it
a **durable feature supply chain**, a **governed model lifecycle**, and **explanations** you can show a regulator. This is
where the spine stops being "a model + a script" and becomes a **managed ML asset**.

> **New to this module?** Read **[M7_Guide.md](M7_Guide.md)** — the single guide that combines the end-to-end
> walkthrough **and** the *why* (feature store vs ad-hoc parquet, registry vs `joblib.dump`, SHAP vs "trust me"),
> plus 15 interview questions with hints.

---

## Where M7 sits in the spine

```
M3  build:     features + model on RDS/S3
M4  package:   Docker + ECR
M5  deploy:    ECS + ALB + CI/CD
M6  monitor:   Evidently drift + GE validation + SNS  (you are watching the model)
M7  manage:    ─────────────────────────────────────────────────────────────────
       Feature Store:   the reference frame stops being a CSV someone emails around →
                        it becomes versioned Hopsworks feature groups (online + offline)
       Model Registry:  joblib.dump → MLflow Registry with Staging→Production→Archived stages
       Experiments:     W&B sweeps to compare runs; how it differs from MLflow
       Explainability:  SHAP — *why* did the model predict "delayed"?
M8  automate:  SageMaker Pipelines tie all of the above into one triggered, self-healing flow
```

Everything in M7 operates on the **real M3 artifacts** that ship in [labs/data/](labs/) — the same
`final_features.csv` (12,308 × 37) and XGBoost model + encoder + scaler used in M6. **Nothing synthetic.**

---

## Module structure

```
Module 7/
├── README.md                              ← you're here
├── M7_Guide.md                            ← THE guide — walkthrough + concepts + 15 interview Qs (read first)
├── M7_Instructor_Manual.md                ← teaching guide, timings, Tier-2 demo SOP, pre-class checklist
│
├── instructor_setup/                      ← CDK: MLflow Tracking Server (Tier 1, same VPC) + Hopsworks note
│   ├── app.py · cdk.json · requirements.txt
│   ├── mlops_m7/m7_stack.py                  MLflow-on-EC2 server in the shared VPC
│   └── scripts/mlflow_userdata.sh           bootstraps the MLflow server
│
└── labs/
    ├── data/                              REAL M3 artifacts (final_features.csv + model) — shipped
    ├── M7_Lab_1_Hopsworks_Feature_Store.ipynb     create + version feature groups; online/offline retrieval
    ├── M7_Lab_2_MLflow_Model_Registry.ipynb       register the model; Staging→Production→Archived
    ├── M7_Lab_3_WandB_Experiments_Sweeps.ipynb    W&B tracking + a hyperparameter sweep; vs MLflow
    └── M7_Lab_4_SHAP_Explainability.ipynb         summary / force / dependence plots for the Truck Delay model
```

---

## The 4 spine labs

| Lab | What | Tier | Output |
|---|---|---|---|
| **1** | [Hopsworks Feature Store](labs/M7_Lab_1_Hopsworks_Feature_Store.ipynb) | 3 (Hopsworks first encounter) | versioned `truck_delay_features` feature group; offline (training) + online (serving) retrieval |
| **2** | [MLflow Model Registry](labs/M7_Lab_2_MLflow_Model_Registry.ipynb) | 3 | the real model registered as `truck-delay-classifier` with stage transitions + an inference-from-registry demo |
| **3** | [W&B experiments + Sweeps](labs/M7_Lab_3_WandB_Experiments_Sweeps.ipynb) | 3 | a W&B sweep over XGBoost hyperparameters; a written MLflow-vs-W&B comparison |
| **4** | [SHAP explainability](labs/M7_Lab_4_SHAP_Explainability.ipynb) | 3 | global (summary/bar) + local (force/waterfall) + dependence plots explaining the real predictions |

**Tier-2 demos (instructor, ~10 min each):** the **MLflow Tracking Server** (running since M3, redeployed by
[instructor_setup/](instructor_setup/)) and the **Hopsworks project** (pre-created — Hopsworks Serverless free tier or API).
Students don't provision these; they use them.

---

## Environment — same SageMaker instance as M6 (2-day continuity)

Run these notebooks on the **same `m6-truck-delay-monitoring` SageMaker instance** you used in M6 (it auto-stops overnight;
just **Start** it). New pip deps this module: `hopsworks`, `mlflow`, `wandb`, `shap`. The notebooks' setup cell installs
them. Portable fallback: Colab/local with Python 3.12.10 (the data ships in `labs/data/`). W&B and Hopsworks both have free
tiers — students sign up themselves (no AWS cost).

> **Region/VPC:** everything stays in **ap-south-1**, in the **same VPC** as M3–M8. The MLflow server is the one carried
> from M3 (Tier 1, CDK). See [instructor_setup/README.md](instructor_setup/).

---

## Cost

| Item | Cost |
|---|---|
| SageMaker notebook (reused from M6, auto-stop) | ~₹4/h running, ~₹0 stopped |
| MLflow Tracking Server (EC2 `t3.small`, Tier 1) | ~₹1.5/h — destroy after M8 |
| Hopsworks Serverless free tier | ₹0 |
| Weights & Biases free tier | ₹0 |
| SHAP | ₹0 (local compute) |

---

## What you'll be able to defend in an interview
- **Feature store vs a shared CSV/parquet** — point-in-time correctness, online/offline parity, reuse across teams, lineage.
- **Model Registry vs `joblib.dump` to S3** — versioning, stage gates, approval, rollback, audit.
- **MLflow vs W&B** — where each wins; why teams run both.
- **SHAP** — global drivers vs a single prediction's explanation; why regulated domains (banking, the FreshBasket context) require it.

Full learning outcomes + 15 interview questions: **[M7_Guide.md](M7_Guide.md)**.
