# Module 7 — Guide
## Feature Stores, Experiment Management & Explainability

> **One document, two jobs.** This guide combines the **hands-on walkthrough** (what to run, in what order, what to submit)
> with the **conceptual KT** (what each tool solves, how the four fit together) and **15 interview questions with hints**.
> If you just want to start running, jump to [§5 Timing](#5-timing) and [§6 The four spine labs](#6-the-four-spine-labs).
> The instructor-facing teaching guide lives separately in [M7_Instructor_Manual.md](M7_Instructor_Manual.md).

**Spine project: Truck Delay Classification.** M6 gave the deployed model *eyes* (drift + validation + alerting). **M7 turns
the spine into a managed ML asset**: a versioned feature supply chain (Hopsworks), a governed model lifecycle (MLflow
Registry), disciplined experimentation (W&B), and explanations (SHAP). Everything runs on the **real M3 artifacts** that
ship in [labs/data/](labs/) — `final_features.csv` (12,308 × 37) plus the model / encoder / scaler. **Nothing synthetic.**

---

## Table of Contents
1. [Prerequisites & environment](#1-prerequisites--environment)
2. [How M7 picks up from M6](#2-how-m7-picks-up-from-m6)
3. [The concepts — the *why*](#3-the-concepts--the-why)
4. [Enterprise use cases](#4-enterprise-use-cases)
5. [Timing](#5-timing)
6. [The four spine labs](#6-the-four-spine-labs)
7. [Deliverables & submission](#7-deliverables--submission)
8. [Teardown](#8-teardown)
9. [Interview questions (15, with hints)](#9-interview-questions-15-with-hints)
10. [Learning outcomes](#10-learning-outcomes)

---

## 1. Prerequisites & environment

| What | Why | Where |
|---|---|---|
| **SageMaker notebook from M6** | run the four notebooks here (auto-stops overnight — just **Start** it) | `m6-truck-delay-monitoring` (same VPC, ap-south-1) |
| **Real M3 artifacts** | the feature frame + model every lab uses | **ship with M7**: `labs/data/` |
| **MLflow Tracking Server** | Lab 2's backend (Tier 1, instructor-provisioned) | `instructor_setup/` → `MLFLOW_TRACKING_URI` |
| **Hopsworks account** (free) | Lab 1's feature store | [app.hopsworks.ai](https://app.hopsworks.ai) — API key |
| **W&B account** (free) | Lab 3's tracking + sweeps | [wandb.ai](https://wandb.ai) — API key |

> **Joining at M7?** You're fine — the real data + model ship in `labs/data/`. You only bring free Hopsworks + W&B
> accounts. Nothing here fabricates a stand-in model or features.

New Python deps (the notebooks' setup cells install them): `hopsworks`, `mlflow`, `wandb`, `shap`. Everything is
**Python 3.12.10**. Portable fallback for any lab: Google Colab or local Jupyter — the data ships with the notebooks.

---

## 2. How M7 picks up from M6

```
M6 end:   a deployed, monitored Truck Delay model. But the "reference" is a CSV; the model is a .pkl on S3;
          tuning was ad-hoc; and you can't explain a single prediction.
M7:       Feature Store   →  the CSV becomes versioned Hopsworks feature groups (online + offline parity)
          Model Registry  →  the .pkl becomes a registered model with Staging→Production→Archived stages
          Experiments     →  W&B sweeps replace ad-hoc tuning
          Explainability  →  SHAP answers "why delayed?"
M7 hands off to M8:  SageMaker Pipelines read features from the store, train, evaluate, and
                     register/promote models in the registry — automatically.
```

---

## 3. The concepts — the *why*

### 3.1 The through-line: from "a model" to "a managed ML asset"

| Capability | Before M7 (M3–M6) | After M7 |
|---|---|---|
| Features | a `final_features.csv` someone shares | **Hopsworks feature groups** — versioned, online+offline, lineage |
| Model storage | `joblib.dump` → S3 | **MLflow Model Registry** — versions, stages, approval, rollback |
| Tuning | ad-hoc loops | **W&B sweeps** — Bayesian search, dashboards |
| Trust | "the model said so" | **SHAP** — per-feature, per-prediction explanations |

These aren't four unrelated tools — they're the four things that turn a working model into something an **organisation**
can operate, govern, and defend.

### 3.2 Feature Store — the concepts that matter

A **feature store** is a centralized system for storing, versioning, and serving the features ML models use. It solves
**training/serving skew** — the silent failure where training and serving compute a feature slightly differently and
accuracy quietly rots. The concepts:

- **Offline store** (columnar, e.g. Hudi/Parquet) → building training datasets at scale.
- **Online store** (low-latency KV, e.g. RonDB) → serving a single feature vector in milliseconds.
- **Online/offline parity** → the *same* feature value in training and serving — this is the structural fix for skew.
- **Feature group** → a table of related features with a **primary key** + **event_time**.
- **Feature view** → a named, versioned *selection* of features = the model's input contract.
- **Point-in-time join** → a training row only sees feature values that existed at its timestamp (no label leakage).
- **Statistics & descriptions** → built-in data-quality stats/histograms (the drift baseline) + governance metadata.

**Popular feature stores — open-source vs cloud-managed.** Not all are cloud-only:

| Feature store | Open-source? | How you run it |
|---|---|---|
| **Feast** | ✅ Fully open-source | Self-host (bring your own DBs) |
| **Hopsworks** | ✅ Open-source core + **free managed Serverless** | Self-host *or* app.hopsworks.ai — **what we use** |
| **AWS SageMaker Feature Store** | ❌ Cloud-managed (AWS) | Fully managed in AWS |
| **Vertex AI Feature Store** | ❌ Cloud-managed (GCP) | Fully managed in GCP |
| **Databricks Feature Store** | ❌ Managed (Databricks) | Inside a Databricks workspace |
| **Tecton** | ❌ Commercial managed | Managed SaaS |

We use **Hopsworks** because it's the sweet spot for learning — **open-source** (concepts transfer anywhere) *and* a
**free managed Serverless tier**, so we get a real online+offline store with lineage + statistics without paying for or
running any infrastructure.

**Why it matters here:** M3's reference frame was a CSV — fine for one person, fragile across a team and across the
training→serving boundary. The feature store is the durable fix.

### 3.3 Model Registry — beyond `joblib.dump`

A registry entry has: **version history**, a **stage** (`None`/`Staging`/`Production`/`Archived`), the **run** that
produced it (params, metrics, lineage), and an **audit trail** of promotions. Promotion to Production is an **approval
gate** — a human or, in M8, a pipeline **condition step**. Rollback = re-promote the prior version (one call); the bad
version is archived, not deleted.

> **MLflow stages vs aliases:** MLflow 2.x is migrating from named *stages* to *aliases* (e.g. `@champion`). The course
> teaches **stages** (still supported in 2.14, and what most 2024–2026 production code uses). Mention aliases as the forward path.

### 3.4 MLflow vs W&B

| | MLflow | W&B |
|---|---|---|
| Model | open-source, self-host | hosted SaaS |
| Strength | **Registry** + stages + open standard | **Sweeps** + dashboards + collaboration |
| Cost | infra you run | free tier / per-seat |
| Best for | governed prod handoff | fast exploratory tuning |

Not either/or — **sweep in W&B, register the winner in MLflow, promote in M8.**

### 3.5 SHAP — explainability that holds up

- **Shapley values** (game theory): fairly attribute the prediction among features. Additive: contributions + base value = prediction.
- **Global** (summary/bar): the model's overall drivers. **Local** (force/waterfall): one prediction. **Dependence**: a feature's effect across its range + interactions.
- **`TreeExplainer`** is exact and fast for tree models (our XGBoost).
- **The M7→M6 link:** weight drift severity by SHAP importance — drift in a top-SHAP feature is the alert that should page someone.

---

## 4. Enterprise use cases
- **Banking (the FreshBasket finance team):** regulators require per-decision explanations → SHAP is mandatory; feature store gives auditable lineage; registry gives model governance.
- **Cross-team feature reuse:** the fraud team and the churn team share the same `customer_*` features from one store — defined once, served consistently.
- **Champion/challenger:** registry stages + W&B sweeps let you safely trial a challenger against the production champion.

---

## 5. Timing

### 7-hour day
| Block | Time |
|---|---|
| Tier-2 demo: MLflow server (since M3) + Hopsworks project | 0:00–0:20 |
| Lab 1 — Hopsworks Feature Store | 0:20–1:50 |
| Lab 2 — MLflow Model Registry | 1:55–2:55 |
| Lunch | 2:55–3:35 |
| Lab 3 — W&B sweeps | 3:35–4:35 |
| Lab 4 — SHAP | 4:45–5:45 |
| Synthesis: feature store ↔ registry ↔ SHAP ↔ M6 drift | 5:45–6:30 |
| Buffer / Q&A | 6:30–7:00 |

### 3-hour fast-track
Lab 1 (50) + Lab 2 (40) + Lab 4 SHAP (40) hands-on; **Lab 3 W&B → take-home** (it's a hosted sweep, self-paced). Tie SHAP
importance back to M6 drift severity as the closing point. Lab 1's *concept* sections (0a–0e) can be pre-read.

---

## 6. The four spine labs

### Lab 1 — Hopsworks Feature Store
**File:** [labs/M7_Lab_1_Hopsworks_Feature_Store.ipynb](labs/M7_Lab_1_Hopsworks_Feature_Store.ipynb)

Starts with the **concepts** (what a feature store is, training/serving skew, offline vs online, the popular tools and
which are open-source). Then, hands-on: register the real M3 feature frame as a **versioned feature group**
(`truck_delay_features`, `trip_id` primary key + `event_time`), **document** each feature, turn on **statistics /
data-quality monitoring**, **read it back** from the store, then retrieve it two ways — **offline** (a training dataset via
a Feature View) and **online** (a single low-latency vector by key). Finally create **v2** to see how features evolve
without breaking v1.

**The point:** one feature definition, reused for training and serving, with **no training/serving skew** — the failure
mode a shared CSV invites. **Output:** `truck_delay_features` v1 + v2 (described + statistics on), a `truck_delay_fv`
feature view.

### Lab 2 — MLflow Model Registry
**File:** [labs/M7_Lab_2_MLflow_Model_Registry.ipynb](labs/M7_Lab_2_MLflow_Model_Registry.ipynb)

Log the **real** model (with its true `model_metadata.json` metrics) to the tracking server and **register** it as
`truck-delay-classifier`. Move it **Staging → Production → Archived**, then load `models:/truck-delay-classifier/Production`
and score real rows. **The point:** a registry gives versioning, stage gates, approval, audit, and one-call rollback —
everything a `joblib.dump` to S3 lacks. **Output:** a registered, Production-staged model.

> Set `MLFLOW_TRACKING_URI` from the `instructor_setup` output before you start.

### Lab 3 — W&B Experiments + Sweeps
**File:** [labs/M7_Lab_3_WandB_Experiments_Sweeps.ipynb](labs/M7_Lab_3_WandB_Experiments_Sweeps.ipynb)

Starts with the **concepts** — what Weights & Biases *is*, and why it's a different kind of tool from the **PyCaret /
Optuna** tuning you did in M2 (optimizers *find* hyperparameters; W&B *tracks, visualizes, orchestrates, and shares* the
runs — they compose, they don't compete). Then, hands-on: **track a single baseline run**, run a **Bayesian hyperparameter
sweep** over the real XGBoost model, **read the results three ways** (best-run API, a pandas leaderboard + parameter
correlation, and the W&B dashboard panels), and **version the winner as a W&B Artifact**. Close with the
**Optuna-vs-MLflow-vs-W&B** comparison. **The point:** W&B's sweep engine + dashboards + artifacts vs MLflow's self-hosted
registry — and why teams run them together. **Output:** a tracked baseline, an 8-trial sweep + best config, a versioned
model artifact.

### Lab 4 — SHAP Explainability
**File:** [labs/M7_Lab_4_SHAP_Explainability.ipynb](labs/M7_Lab_4_SHAP_Explainability.ipynb)

Use `TreeExplainer` on the real model to produce **global** (summary/bar), **local** (waterfall for one flagged shipment),
and **dependence** (`route_avg_precip`) explanations. **The point:** answer "why *delayed*?" — required in regulated
contexts — and connect SHAP importance back to **M6 drift severity** (drift in a top-SHAP feature is the urgent kind).
**Output:** the explanation plots + the SHAP↔drift insight.

---

## 7. Deliverables & submission
1. Hopsworks feature group `truck_delay_features` (v1 + v2) + feature view — screenshot of the Hopsworks UI (Schema +
   Statistics tabs visible).
2. MLflow `truck-delay-classifier` registered, **Production** stage — screenshot of the registry.
3. W&B sweep dashboard URL + best config.
4. SHAP summary + one local waterfall, with a 3-sentence interpretation tying the top driver to M6 drift.

Bundle into your course GitHub repo under `module7/` (the final packaging guide in M8 assembles all modules).

---

## 8. Teardown
Per the **2-day SOP**, don't tear down between M7 and M8 — the MLflow EC2 is cheap (~₹1.5/h; stop it overnight if you
like), and the SageMaker notebook auto-stops. **Destroy everything after M8.** Hopsworks + W&B are free tiers (nothing to
tear down). The registered model + feature groups persist for M8 to consume.

---

## 9. Interview Questions (15, with hints)

1. **What is training/serving skew and how does a feature store prevent it?** *(Hint: same feature definition + values, online vs offline.)*
2. **Offline vs online store — when is each read?** *(Hint: batch training vs real-time inference; latency profiles.)*
3. **What is a point-in-time join and what bug does it prevent?** *(Hint: label leakage / future data in a training row.)*
4. **Feature group vs feature view — what's the difference?** *(Hint: storage table vs the model's input contract/selection.)*
5. **Why version features?** *(Hint: evolve definitions without a flag-day; v1 serves old model, v2 the new.)*
6. **Registry vs `joblib.dump` to S3 — name four things the registry adds.** *(Hint: versions, stages, approval, audit/rollback.)*
7. **Walk the Staging→Production→Archived lifecycle.** *(Hint: candidate → approved/live → retired; rollback re-promotes.)*
8. **How do you roll back a bad production model in MLflow?** *(Hint: archive current, transition previous back; nothing deleted.)*
9. **MLflow vs W&B — when would you pick each, and why run both?** *(Hint: registry/self-host vs sweeps/SaaS; sweep then register.)*
10. **What is a hyperparameter sweep, and what does Bayesian search buy over grid?** *(Hint: controller learns promising regions; fewer trials.)*
11. **What are Shapley values, intuitively?** *(Hint: fair credit allocation; additive to the prediction.)*
12. **Global vs local explanation — give a Truck Delay example of each.** *(Hint: "weather drives delays overall" vs "this shipment flagged because precip + distance".)*
13. **Why is `TreeExplainer` preferred for XGBoost over `KernelExplainer`?** *(Hint: exact + fast for trees vs model-agnostic but slow.)*
14. **How would you connect SHAP to your M6 drift monitoring?** *(Hint: weight drift severity by feature importance.)*
15. **A regulator asks why a customer was denied — which M7 tools answer, and how?** *(Hint: SHAP local explanation + feature-store lineage + registry version/audit.)*

---

## 10. Learning outcomes
By the end of M7 you can: stand up and version features in a feature store (with descriptions + statistics); govern a model
through a registry lifecycle; run and read a hyperparameter sweep; and explain any single prediction with SHAP — and
articulate how all four make the M8 automated pipeline trustworthy.
