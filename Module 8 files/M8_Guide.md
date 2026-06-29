# Module 8 — Guide (Capstone)
## Full Automation with SageMaker Pipelines

> **One document, two jobs.** This guide combines the **hands-on walkthrough** (what to run, in what order, what to submit)
> with the **conceptual KT** (what each piece solves, how they fit) and **15 interview questions with hints**. To start
> running, jump to [§5 Timing](#5-timing) and [§6 The three capstone labs](#6-the-three-capstone-labs). The instructor-facing
> teaching guide is separate: [M8_Instructor_Manual.md](M8_Instructor_Manual.md). Portfolio assembly:
> [M8_Course_Completion_Packaging_Guide.md](M8_Course_Completion_Packaging_Guide.md).

**Spine project: Truck Delay Classification — the finale.** M3–M7 built, deployed, monitored, and governed the model **by
hand**. M8 makes it **run itself**: a SageMaker Pipeline (Process → Train → Evaluate → Condition → Register), a Lambda that
lands new data, and an EventBridge schedule that triggers the whole loop. Trains on the **real** `final_features.csv`
(12,308 × 37) shipped in [labs/data/](labs/). **Nothing synthetic.**

> ⚡ **CLI-first.** Every lab gives you **copy-paste AWS CLI / boto3** as the primary path (Console as an alternative), so the
> capstone is completable quickly from the SageMaker notebook terminal. A consolidated command list is in
> [§11 AWS CLI quick reference](#11-aws-cli-quick-reference).

---

## Table of Contents
1. [Prerequisites & environment](#1-prerequisites--environment)
2. [How M8 ties the whole course together](#2-how-m8-ties-the-whole-course-together)
3. [The concepts — the *why*](#3-the-concepts--the-why)
4. [Enterprise use cases](#4-enterprise-use-cases)
5. [Timing](#5-timing)
6. [The three capstone labs](#6-the-three-capstone-labs)
7. [Capstone deliverable](#7-capstone-deliverable)
8. [Teardown — the end of the course](#8-teardown--the-end-of-the-course)
9. [Interview questions (15, with hints)](#9-interview-questions-15-with-hints)
10. [Learning outcomes](#10-learning-outcomes)
11. [AWS CLI quick reference](#11-aws-cli-quick-reference)

---

## 1. Prerequisites & environment

| What | Why | Where |
|---|---|---|
| **SageMaker notebook from M6/M7** | author + run the pipeline (needs `get_execution_role()`) | `m6-truck-delay-monitoring` (same VPC, ap-south-1) |
| **Real M3 data** | the pipeline trains on it | **ships with M8**: `labs/data/` |
| **M8 `instructor_setup`** | SNS topic + S3 artifact bucket | `cdk deploy` → `PIPELINE_BUCKET`, `PIPELINE_TOPIC_ARN` |
| The pipeline step code | you inspect/run it | `labs/M8_Lab_1_Pipeline_Steps/` (scripts shipped) |

> **First encounters in M8:** SageMaker Pipelines, Lambda, EventBridge (hands-on — this is the learning). **Reused:** SNS
> (from M6, now CDK-provisioned), S3 (since M3). The pipeline uses the **notebook's execution role** — no extra IAM to make.

New dep: the `sagemaker` SDK (the notebook installs it). The pipeline's Processing/Training jobs are **ephemeral**
`ml.m5.large` containers SageMaker launches per step — nothing to leave running.

**Get the env vars the labs need (CLI):**
```bash
# Pull the M8 stack outputs (bucket + SNS topic) straight from CloudFormation
aws cloudformation describe-stacks --stack-name m8-stack \
  --query "Stacks[0].Outputs" --output table --region ap-south-1

export PIPELINE_BUCKET=$(aws cloudformation describe-stacks --stack-name m8-stack \
  --query "Stacks[0].Outputs[?OutputKey=='ArtifactBucket'].OutputValue" --output text --region ap-south-1)
export PIPELINE_TOPIC_ARN=$(aws cloudformation describe-stacks --stack-name m8-stack \
  --query "Stacks[0].Outputs[?OutputKey=='TopicArn'].OutputValue" --output text --region ap-south-1)
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "bucket=$PIPELINE_BUCKET  account=$ACCOUNT"
```

---

## 2. How M8 ties the whole course together

```
M3  features + model   ─┐
M4  Docker + ECR        │
M5  ECS + ALB + CI/CD   │   M8 automates all of it:
M6  monitoring + SNS    │      EventBridge (schedule) → Lambda (land data) → SageMaker Pipeline:
M7  feature store +     │         Process → Train → Evaluate → Condition(f1 ≥ 0.55)
    registry + SHAP     │            ├─ yes → Register model version (M7 registry)
                        ─┘            └─ no  → Fail → SNS alert (M6 skill, M8 topic)
```

Each first-encounter service you learned by hand now runs *itself*. That progression — **learn the service hands-on, then
let CDK/automation run it** — is the spine of the whole course.

---

## 3. The concepts — the *why*

### 3.1 Why a pipeline orchestrator at all?
By M7 you can do every step by hand: process data, train, evaluate, register, deploy, monitor. **SageMaker Pipelines** ties
those steps into one **versioned, reproducible, parameterised DAG** that runs on demand or on a trigger — with **lineage**
(every artifact traces to the run that made it), **caching** (skip unchanged steps), and a **condition gate** (only register
a model that clears the bar). The alternative — a hand-run notebook or a pile of cron scripts — has no lineage, no gate, no
reproducibility, and no audit. **Automation isn't about saving keystrokes; it's about trust.**

> **Where SageMaker Pipelines sits vs other orchestrators:** Airflow/MWAA and AWS Step Functions are general-purpose
> orchestrators; **SageMaker Pipelines** is ML-native (steps *are* SageMaker jobs, with the model registry + lineage built
> in). For an all-AWS ML workflow it's the lowest-friction choice — which is why the capstone uses it.

### 3.2 The canonical steps
| Step | SDK construct | Job |
|---|---|---|
| **Processing** | `ProcessingStep` + `SKLearnProcessor` | encode/scale/split — `code/processing.py` |
| **Training** | `TrainingStep` + `XGBoost` estimator | fit the model — `code/training.py` |
| **Evaluation** | `ProcessingStep` (XGBoost image) + `PropertyFile` | compute f1 → `evaluation.json` |
| **Condition** | `ConditionStep` + `ConditionGreaterThanOrEqualTo` + `JsonGet` | gate: register only if f1 ≥ threshold |
| **Register** | `ModelStep` + `model.register()` | new version in the Model Package Group (the M7 registry idea) |
| **Fail** | `FailStep` | else-branch when the model is below bar (a notification hangs off it) |

### 3.3 How steps pass data — the key mental model
**Properties + PropertyFiles** are how steps hand data to each other:
`step.properties.ModelArtifacts.S3ModelArtifacts`, `JsonGet(... json_path="f1")`. These are **references resolved at run
time**, not values known at build time — the single thing that confuses everyone about pipeline-as-code. You're writing a
*recipe* (the DAG), not executing it; `PipelineSession` (vs the normal `Session`) is what **defers** each job into the DAG
instead of running it immediately when you call `.run()` / `.fit()`.

### 3.4 The condition gate + the registry
The **Condition step** reads `f1` from the evaluation `PropertyFile` and registers the model **only if** it clears the
threshold — otherwise the **Fail** branch fires (and a notification can hang off it). Registration lands a new **version**
in a **Model Package Group** (`TruckDelayModelPackageGroup`) with an **approval status** (`PendingManualApproval` →
`Approved`). That's the M7 registry concept, now populated *automatically* by the pipeline.

### 3.5 Triggers — Lambda + EventBridge
- **Lambda**: serverless, event-driven compute. Here it lands a new data batch; in production it reads a stream/API/CDC.
- **EventBridge (Scheduler)**: cron/event bus. Targets the pipeline (`StartPipelineExecution`) and/or the Lambda. This is
  what removes the human from the loop.

### 3.6 The learn-then-automate rule, completed
| Service | First met (hands-on) | Automated by |
|---|---|---|
| SNS | M6 (drift alerts) | M8 CDK (pipeline-notification topic) |
| ECS / ALB / CodePipeline | M5 | (deployment layer) |
| SageMaker Pipelines / Lambda / EventBridge | **M8 (here)** | the capstone IS the automation |

### 3.7 How the capstone closes every earlier loop
- **M3 features** → the pipeline's Processing step (and M7's feature store as the durable source).
- **M3 model** → the Training step; the registered version is the M7 registry entry.
- **M6 drift/validation** → the condition gate is the automated version of "is the model still good?"; M6's
  `run_monitoring.py` can become a Processing step, and a drift alert can be the *trigger*.
- **M7 registry + SHAP** → registration is automated; SHAP importance can weight the retrain trigger.

---

## 4. Enterprise use cases
- **Weekly retrain on fresh data** with an automatic quality gate (this lab).
- **Drift-triggered retrain:** an EventBridge rule on a drift-alert SNS message starts the pipeline (M6 → M8).
- **Champion/challenger:** the pipeline registers a challenger (`PendingManualApproval`); a human or a second pipeline
  approves + deploys only if it beats the champion.
- **Regulated audit:** every model version traces to a pipeline run, its data, its metrics, and its approver.

---

## 5. Timing

### 7-hour capstone
| Time | Block |
|---|---|
| 0:00–0:20 | Tier-2 demo: the M8 SNS topic + S3 bucket (CDK); recap the automation target |
| 0:20–1:40 | **Lab 1 — read the step scripts** + build the `Pipeline`; create the model package group |
| 1:40–3:00 | **Lab 1 — upsert, start, watch** the run; inspect the registered model + the gate |
| 3:00–3:45 | Lunch (the pipeline run finishes while you eat) |
| 3:45–4:30 | **Lab 2 — Lambda** lands streaming data (pandas-free; no layer) |
| 4:30–5:10 | **Lab 3 — EventBridge** schedules the loop; trigger it now to prove it |
| 5:10–6:00 | Full-loop demo + the SHAP×drift retraining trigger discussion (M7→M6→M8) |
| 6:00–6:40 | **Course wrap-up** + packaging guide kickoff |
| 6:40–7:00 | Final teardown SOP |

---

## 6. The three capstone labs

### Lab 1 — Build the pipeline + run it
**Steps code:** [labs/M8_Lab_1_Pipeline_Steps/](labs/M8_Lab_1_Pipeline_Steps/) · **Run:** [labs/M8_Lab_1_Build_And_Run_Pipeline.ipynb](labs/M8_Lab_1_Build_And_Run_Pipeline.ipynb)

The three step scripts ship ready to read:
- `code/processing.py` — load `final_features.csv` → one-hot + scale + 70/15/15 split → S3.
- `code/training.py` — XGBoost (script mode) → `xgboost-model`.
- `code/evaluation.py` — f1 on the test split → `evaluation.json`.
- `pipeline.py` — wires Process → Train → Evaluate → **Condition(f1 ≥ 0.55)** → Register (else Fail).

The notebook opens with the **concepts** (DAG, run-time properties, the gate, `PipelineSession`), then: create the
**Model Package Group**, upload data, `get_pipeline()` → `upsert()` → `start()` → watch, inspect + **approve** the registered
version, read the `evaluation.json` the gate used, and **re-run with a high threshold to watch the gate Fail** — proving the
quality gate works. Every action is shown as **boto3 *and* AWS CLI**. **Output:** a completed run + a model version in
`TruckDelayModelPackageGroup`.

### Lab 2 — Lambda: land streaming data
**File:** [labs/M8_Lab_2_Lambda_Streaming.md](labs/M8_Lab_2_Lambda_Streaming.md)

Write + deploy a **Lambda** (your first) that samples the real data and drops a timestamped batch into S3 — simulating a
streaming arrival. The primary version is **pandas-free** (Python stdlib + boto3), so **no Lambda layer is needed** and the
whole lab is copy-paste CLI. **The point:** serverless event-driven glue; in production it'd read Kinesis/an API/CDC.
**Output:** a `batch_<ts>.csv` in `s3://.../streaming/`.

### Lab 3 — EventBridge: schedule the loop
**File:** [labs/M8_Lab_3_EventBridge_Schedule.md](labs/M8_Lab_3_EventBridge_Schedule.md)

Create an **EventBridge** schedule (your first) that triggers the pipeline (Pattern B) and optionally the Lambda first
(Pattern A). Prove it immediately with a one-time `at()` schedule (or a direct start) — no waiting a week. **The point:** the
system retrains and re-registers itself with **no human in the loop** — the capstone payoff. Full CLI provided for the
scheduler role + schedule. **Output:** a schedule that starts `TruckDelayClassification` on cron.

---

## 7. Capstone deliverable
A short write-up + screenshots proving the automated loop:
1. A successful pipeline execution graph (Studio) with all steps green.
2. The registered model version in `TruckDelayModelPackageGroup`.
3. The EventBridge schedule + a triggered execution (Run now / one-time `at()`).
4. A paragraph: *trace one shipment from raw data → feature → prediction → drift check → retrain trigger → new model*,
   naming the module each step came from.

Then assemble the full portfolio with the **[Course Completion Packaging Guide](M8_Course_Completion_Packaging_Guide.md)**.

## 8. Teardown — the end of the course
This is the last module — **do the full teardown.** Follow the packaging guide's checklist: delete the EventBridge
schedules, the Lambda + its role, `cdk destroy` every module's `instructor_setup` (M3 infra, M4 ECR, M5, M6 notebook, M7
MLflow, M8 SNS/S3), stop + delete the SageMaker notebook, and empty/delete the pipeline + SageMaker default buckets. Verify
with the read-only sweep in the guide so nothing is left billing. (The teardown CLI is in [§11](#11-aws-cli-quick-reference).)

---

## 9. Interview Questions (15, with hints)

1. **Why use SageMaker Pipelines instead of a notebook or cron scripts?** *(Hint: reproducibility, lineage, caching, condition gates, audit.)*
2. **What is a ProcessingStep vs a TrainingStep?** *(Hint: containerised data job vs a managed training job.)*
3. **How do steps pass data to each other?** *(Hint: `step.properties...` references + `PropertyFile`/`JsonGet`, resolved at run time.)*
4. **What is a Condition step and why gate registration on it?** *(Hint: don't promote a model below the metric bar.)*
5. **Built-in XGBoost vs script-mode XGBoost — when each?** *(Hint: zero-code vs custom training logic / `training.py`.)*
6. **What's a Model Package Group and how does registration relate to M7's registry?** *(Hint: versioned model collection; pipeline registers automatically.)*
7. **Why is `get_execution_role()` enough — no new IAM role?** *(Hint: the notebook's role runs the jobs; least new infra.)*
8. **What is pipeline caching and when does it help?** *(Hint: skip a step whose inputs didn't change; faster iteration.)*
9. **PipelineSession vs the normal Session — why does building need the former?** *(Hint: defers job execution into the pipeline DAG instead of running immediately.)*
10. **How would you trigger the pipeline on *new data arriving* (not a timer)?** *(Hint: S3 PutObject → EventBridge rule → StartPipelineExecution.)*
11. **What does Lambda give you that an EC2 cron doesn't?** *(Hint: serverless, event-driven, no server to manage/patch.)*
12. **EventBridge Scheduler vs an EventBridge rule — what's the difference?** *(Hint: cron/one-off scheduling vs event-pattern matching on the bus.)*
13. **How do you connect M6 drift detection to automated retraining?** *(Hint: drift alert → SNS → EventBridge → pipeline.)*
14. **Where do SNS notifications fit in the pipeline?** *(Hint: callback/Lambda on step success/failure → the M8 topic.)*
15. **Trace one shipment end-to-end through all 8 modules.** *(Hint: raw → feature (M3/M7) → model (M3/M8) → container (M4) → ECS serve (M5) → drift check (M6) → SHAP (M7) → automated retrain (M8).)*

---

## 10. Learning outcomes
You can design and build a SageMaker Pipeline with a quality gate; trigger it via Lambda + EventBridge; register models
automatically into a governed registry; and explain how the capstone closes every loop opened in M3–M7 — the difference
between "I trained a model" and "I built a system that keeps a good model in production by itself."

---

## 11. AWS CLI quick reference

> Set `PIPELINE_BUCKET`, `ACCOUNT`, and `REGION=ap-south-1` first (see [§1](#1-prerequisites--environment)).

**Lab 1 — pipeline:**
```bash
# Create the model package group (idempotent — ignore "already exists")
aws sagemaker create-model-package-group --model-package-group-name TruckDelayModelPackageGroup \
  --model-package-group-description "Truck Delay capstone registry" --region ap-south-1

# Upload the training data
aws s3 cp labs/data/reference/final_features.csv \
  s3://$PIPELINE_BUCKET/truck-delay/input/final_features.csv --region ap-south-1

# Start a run (after the notebook upserts the pipeline) + list its steps
aws sagemaker start-pipeline-execution --pipeline-name TruckDelayClassification \
  --pipeline-parameters Name=F1Threshold,Value=0.55 --region ap-south-1
aws sagemaker list-pipeline-execution-steps --pipeline-execution-arn <arn> --region ap-south-1

# Inspect + approve the registered model
aws sagemaker list-model-packages --model-package-group-name TruckDelayModelPackageGroup \
  --sort-by CreationTime --sort-order Descending --region ap-south-1
aws sagemaker update-model-package --model-package-arn <arn> --model-approval-status Approved --region ap-south-1
```

**Lab 2 — Lambda (pandas-free; no layer):**
```bash
zip function.zip lambda_function.py
aws lambda create-function --function-name truck-delay-land-streaming \
  --runtime python3.12 --handler lambda_function.handler \
  --role arn:aws:iam::$ACCOUNT:role/truck-delay-lambda-role \
  --timeout 60 --memory-size 256 \
  --environment "Variables={PIPELINE_BUCKET=$PIPELINE_BUCKET,BATCH_ROWS=500}" \
  --zip-file fileb://function.zip --region ap-south-1
aws lambda invoke --function-name truck-delay-land-streaming --payload '{}' \
  --cli-binary-format raw-in-base64-out out.json --region ap-south-1 && cat out.json
```

**Lab 3 — EventBridge schedule:**
```bash
aws scheduler create-schedule --name truck-delay-weekly-retrain \
  --schedule-expression "cron(0 6 ? * MON *)" --schedule-expression-timezone "Asia/Kolkata" \
  --flexible-time-window '{"Mode":"OFF"}' \
  --target "{\"Arn\":\"arn:aws:scheduler:::aws-sdk:sagemaker:startPipelineExecution\",\"RoleArn\":\"arn:aws:iam::$ACCOUNT:role/truck-delay-scheduler-role\",\"Input\":\"{\\\"PipelineName\\\":\\\"TruckDelayClassification\\\"}\"}" \
  --region ap-south-1
```

**Teardown:**
```bash
aws scheduler delete-schedule --name truck-delay-weekly-retrain --region ap-south-1
aws lambda delete-function --function-name truck-delay-land-streaming --region ap-south-1
aws sagemaker delete-pipeline --pipeline-name TruckDelayClassification --region ap-south-1
# then: cdk destroy in every instructor_setup/, stop+delete the notebook, empty+delete buckets (see packaging guide)
```
