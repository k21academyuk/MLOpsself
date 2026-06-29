# M8 · Lab 3 — EventBridge: Schedule the Whole Loop

**Module 8 (Capstone) | Spine: Truck Delay Classification**

| Detail | Value |
|---|---|
| Duration | 40 min (incl. ~10 min concept pre-read) |
| Difficulty | Advanced |
| Tools | Amazon EventBridge (Scheduler), **AWS CLI / boto3**, Lambda, SageMaker Pipelines |
| AWS Services | **EventBridge (FIRST encounter)**, Lambda, SageMaker, IAM |
| Prerequisite | Lab 1 (pipeline upserted), Lab 2 (streaming Lambda) |
| Builds Toward | The fully automated spine — no human in the loop |
| Cost | ~₹0 (EventBridge free tier) |

> ⚡ **CLI-first.** Full copy-paste commands for the scheduler role + schedule, plus a **prove-it-now** path so you don't
> wait until Monday. Console steps are kept as an alternative.

---

## 0a · What is EventBridge — and which part do we use?

**Amazon EventBridge** is AWS's serverless event backbone. It has two faces; know the difference (interview favourite):

| Feature | What it does | We use it for |
|---|---|---|
| **EventBridge Scheduler** | runs a target on a **cron / rate / one-off** schedule | ✅ "every Monday 06:00, start the pipeline" |
| **EventBridge Rules (event bus)** | matches an **event pattern** on the bus and routes it to targets | the *event-driven* alternative (e.g. S3 PutObject → pipeline) |

Other vocabulary you'll meet:
- **Schedule expression** — `cron(0 6 ? * MON *)` (fields: min hour day-of-month month day-of-week year) or `rate(1 day)`
  or `at(2026-06-15T10:30:00)` for a one-off. Scheduler supports **timezones** (`Asia/Kolkata`) — no hand-converting to UTC.
- **Target** — what fires: here, SageMaker's `StartPipelineExecution` (a *universal AWS SDK target* — Scheduler can call
  almost any AWS API directly) and/or the Lab 2 Lambda.
- **Execution role** — the IAM role Scheduler assumes to call the target (needs `sagemaker:StartPipelineExecution` and/or
  `lambda:InvokeFunction`).

**Why EventBridge closes the capstone?** You have a pipeline you can `start()` by hand and a Lambda that lands data.
EventBridge is what makes them run **on their own** — the moment the human leaves the loop, the Truck Delay system
monitors, retrains, and re-registers *itself*.

Two clean patterns — pick one (or both):
```
A)  EventBridge Scheduler ──(cron)──▶  Lambda (land data)  ──then──▶  starts the SageMaker Pipeline
B)  EventBridge Scheduler ──(cron)──▶  SageMaker Pipeline directly (StartPipelineExecution target)
```
Pattern **B** is simplest when data is already arriving; pattern **A** chains the data-landing Lambda first. We set up **B**,
then extend to **A**.

---

## Step 0 · Set your shell variables (run once)
```bash
export REGION=ap-south-1
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export PIPELINE_NAME=TruckDelayClassification
echo "account=$ACCOUNT  pipeline=$PIPELINE_NAME"
```

---

## Step 1 · Create the scheduler's execution role (CLI)

**What we're doing:** EventBridge Scheduler needs a role it can assume to call your targets. **Why:** the schedule itself
has no permissions — it acts *as* this role when it fires.

```bash
# 1a. Trust policy: allow EventBridge Scheduler to assume the role
cat > trust-scheduler.json <<'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
 "Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF

aws iam create-role --role-name truck-delay-scheduler-role \
  --assume-role-policy-document file://trust-scheduler.json

# 1b. Permissions: start the pipeline (Pattern B) + invoke the Lambda (Pattern A)
cat > scheduler-policy.json <<EOF
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":"sagemaker:StartPipelineExecution",
  "Resource":"arn:aws:sagemaker:$REGION:$ACCOUNT:pipeline/$PIPELINE_NAME"},
 {"Effect":"Allow","Action":"lambda:InvokeFunction",
  "Resource":"arn:aws:lambda:$REGION:$ACCOUNT:function:truck-delay-land-streaming"}
]}
EOF

aws iam put-role-policy --role-name truck-delay-scheduler-role \
  --policy-name invoke-targets --policy-document file://scheduler-policy.json
```
**Expected:** `create-role` returns the role JSON; `put-role-policy` returns nothing on success.

---

## Step 2 · Create the weekly schedule → pipeline (Pattern B, CLI)

**What we're doing:** a recurring cron schedule whose target is SageMaker's `StartPipelineExecution`. The `Input` JSON is
what gets passed to that API (the pipeline name, and optionally parameters).

```bash
aws scheduler create-schedule \
  --name truck-delay-weekly-retrain \
  --schedule-expression "cron(0 6 ? * MON *)" \
  --schedule-expression-timezone "Asia/Kolkata" \
  --flexible-time-window '{"Mode":"OFF"}' \
  --target "{
     \"Arn\":\"arn:aws:scheduler:::aws-sdk:sagemaker:startPipelineExecution\",
     \"RoleArn\":\"arn:aws:iam::$ACCOUNT:role/truck-delay-scheduler-role\",
     \"Input\":\"{\\\"PipelineName\\\":\\\"$PIPELINE_NAME\\\"}\"
   }" \
  --region $REGION
```
**Expected:** `{"ScheduleArn": "arn:aws:scheduler:ap-south-1:...:schedule/default/truck-delay-weekly-retrain"}`.

**Reading it:** `cron(0 6 ? * MON *)` = 06:00 every Monday, Asia/Kolkata. The target Arn
`arn:aws:scheduler:::aws-sdk:sagemaker:startPipelineExecution` is the *universal target* form — Scheduler calling the
SageMaker API directly, no Lambda in between. `Input` is the API payload (note the escaped inner JSON).

### Console alternative
**EventBridge → Scheduler → Create schedule** → Recurring → cron `0 6 ? * MON *`, timezone Asia/Kolkata → Flexible window
Off → Target = **AWS SDK → SageMaker → StartPipelineExecution**, PipelineName `TruckDelayClassification` → let it create the
role → Create.

---

## Step 3 · Prove it now — don't wait a week

Two quick ways to confirm the wiring without waiting for Monday:

**(a) Fire the target action directly** (proves the role + pipeline work):
```bash
aws sagemaker start-pipeline-execution --pipeline-name $PIPELINE_NAME \
  --pipeline-parameters Name=F1Threshold,Value=0.55 --region $REGION
```

**(b) Create a one-time schedule ~3 minutes out** (proves the *schedule* itself fires):
```bash
RUN_AT=$(date -u -d "+3 minutes" +%Y-%m-%dT%H:%M:%S)        # SageMaker/Linux date math
aws scheduler create-schedule \
  --name truck-delay-test-once \
  --schedule-expression "at($RUN_AT)" --schedule-expression-timezone "UTC" \
  --flexible-time-window '{"Mode":"OFF"}' \
  --action-after-completion DELETE \
  --target "{\"Arn\":\"arn:aws:scheduler:::aws-sdk:sagemaker:startPipelineExecution\",\"RoleArn\":\"arn:aws:iam::$ACCOUNT:role/truck-delay-scheduler-role\",\"Input\":\"{\\\"PipelineName\\\":\\\"$PIPELINE_NAME\\\"}\"}" \
  --region $REGION
echo "One-time schedule set for $RUN_AT UTC (auto-deletes after it runs)."
```
**Watch it land:**
```bash
aws sagemaker list-pipeline-executions --pipeline-name $PIPELINE_NAME \
  --sort-by CreationTime --sort-order Descending --max-results 3 --region $REGION
```
**Expected:** within a few minutes a new execution appears (`Executing` → `Succeeded`). In Studio → Pipelines →
`TruckDelayClassification` you'll see it, triggered by EventBridge — not by you.

> **Console "Run now":** EventBridge Scheduler has no native run-now; the one-time `at()` schedule above is the equivalent.

---

## Step 4 · (Pattern A) chain the data-landing Lambda first
If you want fresh data landed *before* each run:
```bash
# A second schedule 5 minutes earlier, targeting the Lab 2 Lambda
aws scheduler create-schedule \
  --name truck-delay-weekly-land \
  --schedule-expression "cron(55 5 ? * MON *)" --schedule-expression-timezone "Asia/Kolkata" \
  --flexible-time-window '{"Mode":"OFF"}' \
  --target "{\"Arn\":\"arn:aws:lambda:$REGION:$ACCOUNT:function:truck-delay-land-streaming\",\"RoleArn\":\"arn:aws:iam::$ACCOUNT:role/truck-delay-scheduler-role\"}" \
  --region $REGION
```
Result: **05:55** the Lambda lands a batch → **06:00** the pipeline runs on the freshest data.

> **Even tighter (event-driven):** have the Lambda call `start_pipeline_execution(...)` itself at the end, so "data landed"
> *directly* triggers the pipeline — or use an **EventBridge Rule** on an S3 `PutObject` to the `streaming/` prefix targeting
> the pipeline. The two-timer version is easier to reason about in class.

---

## Step 5 · The full automated picture
```
EventBridge (cron)
   ├─▶ Lambda: land fresh batch to s3://.../streaming/        (Pattern A)
   └─▶ SageMaker Pipeline: Process → Train → Evaluate → Condition(f1≥0.55)
                                                          ├─ Register new model version (M7 registry)
                                                          └─ Fail → SNS alert (M6 skill, M8 topic)
```
That is the entire course, automated: **features (M3/M7) → trained model (M3/M8) → evaluated + gated (M8) → registered
(M7) → monitored + alerted (M6) → on a schedule (M8)**, all in one account, one VPC, ap-south-1.

---

## Verification Checklist
- [ ] `truck-delay-scheduler-role` created; it can `StartPipelineExecution` (and `InvokeFunction` for Pattern A).
- [ ] `truck-delay-weekly-retrain` schedule created with a SageMaker `StartPipelineExecution` target.
- [ ] A **one-time `at()` schedule** (or direct start) triggered a real execution, visible in Studio / `list-pipeline-executions`.
- [ ] (optional) a second schedule lands data via the Lambda before the run.
- [ ] You can draw the full automated loop from memory and explain Scheduler vs Rules.

## Teardown reminder
This is the **end of the course**. Delete the schedules so the pipeline doesn't keep running, then follow the
[Course Completion Packaging Guide](../M8_Course_Completion_Packaging_Guide.md) to `cdk destroy` every module's
`instructor_setup` and stop/delete the SageMaker notebook.
```bash
aws scheduler delete-schedule --name truck-delay-weekly-retrain --region $REGION
aws scheduler delete-schedule --name truck-delay-weekly-land --region $REGION 2>/dev/null
aws scheduler delete-schedule --name truck-delay-test-once --region $REGION 2>/dev/null
aws iam delete-role-policy --role-name truck-delay-scheduler-role --policy-name invoke-targets
aws iam delete-role --role-name truck-delay-scheduler-role
```

## Troubleshooting
| Symptom | Fix |
|---|---|
| Schedule runs but pipeline doesn't start | The scheduler role lacks `sagemaker:StartPipelineExecution` (Step 1b), or the resource ARN doesn't match the pipeline. |
| `ValidationException: pipeline not found` | The pipeline name must match `upsert()` exactly (`TruckDelayClassification`). |
| `create-schedule` fails on the role | IAM propagation lag — wait ~10 s and retry. |
| `at(...)` time already passed | `date` produced a past timestamp (clock skew) — bump to `+5 minutes`. |
| Timezone confusion | Set `--schedule-expression-timezone "Asia/Kolkata"`; don't hand-convert to UTC. |
| Want event-driven, not a timer | Use an **EventBridge Rule** on S3 `PutObject` to the `streaming/` prefix → target the pipeline. |
