# M8 · Lab 2 — Lambda: Land New Streaming Data

**Module 8 (Capstone) | Spine: Truck Delay Classification**

| Detail | Value |
|---|---|
| Duration | 45 min (incl. ~10 min concept pre-read) |
| Difficulty | Advanced |
| Tools | AWS Lambda, **AWS CLI / boto3**, S3 |
| AWS Services | **Lambda (FIRST encounter)**, S3, IAM |
| Prerequisite | M8 `instructor_setup` (the `truck-delay-mlops-pipeline-<account>` S3 bucket) + `final_features.csv` uploaded in Lab 1 |
| Builds Toward | Lab 3 (EventBridge triggers Lambda + the pipeline on a schedule) |
| Cost | ~₹0 (Lambda free tier) |

> ⚡ **CLI-first & pandas-free.** The primary function uses only the Python **standard library + boto3** (both already in the
> Lambda runtime), so there is **no layer to attach** and the whole lab is copy-paste from the SageMaker terminal. A
> pandas variant is noted at the end for completeness.

---

## 0a · What is AWS Lambda, and why is it here?

**AWS Lambda** is *serverless, event-driven compute*: you upload a function, pick a trigger (a schedule, an S3 upload, an
API call, an SNS message), and AWS runs it on demand — **no server to provision, patch, or keep running.** You pay only for
the milliseconds it executes. Key ideas in 30 seconds:

| Concept | What it means here |
|---|---|
| **Handler** | the function AWS calls: `lambda_function.handler(event, context)` |
| **Trigger / event** | what invokes it — in Lab 3, an **EventBridge** schedule |
| **Execution role** | the IAM role the function assumes (it needs S3 read/write here) |
| **Runtime** | the language sandbox (Python 3.12). **`boto3` is built in**; pandas/numpy are *not* |
| **Layer** | a way to add extra libraries (e.g. pandas). We **avoid** it by using stdlib only |
| **Cold start** | first invoke after idle is a little slower while AWS spins up the sandbox |

**Why Lambda for this step?** In production, new shipment records **arrive continuously**, and something has to catch each
batch and drop it where the pipeline can find it. Lambda is the canonical serverless "glue" for exactly that. This is your
**first encounter** with Lambda — the learn-then-automate moment.

In this lab the Lambda **simulates** a streaming arrival: it samples fresh rows from the real Truck Delay data and writes a
timestamped batch to S3. Lab 3's EventBridge schedule will invoke it, then kick off the pipeline.

> **Lambda vs an EC2 cron vs Fargate:** EC2 cron means a server you own, patch, and pay for 24/7; Fargate is for
> longer/containerised jobs; **Lambda** wins for short, spiky, event-driven glue like this — zero idle cost, nothing to
> manage. **Production note:** a real version reads Kinesis / an API / a database CDC stream. We simulate the *arrival* so
> the automation is end-to-end testable in class. The spine's `lambda_streaming_data_gen.py`
> (`Projects Repo/Truck Delay PRoject/Part - 3/`) is the real-data-generator reference.

---

## Step 0 · Set your shell variables (run once)

From the SageMaker terminal (or a notebook `!` cell). These make every command below copy-paste-able.

```bash
export REGION=ap-south-1
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export PIPELINE_BUCKET=$(aws cloudformation describe-stacks --stack-name m8-stack \
  --query "Stacks[0].Outputs[?OutputKey=='ArtifactBucket'].OutputValue" --output text --region $REGION)
echo "account=$ACCOUNT  bucket=$PIPELINE_BUCKET"
```

**Expected:** `account=123456789012  bucket=truck-delay-mlops-pipeline-123456789012`. If `PIPELINE_BUCKET` is empty, the M8
stack isn't deployed — ask your instructor (or run `cdk deploy` in `Module 8/instructor_setup/`).

---

## Step 1 · The function code (pandas-free)

**What we're doing:** writing a handler that reads the reference CSV from S3, samples N random rows with the **stdlib**
(`csv`/`random` — no pandas), and writes a timestamped batch back to S3. **Why stdlib:** `boto3` ships in the Lambda
runtime and the standard library covers CSV + sampling, so **no layer is needed** — the #1 source of Lambda friction is
gone.

Create `lambda_function.py`:

```python
import boto3, io, os, random
from datetime import datetime, timezone

s3 = boto3.client("s3")                                   # boto3 is built into the Lambda runtime

BUCKET = os.environ["PIPELINE_BUCKET"]                    # set as an env var on the function
REFERENCE_KEY = os.environ.get("REFERENCE_KEY", "truck-delay/input/final_features.csv")
BATCH_ROWS = int(os.environ.get("BATCH_ROWS", "500"))

def handler(event, context):
    # 1. Read the reference frame straight from S3 (text, no pandas)
    obj = s3.get_object(Bucket=BUCKET, Key=REFERENCE_KEY)
    text = obj["Body"].read().decode("utf-8").splitlines()
    header, rows = text[0], text[1:]

    # 2. Sample a "newly arrived" batch
    batch = random.sample(rows, min(BATCH_ROWS, len(rows)))

    # 3. Write it to a timestamped key the pipeline can pick up
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"truck-delay/streaming/batch_{ts}.csv"
    body = header + "\n" + "\n".join(batch) + "\n"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))

    msg = f"Landed {len(batch)} rows -> s3://{BUCKET}/{key}"
    print(msg)
    return {"statusCode": 200, "rows": len(batch), "key": key}
```

**Reading the code:** lines 1–3 grab config from env vars (so the same code works across buckets); the handler does
*read → sample → write*, returns a small JSON the caller (EventBridge) can log. No third-party imports = no layer.

---

## Step 2 · Create the execution role (CLI)

**What we're doing:** Lambda needs an IAM role it can assume, with permission to read/write the pipeline bucket and write
logs. **Why a dedicated role:** least privilege — this function only touches one bucket.

```bash
# 2a. Trust policy: allow Lambda to assume the role
cat > trust-lambda.json <<'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
 "Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF

aws iam create-role --role-name truck-delay-lambda-role \
  --assume-role-policy-document file://trust-lambda.json

# 2b. Basic execution (CloudWatch Logs)
aws iam attach-role-policy --role-name truck-delay-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# 2c. S3 read+write on just the pipeline bucket
cat > s3-policy.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
 "Action":["s3:GetObject","s3:PutObject","s3:ListBucket"],
 "Resource":["arn:aws:s3:::$PIPELINE_BUCKET","arn:aws:s3:::$PIPELINE_BUCKET/*"]}]}
EOF

aws iam put-role-policy --role-name truck-delay-lambda-role \
  --policy-name s3-access --policy-document file://s3-policy.json
```

**Expected:** `create-role` returns the role JSON (note the `Arn`). The other two return nothing on success.

---

## Step 3 · Package + create the function (CLI)

**What we're doing:** zipping the one file and creating the function. IAM roles take a few seconds to propagate, so if
`create-function` complains the role "cannot be assumed," wait ~10 s and retry.

```bash
zip function.zip lambda_function.py

aws lambda create-function \
  --function-name truck-delay-land-streaming \
  --runtime python3.12 --handler lambda_function.handler \
  --role arn:aws:iam::$ACCOUNT:role/truck-delay-lambda-role \
  --timeout 60 --memory-size 256 \
  --environment "Variables={PIPELINE_BUCKET=$PIPELINE_BUCKET,BATCH_ROWS=500}" \
  --zip-file fileb://function.zip --region $REGION
```

**Expected:** a JSON description of the function with `"State": "Active"`. **To update the code later:**
`zip function.zip lambda_function.py && aws lambda update-function-code --function-name truck-delay-land-streaming --zip-file fileb://function.zip --region $REGION`.

---

## Step 4 · Invoke + verify (CLI)

**What we're doing:** firing the function with an empty event and confirming a batch landed in S3.

```bash
aws lambda invoke --function-name truck-delay-land-streaming \
  --payload '{}' --cli-binary-format raw-in-base64-out \
  out.json --region $REGION && cat out.json

# Confirm the object exists
aws s3 ls s3://$PIPELINE_BUCKET/truck-delay/streaming/ --region $REGION
```

**Expected:**
```
{"statusCode": 200, "rows": 500, "key": "truck-delay/streaming/batch_20260615T100412.csv"}
2026-06-15 10:04:13      78213 batch_20260615T100412.csv
```
**Read it:** the function returned `200` with the row count + key, and `s3 ls` shows the timestamped batch — a simulated
"new arrival" now sitting where the pipeline (or Lab 3's schedule) can pick it up.

---

## Step 5 · (Console path — optional alternative)
Prefer clicking? **Lambda → Create function → Author from scratch** (name `truck-delay-land-streaming`, runtime
**Python 3.12**) → paste `lambda_function.py`, handler `lambda_function.handler` → **Configuration → Environment variables**
add `PIPELINE_BUCKET` + `BATCH_ROWS` → **Configuration → Permissions** attach the S3 policy from Step 2 → **Test** with an
empty `{}` event. Same result as the CLI, more clicks.

---

## Step 6 · (Preview) make it trigger the pipeline
You *could* call `boto3.client("sagemaker").start_pipeline_execution(...)` at the end of the handler so "data landed"
directly triggers the pipeline. The cleaner classroom pattern is **EventBridge** orchestrating both — that's Lab 3. For
now, the Lambda just lands data.

---

## Verification Checklist
- [ ] `truck-delay-land-streaming` created (Python 3.12) — **no layer needed** (stdlib + boto3).
- [ ] Execution role `truck-delay-lambda-role` can read+write the pipeline bucket.
- [ ] Env vars `PIPELINE_BUCKET` + `BATCH_ROWS` set.
- [ ] A test invoke returned `200` and wrote a `batch_<ts>.csv` to `truck-delay/streaming/`.
- [ ] You can explain why Lambda (serverless, event-driven) fits "catch each arriving batch."

## What's next — Lab 3
EventBridge puts this on a **schedule** and chains it to `StartPipelineExecution` — the last piece of full automation.

## Troubleshooting
| Symptom | Fix |
|---|---|
| `create-function`: role "cannot be assumed" | IAM propagation lag — wait ~10 s and retry. |
| `AccessDenied` on S3 in the logs | Execution role missing `s3:GetObject/PutObject` on the bucket (Step 2c). |
| `KeyError: 'PIPELINE_BUCKET'` | Env var not set on the function — add it (`--environment` or Console). |
| `NoSuchKey` for the reference CSV | Upload it first (Lab 1 Step 2) or set `REFERENCE_KEY` to where it lives. |
| `Invalid base64` on `--payload` | Add `--cli-binary-format raw-in-base64-out` (AWS CLI v2). |
| Want pandas anyway | Swap the stdlib sampling for pandas and attach the AWS-managed layer `arn:aws:lambda:$REGION:336392948345:layer:AWSSDKPandas-Python312:<ver>` (find `<ver>` in the Console layer picker). The stdlib version avoids this entirely. |

## Cleanup (or do it in the final teardown)
```bash
aws lambda delete-function --function-name truck-delay-land-streaming --region $REGION
aws iam delete-role-policy --role-name truck-delay-lambda-role --policy-name s3-access
aws iam detach-role-policy --role-name truck-delay-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam delete-role --role-name truck-delay-lambda-role
```
