# M6 Lab 1 — SNS Topic + Email Subscription + Python Alerter

**Module 6 — Monitoring, Testing & Drift Detection | Spine Project: Truck Delay Classification**

| Detail | Value |
|---|---|
| Duration | 45 minutes |
| Difficulty | Beginner |
| Tools | AWS Console + AWS CLI v2 + Python (`boto3`) |
| AWS Services | **SNS (Simple Notification Service)** — first encounter, IAM |
| Prerequisite | AWS account with `AmazonSNSFullAccess` (or `AdministratorAccess`) |
| Builds Toward | Lab 2 + Lab 3 (which produce drift signals); Lab 4 wires them to the topic created here |
| Cost Estimate | ₹0 — SNS topics are free idle; first 1M publishes + 1k email deliveries per month are free |

---

## Learning Objectives

By the end of this lab you will be able to:

1. Explain SNS's pub-sub model: **topic** (the channel), **publisher** (your code), **subscriber** (email / SMS / Lambda / SQS / HTTPS).
2. Create an SNS topic via Console and via CLI, and tell when to use which.
3. Subscribe an email endpoint and complete the **double opt-in** confirmation flow.
4. Publish a message from `boto3` with a **structured JSON payload** and **MessageAttributes** for downstream filtering.
5. Reason about **alert hygiene** — severity levels, dedup, routing — before drift floods the inbox.

---

## Business Context

Last quarter the Truck Delay model started giving consistently optimistic predictions — it kept saying "on-time" when reality was "20% delayed". By the time ops noticed, three weeks of bad scheduling decisions were on the books. The post-mortem identified two failure layers:

1. **No drift detection in place** — no signal that the data distribution had moved.
2. **Even if there had been a signal, nothing alerted anyone** — it would have sat in a log file no one reads.

This lab tackles layer 2 first. We're setting up the alerting channel before we have anything to alert about, because debugging "did my drift signal even fire?" is hard if the plumbing is also broken. With a confirmed SNS subscription, every later lab has a known-good place to publish to.

---

## Prerequisites

### AWS account + region

```bash
aws sts get-caller-identity --query Account --output text
# Returns your 12-digit account ID -- note it as <ACCOUNT_ID>

export AWS_REGION=ap-south-1
```

### A real email you can check

The subscription requires you to click a confirmation link sent to the email address you provide. If the address is unreachable / fake, the subscription stays in `PendingConfirmation` forever.

---

## Step 1: Create the SNS Topic

### Console clicks

1. AWS Console → search **SNS** → open **Simple Notification Service**.
2. Left sidebar → **Topics** → top-right **Create topic**.
3. Fill in:

| Field | Value | Why |
|---|---|---|
| Type | **Standard** | FIFO topics enforce ordering + de-dup but cost more and don't support email subscribers. Standard is right for alerts. |
| Name | `truck-delay-alerts` | Referenced by name in every subsequent M6 lab + the M8 capstone |
| Display name | `TruckDelayAlerts` | Shows up as the email "From" prefix (must be ≤10 chars for SMS — irrelevant for email but courteous to set) |
| Encryption | (default — disabled) | Enable AWS-managed KMS encryption for compliance-sensitive workloads. For class, default is fine. |
| Access policy | (default — only the topic owner can publish) | Restrict to the account that owns the topic. |

4. **Create topic**. Note the **ARN** that appears on the success page — looks like `arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts`. You'll paste this everywhere.

`[SCREENSHOT: SNS "Create topic" form filled with name = truck-delay-alerts and type = Standard]`

### CLI alternative (one command)

```bash
aws sns create-topic --name truck-delay-alerts --region $AWS_REGION
```

Output:
```json
{ "TopicArn": "arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts" }
```

Save the ARN as a shell variable for later steps:
```bash
export TOPIC_ARN=arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts
```

### Standard vs FIFO — when each makes sense

| Aspect | Standard topic (what we're using) | FIFO topic |
|---|---|---|
| Ordering | Best-effort | Strict in-order delivery |
| Deduplication | At-most-once except for retries | Exactly-once within a 5-min window |
| Throughput | Effectively unlimited | 300 publishes/sec |
| Subscribers | Email, SMS, Lambda, SQS, HTTPS, Firehose | SQS FIFO only |
| Cost | Cheaper | ~3× more expensive |
| Use case | Alerts, fanout notifications (this lab) | Order-critical financial events, event-sourcing pipelines |

For drift alerts: order doesn't matter, occasional duplicates are tolerable, and we want email subscribers. Standard wins.

---

## Step 2: Subscribe Your Email + Confirm

### Console

1. On the topic detail page → tab **Subscriptions** → **Create subscription**.
2. Fill in:

| Field | Value |
|---|---|
| Topic ARN | (auto-filled) |
| Protocol | **Email** |
| Endpoint | `your@email.com` |

3. **Create subscription**. Status will show as `Pending confirmation`.

4. **Check your email.** Within 1 minute you'll get an email from `no-reply@sns.amazonaws.com` with subject `AWS Notification - Subscription Confirmation`. Click the **Confirm subscription** link in the email body.

5. Back in the Console, refresh — status changes to `Confirmed`.

`[SCREENSHOT: Subscription list showing email subscription with status = Confirmed]`

### CLI alternative

```bash
aws sns subscribe \
    --topic-arn $TOPIC_ARN \
    --protocol email \
    --notification-endpoint your@email.com \
    --region $AWS_REGION
```

Output:
```json
{ "SubscriptionArn": "pending confirmation" }
```

Then **check your email** and click the confirmation link, same as the Console flow.

> **Why double opt-in?** AWS doesn't want SNS used to spam arbitrary email addresses. Anyone can create a topic; only the email owner can prove they want to receive its messages. Same pattern as Mailchimp / SendGrid.

### Verify the subscription is active

```bash
aws sns list-subscriptions-by-topic \
    --topic-arn $TOPIC_ARN \
    --region $AWS_REGION \
    --query "Subscriptions[].{Endpoint:Endpoint, Status:SubscriptionArn}"
```

Expected:
```json
[ { "Endpoint": "your@email.com", "Status": "arn:aws:sns:...:<subscription-uuid>" } ]
```

`SubscriptionArn = "PendingConfirmation"` means you haven't clicked the link yet.

---

## Step 3: Send a Test Message from the Console

Quick smoke-test before writing any code.

1. Topic detail page → top-right **Publish message**.
2. **Subject:** `Test alert from M6 Lab 1`
3. **Message body:**
   ```
   This is a manual test message confirming the SNS topic + email subscription work end-to-end.
   ```
4. **Publish message**.

Within ~10 seconds, you should receive the email at the subscribed address. If you don't:
- Check spam/junk
- Confirm the subscription is `Confirmed` (Step 2)
- Check that the email address is correct (typos cost minutes)

`[SCREENSHOT: Browser email client showing the test message]`

---

## Step 4: Publish from Python (the real path forward)

Manual Console messages are fine for smoke-testing. The labs from here on publish programmatically.

### Install boto3

```bash
pip install "boto3>=1.34"
```

### Write `sns_publish_test.py`

```python
# sns_publish_test.py
"""Smoke-test the M6 SNS topic with a structured JSON alert message.

Run: python sns_publish_test.py
"""
import json
import os
from datetime import datetime, timezone

import boto3

TOPIC_ARN = os.environ["TOPIC_ARN"]  # exported in your shell

sns = boto3.client("sns", region_name=os.environ.get("AWS_REGION", "ap-south-1"))

payload = {
    "schema_version": "1.0",
    "alert_type":     "drift",                    # drift | data_validation | model_error
    "severity":       "warning",                  # info | warning | critical
    "service":        "truck-delay-classifier",
    "environment":    "production",
    "detected_at":    datetime.now(timezone.utc).isoformat(),
    "summary":        "Test alert -- ignore. Lab 1 smoke test.",
    "details": {
        "drifted_features":   [],
        "validation_errors":  [],
    },
    "runbook_url":  "https://wiki.freshbasket.in/runbooks/truck-delay-drift",
}

response = sns.publish(
    TopicArn=TOPIC_ARN,
    # Subject is shown as the email subject line for email subscribers
    Subject=f"[{payload['severity'].upper()}] Truck Delay {payload['alert_type']} -- TEST",
    Message=json.dumps(payload, indent=2),
    # MessageAttributes let downstream subscribers (e.g., a Lambda) filter
    # without parsing the body
    MessageAttributes={
        "alert_type": {"DataType": "String", "StringValue": payload["alert_type"]},
        "severity":   {"DataType": "String", "StringValue": payload["severity"]},
        "service":    {"DataType": "String", "StringValue": payload["service"]},
    },
)

print(f"Published. MessageId = {response['MessageId']}")
```

### Run it

```bash
# 🪟 Windows PowerShell: $env:TOPIC_ARN = "arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts"
# Linux/macOS/Git Bash:  export TOPIC_ARN=arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts

python sns_publish_test.py
```

Expected output:
```
Published. MessageId = a1b2c3d4-...
```

Check your email — the body is now the pretty-printed JSON payload.

### What's interesting about this code

- **`Subject`** — gets used as the email subject line. Lead with severity in brackets so on-call can triage at a glance.
- **`Message`** — a JSON string. Email subscribers see the raw JSON in the body; a Lambda subscriber can `json.loads()` it. One body, multiple consumers.
- **`MessageAttributes`** — key-value metadata separate from the body. Use these to set up **SNS filter policies** later (e.g., one subscriber only wants `severity=critical`).

> **Production patterns we're previewing:** real teams add `correlation_id`, `model_version`, and `dataset_id` fields here so when the on-call investigates they can trace exactly which model + data window triggered the alert. M8 will revisit this.

---

## Step 5 (optional): Add a Slack subscription via Lambda

Skip if you're short on time — this is a "good to have seen once" extension.

The high-level shape:
1. Create a Slack incoming webhook URL for your channel.
2. Write a tiny Lambda function in Python that receives the SNS event, formats the message as a Slack blocks payload, and POSTs to the webhook.
3. Subscribe the Lambda to your SNS topic.

The Lambda handler is ~25 lines:

```python
import json
import os
import urllib.request

SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK_URL"]


def handler(event, context):
    for record in event["Records"]:
        sns = record["Sns"]
        msg = json.loads(sns["Message"]) if sns["Message"].startswith("{") else {"summary": sns["Message"]}

        slack_payload = {
            "text": sns.get("Subject", "MLOps alert"),
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": sns.get("Subject", "MLOps alert")}},
                {"type": "section", "text": {"type": "mrkdwn",
                    "text": f"*Severity:* {msg.get('severity', '?')}\n"
                            f"*Service:* {msg.get('service', '?')}\n"
                            f"*Detected:* {msg.get('detected_at', '?')}\n\n"
                            f"{msg.get('summary', '')}"}},
            ],
        }

        req = urllib.request.Request(
            SLACK_WEBHOOK,
            data=json.dumps(slack_payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req).read()
    return {"statusCode": 200}
```

You'd give the Lambda execution role permission to read from SNS (the SNS subscription itself grants the invoke; the role just needs basic execution + outbound network).

#### Deploy it from the CLI (no Console needed)

The whole fan-out is four CLI calls. Save the handler above as `handler.py`, then `zip slack_alerter.zip handler.py` (🪟 PowerShell: `Compress-Archive handler.py slack_alerter.zip`):

```bash
# 1. Minimal execution role -- Lambda basic logging only.
#    SNS invokes the function, so the role needs NO sns:* permissions.
aws iam create-role --role-name m6-slack-alerter-role \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy --role-name m6-slack-alerter-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# 2. Create the function (--handler is <file>.<function> => handler.handler)
aws lambda create-function --function-name m6-slack-alerter \
    --runtime python3.12 --handler handler.handler \
    --role arn:aws:iam::<ACCOUNT_ID>:role/m6-slack-alerter-role \
    --environment "Variables={SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ}" \
    --zip-file fileb://slack_alerter.zip --region $AWS_REGION

# 3. Let SNS invoke the function
aws lambda add-permission --function-name m6-slack-alerter \
    --statement-id sns-invoke --action lambda:InvokeFunction \
    --principal sns.amazonaws.com --source-arn $TOPIC_ARN --region $AWS_REGION

# 4. Subscribe the function to the topic
aws sns subscribe --topic-arn $TOPIC_ARN --protocol lambda \
    --notification-endpoint arn:aws:lambda:<REGION>:<ACCOUNT_ID>:function:m6-slack-alerter \
    --region $AWS_REGION
```

> **Unlike the email subscription, a Lambda subscription needs no confirmation click** — `add-permission` (step 3) is the trust handshake instead. Publish a test message (Step 3 or 4 above) and the Slack post should appear within a second or two. Teardown: `aws sns unsubscribe`, `aws lambda delete-function --function-name m6-slack-alerter`, then delete the role.

M8 walks through Lambda creation via the Console in detail — for now, knowing "subscribe a Lambda to fan out to Slack" (and how to do it from the CLI) is enough.

---

## Step 6: Alert Hygiene — Set Yourself Up to Not Hate Alerts in Production

You're about to wire two automated checks (Evidently + GE) to publish into this topic. Without rules, drift detectors publish *constantly*. By the time Lab 4 is wired up, plan for these:

| Rule | Why |
|---|---|
| **Severity levels first** | Always include `severity` in MessageAttributes. Email gets `critical` only; everything else goes to a Slack `#ml-monitoring-info` channel via Lambda fanout. |
| **Dedup window** | A drift detector that re-runs hourly will alert hourly until the drift goes away — 168 alerts/week. Add a "last alert sent at" cache (small DynamoDB table, or even a local file for solo use) and suppress identical alerts within N hours. |
| **Runbook URL in every alert** | The on-call wakes up at 3 AM. The alert needs to link to "what do I do?" — paste the URL. Even a stub doc is better than nothing. |
| **One-line summary in `Subject`** | Email triage is on-glance. `[WARNING] Truck Delay drift on route_avg_temp` beats `Alert from MLOps`. |
| **Test alerts on creation** | You did this in Step 3. Do it after any subscription change. Silent failure is the worst failure. |

We won't build dedup in this course (it's a one-page DynamoDB exercise outside the M6 scope), but design with the slot in mind.

---

## Verification Checklist

Before moving on to Lab 2:

- [ ] `aws sns list-topics --region ap-south-1` shows `truck-delay-alerts`
- [ ] `aws sns list-subscriptions-by-topic ...` shows your email with a real `SubscriptionArn` (not `PendingConfirmation`)
- [ ] Console "Publish message" delivered an email within ~30 seconds
- [ ] `python sns_publish_test.py` printed a MessageId and you received the JSON-body email
- [ ] You can articulate the difference between `Subject`, `Message`, and `MessageAttributes`
- [ ] You can write down the topic ARN from memory

If any of these fail, see **Troubleshooting** below before moving to Lab 2.

---

## What's next — Lab 2

You've built the *channel*. The next two labs build the *signals* — Lab 2 generates drift signals from Evidently; Lab 3 generates validation signals from Great Expectations. Lab 4 wires both into this topic. By the end of Lab 4, every drift / validation failure becomes an email.

---

## Troubleshooting

| Symptom | Diagnosis | Fix |
|---|---|---|
| Email subscription stays `PendingConfirmation` | Confirmation email not clicked yet; or it went to spam | Check spam folder for `no-reply@sns.amazonaws.com`. Resend via Console → Subscriptions → Request confirmation |
| `python sns_publish_test.py` errors `botocore.exceptions.NoRegionError` | `AWS_REGION` not set in environment | `export AWS_REGION=ap-south-1` or pass `region_name=` to `boto3.client(...)` |
| `boto3` errors `AccessDenied: User ... is not authorized to perform: sns:Publish` | IAM user lacks SNS perms | Attach `AmazonSNSFullAccess` to the user, or write a scoped policy granting `sns:Publish` on this topic ARN |
| Email arrives but the body looks like `Records[0].Sns.Message: ...` | You subscribed Lambda not email | Re-check the subscription protocol; recreate as Email |
| Email landed but SubjectLine is just "AWS Notification" | You forgot to set `Subject` in the publish call | Always pass `Subject="..."` — required for email readability |
| Want to subscribe SMS but it stays `PendingConfirmation` | India SMS requires DLT registration (regulatory) | Use email for the course. SMS to international numbers works but costs money and Indian SMS requires DLT. |
| `aws sns subscribe` returned `SubscriptionArn: pending confirmation` but no email | Wrong email address typed; or AWS rate-limited resends | Check the address; delete the pending subscription and recreate |
| Test message published but no email arrives within 5 min | Topic policy blocks publish OR subscription deleted | Check `aws sns get-topic-attributes` for the `Policy` field; check subscription status |

---

## Quick reference

```bash
# Topic
aws sns create-topic --name truck-delay-alerts --region ap-south-1
export TOPIC_ARN=arn:aws:sns:ap-south-1:<ACCOUNT_ID>:truck-delay-alerts

# Subscribe (then click the confirmation link in your inbox)
aws sns subscribe --topic-arn $TOPIC_ARN --protocol email --notification-endpoint you@example.com

# Verify
aws sns list-subscriptions-by-topic --topic-arn $TOPIC_ARN --query "Subscriptions[].{Endpoint:Endpoint, Status:SubscriptionArn}"

# Publish (Console: top-right "Publish message")
aws sns publish --topic-arn $TOPIC_ARN --subject "Test" --message "Smoke test"

# Python
python sns_publish_test.py
```

Save this command set. Labs 4 and 8 reuse the topic ARN — write it down somewhere durable.
