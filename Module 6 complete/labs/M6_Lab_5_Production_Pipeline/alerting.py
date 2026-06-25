"""
alerting.py — severity routing, structured payload, and SNS publish. All functions.

The payload is a plain dict (NOT a class) so any consumer — email, a Slack
Lambda, the M8 pipeline trigger — parses it without importing our code.
"""
import json
from datetime import datetime, timezone

import config


def route_severity(alert_type: str, drift_share: float = 0.0) -> str:
    """GE failures are always critical; drift severity scales with how much drifted."""
    if alert_type == "ge_validation":
        return "critical"
    if drift_share > config.SEVERITY_CRITICAL_SHARE:
        return "critical"
    if drift_share > config.SEVERITY_WARNING_SHARE:
        return "warning"
    return "info"


def build_alert_payload(alert_type: str, severity: str, summary: str, details: dict) -> dict:
    """The structured message published on every failure path. 8 stable top-level fields."""
    return {
        "schema_version": "1.0",
        "alert_type": alert_type,            # ge_validation | drift
        "severity": severity,                # info | warning | critical
        "service": config.SERVICE_NAME,
        "environment": config.ENVIRONMENT,
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "details": details,
        "runbook_url": f"https://wiki.freshbasket.in/runbooks/{alert_type}",
    }


def publish_alert(payload: dict, dry_run: bool = False):
    """Publish to SNS, or print the payload when dry_run / no topic configured. Returns MessageId or None."""
    subject = f"[{payload['severity'].upper()}] {payload['service']} {payload['alert_type']}"
    body = json.dumps(payload, indent=2)

    if dry_run or not config.TOPIC_ARN:
        reason = "dry-run" if dry_run else "no TOPIC_ARN configured"
        print(f"\n── ALERT ({reason}) — would publish to {config.TOPIC_ARN or '<unset>'}")
        print("Subject:", subject)
        print(body, "\n")
        return None

    import boto3
    resp = boto3.client("sns", region_name=config.AWS_REGION).publish(
        TopicArn=config.TOPIC_ARN,
        Subject=subject[:99],                # SNS subject limit
        Message=body,
        MessageAttributes={
            k: {"DataType": "String", "StringValue": payload[k]}
            for k in ("alert_type", "severity", "service")})
    return resp["MessageId"]
