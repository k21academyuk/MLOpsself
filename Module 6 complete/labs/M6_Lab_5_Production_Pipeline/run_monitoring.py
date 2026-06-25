#!/usr/bin/env python3
"""
run_monitoring.py — production entry point for Truck Delay model monitoring.

Composes the two checks from Labs 2-3 with a clean exit-code contract so it runs
identically under cron, Airflow, EventBridge, or a SageMaker Processing step:

    GE validate (cheap, fail-fast)  ->  if fail: publish critical alert, exit 1
    Evidently drift (expensive)     ->  if drift: publish info|warning|critical, exit 1
    both pass                       ->  log success, exit 0

Usage
-----
    # Score a real production dump (CSV or Parquet)
    python run_monitoring.py --current production_batch.parquet

    # Self-test with a synthetic drifted batch (no real prod data in class)
    python run_monitoring.py --simulate
    python run_monitoring.py --simulate-corrupt

    # Print payloads but DON'T publish to SNS
    python run_monitoring.py --simulate --dry-run

Environment (see config.py): TOPIC_ARN, AWS_REGION, M6_DATA_DIR, GE_PROJECT_DIR,
ENVIRONMENT, SEVERITY_WARNING_SHARE, SEVERITY_CRITICAL_SHARE.

Exit codes: 0 healthy · 1 alert published · 2 usage/config error.
"""
import argparse
import sys
from datetime import datetime, timezone

import config
from alerting import build_alert_payload, publish_alert, route_severity
from checks import run_drift_detection, run_ge_validation
from data_sources import (load_batch_file, load_reference, simulate_corrupt_batch,
                          simulate_drifted_batch)


def _log(stage, msg):
    print(f"[{stage}] {msg}")


def run_monitoring(current, reference, dry_run=False) -> int:
    """Run both checks on one batch. Returns 0 (healthy) or 1 (alert)."""
    _log("start", f"{datetime.now(timezone.utc).isoformat()}  batch={current.shape}")

    # 1) Great Expectations — cheap, fail fast on a structural break
    _log("ge", "running expectation suite ...")
    ge_ok, ge = run_ge_validation(current)
    _log("ge", f"success={ge_ok}  passed={ge['passed']}/{ge['evaluated']}")
    if not ge_ok:
        payload = build_alert_payload(
            "ge_validation", route_severity("ge_validation"),
            f"Great Expectations validation failed: {ge['failed']}/{ge['evaluated']} expectations broke",
            {"ge": ge})
        mid = publish_alert(payload, dry_run)
        _log("publish", f"ge_validation alert  MessageId={mid}")
        return 1

    # 2) Evidently drift — only meaningful once the data is structurally sound
    _log("drift", "computing reference vs current ...")
    no_drift, dr = run_drift_detection(reference, current)
    _log("drift", f"dataset_drift={not no_drift}  drift_share={dr['drift_share']:.2%}")
    if not no_drift:
        sev = route_severity("drift", dr["drift_share"])
        payload = build_alert_payload(
            "drift", sev,
            f"Data drift detected: {dr['number_of_drifted_columns']}/{dr['number_of_columns']} "
            f"columns drifted ({dr['drift_share']:.0%} of features)",
            {"drift": dr})
        mid = publish_alert(payload, dry_run)
        _log("publish", f"drift alert ({sev})  MessageId={mid}")
        return 1

    _log("done", "all checks passed — no alert published")
    return 0


def main(argv) -> int:
    p = argparse.ArgumentParser(description="Truck Delay model monitoring pipeline")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--current", metavar="FILE", help="production batch (.csv or .parquet)")
    src.add_argument("--simulate", action="store_true", help="synthetic drifted batch (monsoon)")
    src.add_argument("--simulate-corrupt", action="store_true", help="synthetic schema-corrupted batch")
    p.add_argument("--dry-run", action="store_true", help="print payloads instead of publishing to SNS")
    args = p.parse_args(argv)

    if not config.TOPIC_ARN and not args.dry_run:
        print("WARNING: TOPIC_ARN is not set — alerts will print instead of publishing. "
              "Set TOPIC_ARN (Lab 1) or pass --dry-run to silence this.")

    # Reference is the REAL M3 distribution and is required (no synthetic fallback).
    reference = load_reference()
    _log("reference", f"loaded {reference.shape} from {config.REFERENCE_CSV}")

    if args.simulate:
        current = simulate_drifted_batch(reference)
    elif args.simulate_corrupt:
        current = simulate_corrupt_batch(reference)
    else:
        current = load_batch_file(args.current)
        _log("input", f"loaded {current.shape} from {args.current}")

    return run_monitoring(current, reference, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
