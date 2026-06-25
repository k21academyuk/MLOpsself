"""
checks.py — the two monitors, as pure functions.

  run_ge_validation(df)            -> (success: bool, details: dict)   # Great Expectations (Lab 3 suite)
  run_drift_detection(ref, cur)    -> (no_drift: bool, details: dict)  # Evidently (Lab 2 logic)

Heavy libraries (great_expectations, evidently) are imported lazily so that
`--help` and config errors don't pay the import cost.
"""
from datetime import datetime

import pandas as pd

import config
from data_sources import load_feature_meta


def run_ge_validation(df: pd.DataFrame, suite_name: str = None) -> tuple:
    """Validate a batch against the Lab 3 expectation suite. Returns (success, details)."""
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest

    suite_name = suite_name or config.GE_SUITE_NAME
    context = gx.get_context(context_root_dir=config.GE_PROJECT_DIR)

    if "truck_delay_runtime" not in [d["name"] for d in context.list_datasources()]:
        context.add_datasource(
            name="truck_delay_runtime", class_name="Datasource",
            execution_engine={"class_name": "PandasExecutionEngine"},
            data_connectors={"default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"]}})

    batch_request = RuntimeBatchRequest(
        datasource_name="truck_delay_runtime",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="production_batch",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": datetime.utcnow().isoformat()})

    results = context.get_validator(
        batch_request=batch_request, expectation_suite_name=suite_name).validate()

    details = {
        "evaluated": results["statistics"]["evaluated_expectations"],
        "passed": results["statistics"]["successful_expectations"],
        "failed": results["statistics"]["unsuccessful_expectations"],
        "success_percent": round(results["statistics"]["success_percent"], 1),
        "failures": [
            {"expectation": r["expectation_config"]["expectation_type"],
             "column": r["expectation_config"]["kwargs"].get("column"),
             "unexpected_count": r["result"].get("unexpected_count", 0),
             "sample": r["result"].get("partial_unexpected_list", [])[:3]}
            for r in results["results"] if not r["success"]],
    }
    return bool(results["success"]), details


def run_drift_detection(reference: pd.DataFrame, current: pd.DataFrame) -> tuple:
    """Evidently data + target drift. Returns (no_drift, details). no_drift=True means fine."""
    from evidently import ColumnMapping
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset, TargetDriftPreset

    meta = load_feature_meta()
    cm = ColumnMapping(
        target=meta["target"], prediction=None,
        numerical_features=meta["continuous_features"],
        categorical_features=meta["categorical_features"])

    report = Report(metrics=[DataDriftPreset(), TargetDriftPreset()])
    report.run(reference_data=reference, current_data=current, column_mapping=cm)
    r = report.as_dict()["metrics"][0]["result"]

    details = {
        "dataset_drift": bool(r["dataset_drift"]),
        "drift_share": float(r["drift_share"]),
        "number_of_drifted_columns": r["number_of_drifted_columns"],
        "number_of_columns": r["number_of_columns"],
        "drifted_columns": [c for c, cr in r["drift_by_columns"].items() if cr["drift_detected"]][:10],
    }
    return (not details["dataset_drift"]), details
