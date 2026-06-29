"""
evaluation.py — SageMaker Pipeline **Evaluation step**.

Loads the model the Training step produced (a SageMaker XGBoost `model.tar.gz`
at /opt/ml/processing/model) and the held-out test split, computes metrics, and
writes evaluation.json. The pipeline's **Condition step** reads `f1` from this
file to decide whether to register the model.

Functional — no classes.
"""
import json
import os
import tarfile

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

BASE = "/opt/ml/processing"


def main():
    # Unpack the model.tar.gz the Training step wrote
    model_dir = os.path.join(BASE, "model")
    with tarfile.open(os.path.join(model_dir, "model.tar.gz")) as tar:
        tar.extractall(path=model_dir)
    booster = xgb.Booster()
    booster.load_model(os.path.join(model_dir, "xgboost-model"))

    # Test split: label is column 0, no header
    test = pd.read_csv(os.path.join(BASE, "test", "test.csv"), header=None).values
    y_test = test[:, 0].astype(int)
    X_test = test[:, 1:]

    proba = booster.predict(xgb.DMatrix(X_test))
    pred = (proba >= 0.5).astype(int)

    report = {
        "binary_classification_metrics": {
            "accuracy":  {"value": float(accuracy_score(y_test, pred))},
            "precision": {"value": float(precision_score(y_test, pred, zero_division=0))},
            "recall":    {"value": float(recall_score(y_test, pred, zero_division=0))},
            "f1":        {"value": float(f1_score(y_test, pred, zero_division=0))},
            "roc_auc":   {"value": float(roc_auc_score(y_test, proba))},
        }
    }
    # Flat key too, so the Condition step's JsonGet can read `f1` directly
    report["f1"] = report["binary_classification_metrics"]["f1"]["value"]

    out = os.path.join(BASE, "evaluation")
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "evaluation.json"), "w") as f:
        json.dump(report, f, indent=2)
    print("[evaluation]", json.dumps(report["binary_classification_metrics"], indent=2))


if __name__ == "__main__":
    main()
