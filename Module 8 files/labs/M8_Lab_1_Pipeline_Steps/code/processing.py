"""
processing.py — SageMaker Pipeline **Processing step**.

Runs inside a SageMaker-managed SKLearn Processing container. Reads the real
Truck Delay feature frame (final_features.csv) from S3, fits the SAME M3
preprocessing (one-hot encode the 6 categoricals + scale the 27 continuous +
pass through the 3 binary/ordinal), splits train/validation/test, and writes
the arrays back to S3 for the Training + Evaluation steps.

SageMaker mounts inputs/outputs at /opt/ml/processing/{input,train,validation,test}.
Functional style — no classes.
"""
import argparse
import json
import os

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler

BASE = "/opt/ml/processing"

CONTINUOUS = [
    "route_avg_temp", "route_avg_wind_speed", "route_avg_precip", "route_avg_humidity",
    "route_avg_visibility", "route_avg_pressure", "origin_avg_temp", "origin_avg_wind_speed",
    "origin_avg_precip", "origin_avg_humidity", "origin_avg_visibility", "origin_avg_pressure",
    "dest_avg_temp", "dest_avg_wind_speed", "dest_avg_precip", "dest_avg_humidity",
    "dest_avg_visibility", "dest_avg_pressure", "truck_age", "load_capacity_pounds",
    "mileage_mpg", "age", "experience", "average_speed_mph", "avg_no_of_vehicles",
    "distance", "average_hours",
]
CATEGORICAL = ["route_description", "origin_description", "dest_description",
               "fuel_type", "gender", "driving_style"]
BINARY = ["accident", "ratings", "is_midnight"]
TARGET = "delay"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input-file", default=os.path.join(BASE, "input", "final_features.csv"))
    args = p.parse_args()

    print(f"[processing] reading {args.input_file}")
    df = pd.read_csv(args.input_file)
    y = df[TARGET].astype(int).values

    # Fit-fresh preprocessing (a retraining pipeline re-fits — that's the point)
    pre = ColumnTransformer([
        ("cont", StandardScaler(), CONTINUOUS),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL),
        ("bin", "passthrough", BINARY),
    ])
    X = pre.fit_transform(df)
    print(f"[processing] feature matrix: {X.shape}")

    # 70 / 15 / 15 split, stratified
    X_tr, X_tmp, y_tr, y_tmp = train_test_split(X, y, test_size=0.30, random_state=42, stratify=y)
    X_val, X_te, y_val, y_te = train_test_split(X_tmp, y_tmp, test_size=0.50, random_state=42, stratify=y_tmp)

    # XGBoost's SageMaker container expects label in the FIRST column, no header
    def _dump(split, Xs, ys):
        out = os.path.join(BASE, split)
        os.makedirs(out, exist_ok=True)
        arr = np.concatenate([ys.reshape(-1, 1), Xs], axis=1)
        pd.DataFrame(arr).to_csv(os.path.join(out, f"{split}.csv"), header=False, index=False)
        print(f"[processing] wrote {split}: {arr.shape}")

    _dump("train", X_tr, y_tr)
    _dump("validation", X_val, y_val)
    _dump("test", X_te, y_te)

    # Persist feature count for downstream sanity
    with open(os.path.join(BASE, "train", "meta.json"), "w") as f:
        json.dump({"n_features": int(X.shape[1]), "train_rows": int(X_tr.shape[0])}, f)


if __name__ == "__main__":
    main()
