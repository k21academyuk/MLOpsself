"""
training.py — SageMaker Pipeline **Training step** (XGBoost, script mode).

Runs inside SageMaker's XGBoost framework container. SageMaker mounts the
Processing step's outputs as channels (SM_CHANNEL_TRAIN / SM_CHANNEL_VALIDATION)
and tars whatever we write to SM_MODEL_DIR into model.tar.gz — which the
Evaluation step then loads.

Hyperparameters arrive as CLI args (set from pipeline.py). Functional — no classes.
"""
import argparse
import os

import pandas as pd
import xgboost as xgb


def _load(channel_dir, name):
    arr = pd.read_csv(os.path.join(channel_dir, f"{name}.csv"), header=None).values
    return arr[:, 1:], arr[:, 0]          # X, y  (label is column 0)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--max-depth", type=int, default=5)
    p.add_argument("--eta", type=float, default=0.2)            # learning rate
    p.add_argument("--num-round", type=int, default=200)
    p.add_argument("--subsample", type=float, default=0.9)
    p.add_argument("--scale-pos-weight", type=float, default=1.8)   # delay rate ~0.35 → up-weight positives
    p.add_argument("--train", default=os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train"))
    p.add_argument("--validation", default=os.environ.get("SM_CHANNEL_VALIDATION", "/opt/ml/input/data/validation"))
    p.add_argument("--model-dir", default=os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
    args = p.parse_args()

    X_tr, y_tr = _load(args.train, "train")
    X_val, y_val = _load(args.validation, "validation")
    print(f"[training] train={X_tr.shape}  validation={X_val.shape}")

    dtrain = xgb.DMatrix(X_tr, label=y_tr)
    dval = xgb.DMatrix(X_val, label=y_val)

    params = {
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "auc"],
        "max_depth": args.max_depth,
        "eta": args.eta,
        "subsample": args.subsample,
        "scale_pos_weight": args.scale_pos_weight,
    }
    booster = xgb.train(
        params, dtrain, num_boost_round=args.num_round,
        evals=[(dtrain, "train"), (dval, "validation")],
        early_stopping_rounds=20, verbose_eval=25)

    os.makedirs(args.model_dir, exist_ok=True)
    booster.save_model(os.path.join(args.model_dir, "xgboost-model"))
    print(f"[training] saved model -> {args.model_dir}/xgboost-model")


if __name__ == "__main__":
    main()
