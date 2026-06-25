"""
data_sources.py — load the REAL reference frame and produce a "current" batch.

Three ways to get a current batch, behind one return type (a DataFrame):
  - load_batch_file(path)     : a real production dump (CSV or Parquet)
  - simulate_drifted_batch()  : monsoon drift (same logic as Lab 2/4) — for testing
  - simulate_corrupt_batch()  : schema corruption (same logic as Lab 3/4) — for testing

The reference frame and feature metadata are the genuine M3 Lab B outputs. They
are REQUIRED — if absent we raise a clear error rather than inventing data.
"""
import json
import os

import numpy as np
import pandas as pd

import config


def load_feature_meta() -> dict:
    if not os.path.exists(config.FEATURE_META):
        raise FileNotFoundError(
            f"feature_metadata.json not found at {config.FEATURE_META}. "
            "This is the real M3 Lab B output that ships in Module 6/labs/data/. "
            "Do not substitute synthetic metadata — restore the file or set M6_DATA_DIR."
        )
    with open(config.FEATURE_META) as f:
        return json.load(f)


def load_reference() -> pd.DataFrame:
    if not os.path.exists(config.REFERENCE_CSV):
        raise FileNotFoundError(
            f"Reference frame not found at {config.REFERENCE_CSV}.\n"
            "This is the REAL M3 training distribution (final_features.csv, 12,308 x 37).\n"
            "Regenerate it with Module 3/labs/regenerate_final_features.py, or set M6_DATA_DIR.\n"
            "The pipeline will NOT fabricate a reference distribution."
        )
    return pd.read_csv(config.REFERENCE_CSV)


def load_batch_file(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)


def simulate_drifted_batch(reference: pd.DataFrame, n: int = 2000, seed: int = 42) -> pd.DataFrame:
    """Monsoon drift: precip/humidity up, fleet aging, more heavy-rain, more delays. (Lab 2 logic.)"""
    meta = load_feature_meta()
    target = meta["target"]
    rng = np.random.default_rng(seed)
    cur = reference.sample(n=n, random_state=seed).reset_index(drop=True).copy()
    for p in ("route", "origin", "dest"):
        cur[f"{p}_avg_precip"] = cur[f"{p}_avg_precip"] * rng.uniform(1.3, 1.8, n)
        cur[f"{p}_avg_humidity"] = (cur[f"{p}_avg_humidity"] + rng.normal(8, 2, n)).clip(0, 100)
    cur["truck_age"] = (cur["truck_age"] + rng.integers(0, 4, n)).clip(1, 30)
    heavy = rng.random(n) < 0.30
    cur.loc[heavy, "route_description"] = rng.choice(
        ["Heavy rain", "Moderate rain", "Patchy rain possible"], size=heavy.sum())
    flip = (cur[target] == 0) & (rng.random(n) < 0.18)
    cur.loc[flip, target] = 1
    return cur


def simulate_corrupt_batch(reference: pd.DataFrame, n: int = 500, seed: int = 7) -> pd.DataFrame:
    """Schema corruption GE should catch: negative age, null ratings, unknown fuel, bad target. (Lab 3 logic.)"""
    meta = load_feature_meta()
    target = meta["target"]
    b = reference.sample(n=n, random_state=seed).reset_index(drop=True).copy()
    b.loc[b.index[:25], "truck_age"] = -3
    b.loc[b.sample(frac=0.05, random_state=1).index, "ratings"] = np.nan
    b.loc[b.index[300:310], "fuel_type"] = "hydrogen"
    b.loc[b.index[400:402], target] = 2
    return b
