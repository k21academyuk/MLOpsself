"""
config.py — central, env-driven configuration for the Truck Delay monitoring pipeline.

Every value can be overridden by an environment variable so the SAME code runs
unchanged on a laptop (cron), an Airflow worker, or a SageMaker Processing job.
No secrets are hard-coded.
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))

# --- Data + artifacts (the REAL M3 outputs that ship with Module 6) -----------
# These are required. The pipeline does NOT fabricate a stand-in reference frame
# or model — if they're missing it errors out and points you at the materials.
DATA_DIR       = os.environ.get("M6_DATA_DIR", os.path.normpath(os.path.join(HERE, "..", "data")))
REFERENCE_CSV  = os.path.join(DATA_DIR, "reference", "final_features.csv")
FEATURE_META   = os.path.join(DATA_DIR, "reference", "feature_metadata.json")
ARTIFACTS_DIR  = os.path.join(DATA_DIR, "artifacts")          # xgboost_model/encoder/scaler/model_metadata

# --- Great Expectations (the suite built in Lab 3) ----------------------------
GE_PROJECT_DIR = os.environ.get("GE_PROJECT_DIR", os.path.normpath(os.path.join(HERE, "..", "great_expectations")))
GE_SUITE_NAME  = os.environ.get("GE_SUITE_NAME", "truck_delay_features")

# --- SNS alerting (the topic from Lab 1) --------------------------------------
TOPIC_ARN   = os.environ.get("TOPIC_ARN", "")                 # arn:aws:sns:<region>:<acct>:truck-delay-alerts
AWS_REGION  = os.environ.get("AWS_REGION", "ap-south-1")

# --- Identity carried in every alert ------------------------------------------
SERVICE_NAME = "truck-delay-classifier"
ENVIRONMENT  = os.environ.get("ENVIRONMENT", "production")

# --- Drift severity thresholds (fraction of features drifted) -----------------
SEVERITY_CRITICAL_SHARE = float(os.environ.get("SEVERITY_CRITICAL_SHARE", "0.50"))
SEVERITY_WARNING_SHARE  = float(os.environ.get("SEVERITY_WARNING_SHARE", "0.30"))
