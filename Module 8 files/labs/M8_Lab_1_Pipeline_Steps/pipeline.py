"""
pipeline.py — the Truck Delay SageMaker Pipeline (capstone).

Composes the canonical 6-step flow on the REAL Truck Delay data:

    Processing  ─▶  Training  ─▶  Evaluation  ─▶  Condition(f1 ≥ threshold?)
                                                     ├─ yes ─▶ RegisterModel (Model Registry)
                                                     └─ no  ─▶ Fail  (a notification Lambda/SNS can hang off this)

`get_pipeline(...)` returns a `Pipeline` object. The build-and-run notebook
calls `pipeline.upsert()` then `pipeline.start()`. Region: ap-south-1. Role +
bucket come from the SageMaker notebook (no hard-coded ARNs). Functional style.
"""
import os

import boto3
import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.parameters import ParameterString, ParameterFloat, ParameterInteger
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.fail_step import FailStep
from sagemaker.workflow.functions import JsonGet, Join
from sagemaker.workflow.model_step import ModelStep
from sagemaker.processing import ProcessingInput, ProcessingOutput, ScriptProcessor
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.xgboost.estimator import XGBoost
from sagemaker.inputs import TrainingInput
from sagemaker.model import Model
from sagemaker.model_metrics import ModelMetrics, MetricsSource

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
CODE_DIR = os.path.join(BASE_DIR, "code")
XGB_VERSION = "1.7-1"


def get_pipeline(
    region="ap-south-1",
    role=None,
    default_bucket=None,
    input_data_uri=None,                 # s3://.../final_features.csv (uploaded by the notebook)
    pipeline_name="TruckDelayClassification",
    model_package_group_name="TruckDelayModelPackageGroup",
    processing_instance_type="ml.m5.large",
    training_instance_type="ml.m5.large",
):
    boto_session = boto3.Session(region_name=region)
    sm_client = boto_session.client("sagemaker")
    pipeline_session = PipelineSession(boto_session=boto_session, sagemaker_client=sm_client,
                                       default_bucket=default_bucket)
    role = role or sagemaker.session.get_execution_role(
        sagemaker.session.Session(boto_session, sm_client))

    # ── Parameters (overridable at start() time) ────────────────────────────────
    p_input = ParameterString(name="InputData", default_value=input_data_uri)
    p_approval = ParameterString(name="ModelApprovalStatus", default_value="PendingManualApproval")
    p_f1_threshold = ParameterFloat(name="F1Threshold", default_value=0.55)
    p_proc_count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)

    xgb_image = sagemaker.image_uris.retrieve("xgboost", region, version=XGB_VERSION)

    # ── 1. Processing: encode/scale/split ───────────────────────────────────────
    sklearn_proc = SKLearnProcessor(framework_version="1.2-1", role=role,
                                    instance_type=processing_instance_type,
                                    instance_count=p_proc_count,
                                    base_job_name="truck-delay-process",
                                    sagemaker_session=pipeline_session)
    proc_args = sklearn_proc.run(
        inputs=[ProcessingInput(source=p_input, destination="/opt/ml/processing/input")],
        outputs=[
            ProcessingOutput(output_name="train", source="/opt/ml/processing/train"),
            ProcessingOutput(output_name="validation", source="/opt/ml/processing/validation"),
            ProcessingOutput(output_name="test", source="/opt/ml/processing/test"),
        ],
        code=os.path.join(CODE_DIR, "processing.py"),
    )
    step_process = ProcessingStep(name="ProcessTruckDelayData", step_args=proc_args)

    # ── 2. Training: XGBoost (script mode) ──────────────────────────────────────
    xgb_estimator = XGBoost(
        entry_point="training.py", source_dir=CODE_DIR,
        framework_version=XGB_VERSION, instance_type=training_instance_type,
        instance_count=1, role=role, base_job_name="truck-delay-train",
        sagemaker_session=pipeline_session,
        hyperparameters={"max-depth": 5, "eta": 0.2, "num-round": 200,
                         "subsample": 0.9, "scale-pos-weight": 1.8},
    )
    train_args = xgb_estimator.fit({
        "train": TrainingInput(
            s3_data=step_process.properties.ProcessingOutputConfig.Outputs["train"].S3Output.S3Uri,
            content_type="text/csv"),
        "validation": TrainingInput(
            s3_data=step_process.properties.ProcessingOutputConfig.Outputs["validation"].S3Output.S3Uri,
            content_type="text/csv"),
    })
    step_train = TrainingStep(name="TrainTruckDelayModel", step_args=train_args)

    # ── 3. Evaluation: f1 on the held-out test split ────────────────────────────
    eval_proc = ScriptProcessor(image_uri=xgb_image, command=["python3"], role=role,
                                instance_type=processing_instance_type, instance_count=1,
                                base_job_name="truck-delay-eval",
                                sagemaker_session=pipeline_session)
    eval_report = PropertyFile(name="EvaluationReport", output_name="evaluation", path="evaluation.json")
    eval_args = eval_proc.run(
        inputs=[
            ProcessingInput(source=step_train.properties.ModelArtifacts.S3ModelArtifacts,
                            destination="/opt/ml/processing/model"),
            ProcessingInput(
                source=step_process.properties.ProcessingOutputConfig.Outputs["test"].S3Output.S3Uri,
                destination="/opt/ml/processing/test"),
        ],
        outputs=[ProcessingOutput(output_name="evaluation", source="/opt/ml/processing/evaluation")],
        code=os.path.join(CODE_DIR, "evaluation.py"),
    )
    step_eval = ProcessingStep(name="EvaluateTruckDelayModel", step_args=eval_args,
                               property_files=[eval_report])

    # ── 4. Register (only if f1 ≥ threshold) ────────────────────────────────────
    model_metrics = ModelMetrics(model_statistics=MetricsSource(
        s3_uri=Join(on="/", values=[
            step_eval.properties.ProcessingOutputConfig.Outputs["evaluation"].S3Output.S3Uri,
            "evaluation.json"]),
        content_type="application/json"))

    model = Model(image_uri=xgb_image,
                  model_data=step_train.properties.ModelArtifacts.S3ModelArtifacts,
                  sagemaker_session=pipeline_session, role=role)
    register_args = model.register(
        content_types=["text/csv"], response_types=["text/csv"],
        inference_instances=["ml.t2.medium", "ml.m5.large"],
        transform_instances=["ml.m5.large"],
        model_package_group_name=model_package_group_name,
        approval_status=p_approval, model_metrics=model_metrics)
    step_register = ModelStep(name="RegisterTruckDelayModel", step_args=register_args)

    step_fail = FailStep(name="ModelBelowThreshold",
                         error_message=Join(on=" ", values=["F1", "below", p_f1_threshold]))

    # ── 5. Condition: gate registration on f1 ───────────────────────────────────
    cond = ConditionGreaterThanOrEqualTo(
        left=JsonGet(step_name=step_eval.name, property_file=eval_report, json_path="f1"),
        right=p_f1_threshold)
    step_condition = ConditionStep(name="CheckF1Threshold", conditions=[cond],
                                   if_steps=[step_register], else_steps=[step_fail])

    return Pipeline(
        name=pipeline_name,
        parameters=[p_input, p_approval, p_f1_threshold, p_proc_count],
        steps=[step_process, step_train, step_eval, step_condition],
        sagemaker_session=pipeline_session,
    )
