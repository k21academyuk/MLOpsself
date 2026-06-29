# Course Completion — Portfolio Packaging & Final Teardown

You finished the AWS MLOps Master Course. This guide does two things: **(A)** assemble everything into a portfolio you can
show in interviews, and **(B)** tear down **all** AWS resources so nothing keeps billing.

---

## A · Assemble your portfolio

You built **five** projects. Bundle them into one GitHub repo (`aws-mlops-portfolio/`) with a top-level README that tells
the story.

### A.1 The five projects
| # | Project | Modules | Headline skill |
|---|---|---|---|
| 1 | **Truck Delay Classification (spine)** | M3–M8 | end-to-end: features → deploy → monitor → govern → automate |
| 2 | **SBERT Search Relevancy** | M4 branch | NLP serving, Docker, semantic search |
| 3 | **Customer Churn — Banking** | M5 branch | ECS + ALB + **Terraform** + CodePipeline |
| 4 | **Loan/Churn Monitoring (Airflow)** | M6 branch | Airflow + Docker drift monitoring |
| 5 | **(M2) Pune Real-Estate** | M2 | the "before cloud" baseline (notebooks → scripts → FastAPI) |

### A.2 Spine repo layout
```
aws-mlops-portfolio/
├── README.md                     ← the story (architecture diagram + what each module added)
├── truck-delay-spine/
│   ├── m3-features-model/        EDA + feature engineering + training (notebooks) + final_features.csv provenance
│   ├── m4-docker-ecr/            Dockerfile + ECR push
│   ├── m5-ecs-cicd/              task def, ALB, GitHub Actions / CodePipeline (truck-delay-deploy)
│   ├── m6-monitoring/            Evidently/GE notebooks + run_monitoring.py (production script)
│   ├── m7-feature-store-registry/ Hopsworks + MLflow + W&B + SHAP notebooks
│   └── m8-pipeline/              processing/training/evaluation/pipeline.py + Lambda + EventBridge
└── branches/
    ├── sbert-search/  churn-ecs-terraform/  airflow-monitoring/
```

### A.3 The README story (what reviewers read first)
Lead with the **one-paragraph arc** and a diagram:
> *A delay-prediction model for a grocery logistics company, taken from notebook to a self-healing production system:
> trained and tracked (M3/M7), containerised (M4), deployed to ECS with CI/CD (M5), monitored for drift with alerting
> (M6), governed through a feature store + model registry with SHAP explanations (M7), and fully automated with a
> SageMaker Pipeline triggered by EventBridge (M8) — all in one AWS account, ap-south-1.*

Then **trace one shipment** through all 8 modules (the capstone write-up). That single trace is the most convincing thing
in the repo.

### A.4 Résumé bullets you've earned
- "Built an end-to-end MLOps system on AWS: SageMaker Pipelines, ECS/ALB, ECR, Lambda, EventBridge, SNS, S3."
- "Implemented drift + data-quality monitoring (Evidently, Great Expectations) with SNS alerting."
- "Governed models with a feature store (Hopsworks) and MLflow Model Registry; explained predictions with SHAP."
- "Infrastructure as code with **AWS CDK** (Python) and **Terraform**; CI/CD via GitHub Actions and CodePipeline."

> **Verify the reference frame is reproducible:** keep `Module 3/labs/regenerate_final_features.py` in the repo — it
> rebuilds `final_features.csv` from the raw CSVs and round-trips through the model (acc 0.93). It proves your data
> lineage to a reviewer.

---

## B · Final teardown — leave a clean account

The cost incidents in this course came from **forgetting to tear down**. Do this in order. Region: **ap-south-1**
(check us-east-1 too if you ever deployed there).

### B.1 Stop the automation first (so nothing re-triggers mid-teardown)
```bash
# Disable/delete the EventBridge schedules
aws scheduler delete-schedule --name truck-delay-weekly-retrain --region ap-south-1 2>/dev/null || true
# (delete any second data-landing schedule too)
```

### B.2 `cdk destroy` every module's instructor_setup
```bash
for M in "Module 8" "Module 7" "Module 6" "Module 5" "Module 4" "Module 3"; do
  if [ -d "$M/instructor_setup" ]; then
    echo "== destroying $M =="; ( cd "$M/instructor_setup" && cdk destroy --force )
  fi
done
```
This removes: M8 SNS topic + S3 artifact bucket; M7 MLflow EC2; M6 SageMaker notebook + lifecycle config; M4 ECR (if you
deployed it); M3 EC2/RDS/S3/VPC. (M5 was always hands-on — use the M5 `m5_teardown_all.sh` for its ECS/ALB/CodePipeline.)

### B.3 SageMaker leftovers (pipelines, models, endpoints)
```bash
R=ap-south-1
# Delete the pipeline
aws sagemaker delete-pipeline --pipeline-name TruckDelayClassification --region $R 2>/dev/null || true
# Delete any deployed endpoints (endpoints bill per-hour!)
aws sagemaker list-endpoints --region $R --query "Endpoints[].EndpointName" --output text
# for each: aws sagemaker delete-endpoint --endpoint-name <name> --region $R
# Model packages / groups (free, optional cleanup)
aws sagemaker delete-model-package-group --model-package-group-name TruckDelayModelPackageGroup --region $R 2>/dev/null || true
```

### B.4 Lambda
```bash
aws lambda delete-function --function-name truck-delay-land-streaming --region ap-south-1 2>/dev/null || true
```

### B.5 The SageMaker notebook (if not removed by B.2)
```bash
aws sagemaker stop-notebook-instance --notebook-instance-name m6-truck-delay-monitoring --region ap-south-1 2>/dev/null || true
# wait for Stopped, then:
aws sagemaker delete-notebook-instance --notebook-instance-name m6-truck-delay-monitoring --region ap-south-1 2>/dev/null || true
```

### B.6 Buckets (empty before delete)
```bash
# Pipeline artifact bucket is auto-deleted by CDK (auto_delete_objects). The SageMaker DEFAULT bucket lingers:
aws s3 ls | grep sagemaker-$R   # sagemaker-ap-south-1-<account>
# aws s3 rb s3://sagemaker-ap-south-1-<account> --force   # only if you don't reuse it
```

### B.7 Read-only verification sweep (confirm nothing's left billing)
```bash
R=ap-south-1
echo "Notebooks:"; aws sagemaker list-notebook-instances --region $R --query "NotebookInstances[].{N:NotebookInstanceName,S:NotebookInstanceStatus}"
echo "Endpoints (MUST be empty):"; aws sagemaker list-endpoints --region $R --query "Endpoints[].EndpointName"
echo "RDS:"; aws rds describe-db-instances --region $R --query "DBInstances[].DBInstanceIdentifier"
echo "EC2 (course):"; aws ec2 describe-instances --region $R --filters "Name=instance-state-name,Values=running" --query "Reservations[].Instances[].Tags[?Key=='Name'].Value"
echo "Load balancers:"; aws elbv2 describe-load-balancers --region $R --query "LoadBalancers[].LoadBalancerName"
echo "CFN stacks:"; aws cloudformation list-stacks --region $R --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE --query "StackSummaries[?starts_with(StackName,'m')].StackName"
```
Everything course-related should be empty/gone. **The two cost-drivers to be paranoid about: SageMaker *endpoints* and
*running notebook instances/EC2/RDS*.** If any remain, delete them.

### B.8 Keep (intentionally)
- The **GitHub repos** (your portfolio).
- The local `labs/data/` real artifacts + `regenerate_final_features.py` (reproducibility).
- Free-tier accounts (Hopsworks, W&B) — nothing to bill.

---

## You're done 🎓
You took one model from a notebook to a self-healing, monitored, governed, automated production system on AWS — and you can
defend every architectural choice. That's the job. Go get it.
