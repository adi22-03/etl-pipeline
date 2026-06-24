#!/bin/bash
# run_on_emr.sh
# Submits the ETL job as an EMR Step (Spark step).
# Prerequisites:
#   - AWS CLI installed and configured (aws configure)
#   - etl_job.py already uploaded to S3 (see SCRIPT_S3 below)
#   - An existing EMR cluster (or use --auto-terminate to create one)

set -euo pipefail

# ── CONFIG — edit these ────────────────────────────────────────────────────
CLUSTER_ID="j-XXXXXXXXXX"           # your EMR cluster ID, or leave blank to create one
BUCKET="your-analytics-bucket"
SCRIPT_S3="s3://${BUCKET}/scripts/etl_job.py"
INPUT_S3="s3://${BUCKET}/raw/year=2024/month=01/day=15/*.json"
OUTPUT_S3="s3://${BUCKET}/processed/fact_user_daily"
REGION="us-east-1"
# ──────────────────────────────────────────────────────────────────────────

echo "Uploading etl_job.py to S3..."
aws s3 cp etl_job.py "${SCRIPT_S3}" --region "${REGION}"

echo "Adding Spark step to cluster ${CLUSTER_ID}..."
STEP_ID=$(aws emr add-steps \
  --cluster-id "${CLUSTER_ID}" \
  --region "${REGION}" \
  --steps "[
    {
      \"Name\": \"fact_user_daily_etl\",
      \"ActionOnFailure\": \"CONTINUE\",
      \"HadoopJarStep\": {
        \"Jar\": \"command-runner.jar\",
        \"Args\": [
          \"spark-submit\",
          \"--deploy-mode\", \"cluster\",
          \"--conf\", \"spark.sql.shuffle.partitions=200\",
          \"--conf\", \"spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\",
          \"${SCRIPT_S3}\",
          \"--input\",  \"${INPUT_S3}\",
          \"--output\", \"${OUTPUT_S3}\"
        ]
      }
    }
  ]" \
  --query 'StepIds[0]' \
  --output text)

echo "Step submitted: ${STEP_ID}"
echo "Monitor at: https://${REGION}.console.aws.amazon.com/emr/home?region=${REGION}#/clusters/${CLUSTER_ID}/steps"

# Poll until the step completes
echo "Waiting for step to finish..."
aws emr wait step-complete \
  --cluster-id "${CLUSTER_ID}" \
  --step-id "${STEP_ID}" \
  --region "${REGION}"

STATE=$(aws emr describe-step \
  --cluster-id "${CLUSTER_ID}" \
  --step-id "${STEP_ID}" \
  --region "${REGION}" \
  --query 'Step.Status.State' \
  --output text)

echo "Step finished with state: ${STATE}"
