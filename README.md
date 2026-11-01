# mission-deh-hof-lyft-pipeline-codebase

This repository contains the infrastructure and scripts for the **DEH HOF Lyft NYCTLC Data Pipeline** — an end-to-end data engineering pipeline that ingests, transforms, and aggregates NYC Taxi & Limousine Commission (NYCTLC) ride data on AWS.

---

## Overview

The pipeline is fully serverless and event-driven. When a new file lands in S3, a Lambda function automatically triggers a Step Functions state machine that runs three Glue jobs in sequence — ingestion, raw-to-curated transformation, and curated-to-aggregated aggregation. Alerts are sent via SNS on success or failure.

---

## Architecture

```
S3 File Arrival
      ↓
Lambda (pipeline-trigger)
      ↓
Step Functions State Machine
      ↓
Glue Job 1: NYCTLC Ingestion     (Python Shell)
      ↓
Glue Job 2: Raw → Curated        (Spark ETL)
      ↓
Glue Job 3: Curated → Aggregated (Spark ETL)
      ↓
SNS Email Alert (✅ Success / ❌ Failure)
```

---

## Repository Structure

```
├── cloudformation/
│   └── lyft-pipeline.yaml          # CloudFormation template — provisions all AWS resources
├── glue-scripts/
│   ├── backup-nyctlc-ingestion.py          # Glue Job 1: ingests raw NYCTLC data into S3
│   ├── backup-raw-to-curated.py            # Glue Job 2: cleans and transforms raw data
│   └── backup-curated-to-aggregated.py     # Glue Job 3: aggregates curated data
├── lambda-scripts/
│   └── pipeline-trigger/
│       └── lambda_function.py      # Lambda that starts the Step Functions execution
├── step-functions/
│   └── pipeline-state-machine.asl.json  # State machine definition (ASL)
├── dags/
│   └── sample_dag.py               # Sample Airflow DAG for testing
└── README.md
```

---

## AWS Services Used

| Service | Purpose |
|---|---|
| AWS Glue | Data ingestion and ETL processing |
| AWS Step Functions | Orchestrates the pipeline jobs in sequence |
| AWS Lambda | Event-driven trigger on S3 file arrival |
| Amazon S3 | Stores raw, curated, and aggregated data + Glue scripts |
| Amazon SNS | Sends email alerts on pipeline success or failure |
| AWS CloudFormation | Infrastructure as Code — provisions all resources |

---

## Deployment

### Prerequisites
- IAM roles for Glue, Lambda, and Step Functions must be pre-provisioned by the Cloud Infra team
- S3 bucket `mission-deh-hof-nyctlc-<account-id>` must exist
- Upload Glue scripts to S3 under the `scripts/` prefix before deploying

### Deploy the CloudFormation stack
```bash
aws cloudformation deploy \
  --template-file cloudformation/lyft-pipeline.yaml \
  --stack-name mission-deh-hof-lyft-pipeline \
  --capabilities CAPABILITY_NAMED_IAM
```

### Post-deployment
After the stack deploys, configure an S3 event notification on the data bucket to trigger the `mission-deh-hof-pipeline-trigger` Lambda. Scope it to your landing prefix (e.g. `landing/`) to avoid re-trigger loops.

---

## Branch Strategy

| Branch | Purpose |
|---|---|
| `master` | Stable / production-ready code |
| `develop` | Ongoing development |
| `lyft-pipeline-ingestion` | Feature branch for this pipeline |

---

## Alerts

Pipeline alerts are sent to `connect@dataengineeringhub.in` via SNS. The email subscription must be confirmed via the link sent to that address after the stack is first deployed.
