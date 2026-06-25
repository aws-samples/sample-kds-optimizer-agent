# Kinesis Data Streams Mode Optimizer Agent

An AI-powered agent running on Amazon Bedrock that analyzes your Kinesis Data Streams usage and recommends the optimal capacity mode: **On-demand Standard**, **On-demand Advantage**, or **Provisioned**.

The agent strongly prefers on-demand modes for their operational simplicity (automatic scaling, no capacity planning, no throttling risk) and only recommends provisioned when it's demonstrably cheaper (>15%).

## How It Works

See [HOW_IT_WORKS.md](HOW_IT_WORKS.md) for a detailed step-by-step explanation of the analysis and decision logic.

**Short version:** The agent collects 7 days of CloudWatch metrics for every Kinesis stream in the region, discovers EFO consumers, computes throughput patterns and cost comparisons across all three pricing modes, then recommends per-stream actions (provisioned vs on-demand) and an account-level On-demand Advantage assessment. Results are stored as both JSON and a visual HTML report in S3.

## Architecture

```
EventBridge Schedule (daily/weekly/custom)
    │
    ▼
Scheduler Lambda ──► Bedrock Agent 
                          │
                          ▼
                     Action Group Lambda
                          │
                ┌─────────┼─────────┐
                ▼         ▼         ▼
           Kinesis    CloudWatch    S3
           (list)     (metrics)   (report)
```

## Region Behavior

**The agent is region-specific.** When deployed to a region, it automatically analyzes only the Kinesis Data Streams in that region. The Lambda picks up the region from its runtime environment (`AWS_REGION`) — no hardcoded region configuration needed.

**To cover multiple regions**, set the region environment variable and deploy separately for each:

**PowerShell:**
```powershell
$env:AWS_DEFAULT_REGION="us-east-1"
cdk bootstrap aws://<ACCOUNT_ID>/us-east-1
cdk deploy

$env:AWS_DEFAULT_REGION="eu-west-1"
cdk bootstrap aws://<ACCOUNT_ID>/eu-west-1
cdk deploy
```

Each deployment is independent — its own agent, its own S3 bucket, its own schedule.

## Prerequisites

- **AWS CDK v2** — `npm install -g aws-cdk`
- **Python 3.12+**
- **AWS CLI** configured with credentials that have admin access
- **Bedrock model access** — ensure you have access to Claude Sonnet 4.5 (or your chosen model) in the target region. Check in the Bedrock console under "Model access".

## Step-by-Step Deployment

### 1. Clone the repository

```bash
git clone <repo-url>
cd kds-optimiser-agent
```

### 2. Install CDK dependencies

```bash
cd infra
pip install -r requirements.txt
```

### 3. Set your target region

The stack deploys to whatever region is set in the `AWS_DEFAULT_REGION` environment variable. Set it before running any CDK commands:

**PowerShell:**
```powershell
$env:AWS_DEFAULT_REGION="us-east-1"
```

**cmd:**
```cmd
set AWS_DEFAULT_REGION=us-east-1
```

**Linux/macOS:**
```bash
export AWS_DEFAULT_REGION=us-east-1
```

### 4. Bootstrap CDK (first time per account/region)

```bash
cdk bootstrap aws://<ACCOUNT_ID>/<REGION>
```

Example:
```bash
cdk bootstrap aws://123456789012/us-east-1
```

### 5. Deploy

```bash
cdk deploy
```

With custom schedule (default is daily):
```bash
cdk deploy --parameters ReportSchedule="rate(7 days)"
```

With a specific bucket name:
```bash
cdk deploy --parameters ReportBucketName=my-kds-reports
```

**To deploy to a different region**, change the environment variable and repeat steps 4-5:
```powershell
$env:AWS_DEFAULT_REGION="eu-west-1"
cdk bootstrap aws://123456789012/eu-west-1
cdk deploy
```

### 6. Check Bedrock model availability (if deploy fails)

The stack automatically selects the right Claude model/inference profile for your region. If you hit model access errors, confirm you have Bedrock model access enabled in the target region:

```bash
aws bedrock list-inference-profiles --region us-east-1 --query "InferenceProfileSummaries[?contains(InferenceProfileId,'claude')].{Id:InferenceProfileId,Name:InferenceProfileName}" --output table
```

### 7. Verify deployment

The deploy outputs will show:
- **AgentId** — Bedrock Agent identifier
- **AgentAliasId** — Alias for invocation
- **ReportBucketOutput** — S3 bucket where reports land
- **LambdaFunctionArn** — The action group Lambda
- **FoundationModel** — Which model was selected for this region

### 7. Test the agent

In the **AWS Bedrock Console**:
1. Go to Agents → `kds-mode-optimizer`
2. Open the Test panel (right side)
3. Select the alias and type: "Analyze all streams and generate a report"

Or via CLI:
```bash
aws bedrock-agent-runtime invoke-agent \
  --agent-id <AGENT_ID> \
  --agent-alias-id <ALIAS_ID> \
  --session-id "test-001" \
  --input-text "Generate an optimization report" \
  --region <REGION>
```

### 8. View reports

Reports are stored in S3 at:
```
s3://<bucket>/kds-optimization-reports/YYYY/MM/DD/HHMMSS-<report-id>.html
s3://<bucket>/kds-optimization-reports/YYYY/MM/DD/HHMMSS-<report-id>.json
```

Generate a pre-signed URL to view the HTML report in a browser:
```bash
aws s3 presign s3://<bucket>/kds-optimization-reports/2026/01/15/080000-abc123.html --expires-in 3600
```

## Configuration

### Schedule expressions

| Expression | Frequency |
|------------|-----------|
| `rate(1 day)` | Daily |
| `rate(7 days)` | Weekly |
| `cron(0 8 ? * MON *)` | Every Monday at 8am UTC |
| `rate(12 hours)` | Twice daily |

### Changing the model

Edit `infra/kds_optimizer_stack.py` and update `foundation_model` to a model available in your target region. Also update the IAM policy resources to match.

## Project Structure

```
├── README.md                   # This file
├── HOW_IT_WORKS.md             # Detailed explanation of the analysis logic
├── .gitignore
├── lambda/
│   ├── handler.py              # Bedrock Agent action group Lambda handler
│   ├── kinesis_analyzer.py     # Core analysis engine (metrics + recommendations)
│   ├── report_generator.py     # HTML + JSON report generation and S3 storage
│   ├── openapi_schema.json     # OpenAPI spec defining agent actions
│   └── requirements.txt
├── infra/
│   ├── app.py                  # CDK app entry point
│   ├── cdk.json                # CDK configuration
│   ├── kds_optimizer_stack.py  # Full infrastructure stack
│   └── requirements.txt
└── tests/
    ├── __init__.py
    ├── test_kinesis_analyzer.py # Unit tests for recommendation logic
    └── test_scale.py           # Scale test (500 streams)
```

## Decision Logic Summary

The analysis produces two types of recommendations:

### Per-Stream Recommendations (Provisioned vs On-demand)

| Condition | Recommendation |
|-----------|---------------|
| Provisioned stream with throttling (>1%) | → **On-demand** (HIGH priority) |
| Highly variable traffic (peak > 3x avg) | → **On-demand** (MEDIUM priority) |
| Provisioned is >15% cheaper AND stable traffic | → Stay **Provisioned** (LOW priority) |
| Everything else | → **On-demand** (LOW priority) |

### Account-Level Recommendation (On-demand Advantage)

On-demand Advantage is an **account-level setting per region** — not a per-stream choice. The report assesses eligibility separately based on:

| Trigger | Threshold |
|---------|-----------|
| Aggregate sustained throughput (7-day avg) | ≥ 10 MiB/s |
| EFO consumers on any stream | > 2 |
| Total streams in account/region | > 50 |

If any trigger is met, the report recommends enabling Advantage at the account level after switching provisioned streams to on-demand.

### Pricing Model

The cost analysis uses full three-way comparison (us-east-1 rates):

| Dimension | On-demand Standard | On-demand Advantage | Provisioned |
|-----------|-------------------|--------------------:|------------:|
| Data ingest | $0.08/GB | $0.032/GB | $0.015/shard-hr + $0.014/1M PUT units |
| Data retrieval | $0.04/GB | $0.016/GB | Included in shard cost |
| EFO retrieval | $0.05/GB | $0.016/GB | $0.013/GB + $0.015/consumer-shard-hr |
| Per-stream charge | $0.04/hr | None | None |
| Retention (24h-7d) | $0.10/GB-month | $0.023/GB-month | $0.02/shard-hr |
| Retention (>7d) | $0.023/GB-month | $0.023/GB-month | $0.023/GB-month |

## Running Tests

```bash
# Unit tests
python -m pytest tests/test_kinesis_analyzer.py -v

# Scale test (simulates 500 streams)
python tests/test_scale.py
```

## Cleanup

To remove the stack from a region:
```bash
cd infra
cdk destroy --region <REGION>
```

Note: The S3 bucket has `RemovalPolicy.RETAIN` — it won't be deleted with the stack. Delete it manually if needed.


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.