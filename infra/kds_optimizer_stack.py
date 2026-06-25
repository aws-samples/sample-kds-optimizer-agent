"""
CDK Stack for the Kinesis Data Streams Mode Optimizer Bedrock Agent.

Deploys:
- S3 bucket for reports
- Lambda function (action group handler)
- IAM roles for Bedrock Agent and Lambda
- Bedrock Agent with action group
- EventBridge Scheduler to invoke the agent on a schedule

The stack dynamically selects the appropriate foundation model / inference profile
based on the deployment region.
"""

import json
from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    CfnParameter,
    Duration,
    RemovalPolicy,
    Stack,
    aws_bedrock as bedrock,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct

# Map of regions to the best available Claude model/inference profile.
# Inference profiles are used where direct on-demand invocation isn't supported.
# Update this map if new models or profiles become available.
REGION_MODEL_MAP = {
    # US regions — Claude Sonnet 4.5 via US inference profile
    "us-east-1": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "us-east-2": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "us-west-2": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    # EU regions — Claude Sonnet 4.5 via EU inference profile
    "eu-west-1": "eu.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "eu-west-2": "eu.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "eu-west-3": "eu.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "eu-central-1": "eu.anthropic.claude-sonnet-4-5-20250929-v1:0",
    # APAC regions
    "ap-southeast-2": "au.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "ap-northeast-1": "jp.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "ap-northeast-2": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "ap-southeast-1": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "ap-south-1": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
}

# Fallback model if region not in map — global inference profile
DEFAULT_MODEL = "global.anthropic.claude-sonnet-4-5-20250929-v1:0"


def get_model_for_region(region: str) -> str:
    """Return the best model/inference profile ID for the given region."""
    return REGION_MODEL_MAP.get(region, DEFAULT_MODEL)


class KdsOptimizerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Resolve the deploy region at synth time
        deploy_region = self.region if self.region else "us-east-1"
        foundation_model = get_model_for_region(deploy_region)

        # --- Parameters ---
        report_schedule = CfnParameter(
            self,
            "ReportSchedule",
            type="String",
            default="rate(1 day)",
            description="EventBridge schedule expression for report generation (e.g., rate(1 day), rate(7 days), cron(0 8 ? * MON *)).",
        )

        report_bucket_name = CfnParameter(
            self,
            "ReportBucketName",
            type="String",
            default="",
            description="S3 bucket name for storing reports. Leave empty for auto-generated name.",
        )

        # --- S3 Bucket for Reports ---
        bucket_props = {
            "removal_policy": RemovalPolicy.RETAIN,
            "encryption": s3.BucketEncryption.S3_MANAGED,
            "block_public_access": s3.BlockPublicAccess.BLOCK_ALL,
            "enforce_ssl": True,
            "versioned": True,
        }

        has_bucket_name = cdk.CfnCondition(
            self,
            "HasBucketName",
            expression=cdk.Fn.condition_not(
                cdk.Fn.condition_equals(report_bucket_name.value_as_string, "")
            ),
        )

        report_bucket = s3.Bucket(
            self,
            "ReportBucket",
            bucket_name=cdk.Fn.condition_if(
                has_bucket_name.logical_id,
                report_bucket_name.value_as_string,
                cdk.Aws.NO_VALUE,
            ).to_string(),
            **bucket_props,
        )

        # --- Lambda Function ---
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kinesis:ListStreams",
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:ListShards",
                    "kinesis:ListStreamConsumers",
                ],
                resources=["*"],
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "cloudwatch:GetMetricStatistics",
                    "cloudwatch:GetMetricData",
                    "cloudwatch:ListMetrics",
                ],
                resources=["*"],
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sts:GetCallerIdentity"],
                resources=["*"],
            )
        )

        report_bucket.grant_write(lambda_role)

        lambda_fn = _lambda.Function(
            self,
            "KdsOptimizerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                str(Path(__file__).parent.parent / "lambda")
            ),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "REPORT_BUCKET_NAME": report_bucket.bucket_name,
            },
            description="KDS Mode Optimizer - Bedrock Agent Action Group handler",
        )

        # --- Bedrock Agent IAM Role ---
        agent_role = iam.Role(
            self,
            "BedrockAgentRole",
            assumed_by=iam.ServicePrincipal(
                "bedrock.amazonaws.com",
                conditions={
                    "StringEquals": {"aws:SourceAccount": cdk.Aws.ACCOUNT_ID},
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:bedrock:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:agent/*"
                    },
                },
            ),
            inline_policies={
                "BedrockAgentPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "bedrock:InvokeModel",
                                "bedrock:InvokeModelWithResponseStream",
                            ],
                            resources=[
                                "arn:aws:bedrock:*::foundation-model/*",
                                f"arn:aws:bedrock:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:inference-profile/*",
                            ],
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "bedrock:GetInferenceProfile",
                                "bedrock:ListInferenceProfiles",
                                "bedrock:GetFoundationModel",
                            ],
                            resources=["*"],
                        ),
                    ]
                )
            },
        )

        # --- Bedrock Agent ---
        agent_instruction = """You are a Kinesis Data Streams capacity mode optimization agent. Your job is to analyze 
Kinesis Data Streams usage in this AWS account and region, and recommend the best capacity mode for each stream.

When asked to generate a report or analyze streams, you should:
1. Call the analyzeStreams API to get current usage metrics and recommendations for all streams.
2. Call the generateReport API to store the analysis as a report in S3.
3. Summarize findings including which streams need mode changes, the reasoning, and priorities.

Mode selection principles:
- STRONGLY PREFER On-demand modes over Provisioned because of automatic scaling, zero capacity planning, and no throttling risk.
- Recommend On-demand Advantage when the account aggregate throughput (ingest + retrieval) across on-demand streams is >= 10 MiB/s, as it offers 60%+ lower pricing.
- Recommend On-demand Standard for variable/unpredictable traffic patterns.
- Only recommend Provisioned mode when it is more than 15% cheaper than on-demand AND traffic is stable. Even then, note that on-demand is operationally simpler.
- Always recommend switching away from Provisioned if throttling is detected.

When presenting results, highlight high-priority actions first and explain the cost and operational implications of each recommendation."""

        agent = bedrock.CfnAgent(
            self,
            "KdsOptimizerAgent",
            agent_name="kds-mode-optimizer",
            agent_resource_role_arn=agent_role.role_arn,
            foundation_model=foundation_model,
            instruction=agent_instruction,
            description="Analyzes Kinesis Data Streams usage and recommends optimal capacity modes (On-demand Standard, On-demand Advantage, or Provisioned).",
            idle_session_ttl_in_seconds=600,
            auto_prepare=True,
            action_groups=[
                bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="KdsOptimizerActions",
                    description="Actions to analyze Kinesis Data Streams and generate optimization reports",
                    action_group_executor=bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=lambda_fn.function_arn,
                    ),
                    api_schema=bedrock.CfnAgent.APISchemaProperty(
                        payload=json.dumps(
                            json.loads(
                                (Path(__file__).parent.parent / "lambda" / "openapi_schema.json").read_text()
                            )
                        )
                    ),
                )
            ],
        )

        # Grant Bedrock permission to invoke the Lambda
        lambda_fn.add_permission(
            "BedrockInvokePermission",
            principal=iam.ServicePrincipal("bedrock.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:bedrock:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:agent/*",
        )

        # --- Bedrock Agent Alias ---
        agent_alias = bedrock.CfnAgentAlias(
            self,
            "KdsOptimizerAgentAlias",
            agent_id=agent.attr_agent_id,
            agent_alias_name="live",
            description=f"Live alias - model: {foundation_model}",
        )
        agent_alias.add_dependency(agent)

        # --- EventBridge Rule to trigger the agent on schedule ---
        scheduler_role = iam.Role(
            self,
            "SchedulerInvokeRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        scheduler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeAgent"],
                resources=[
                    f"arn:aws:bedrock:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:agent-alias/*",
                ],
            )
        )

        scheduler_lambda = _lambda.Function(
            self,
            "SchedulerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_inline(self._scheduler_lambda_code()),
            role=scheduler_role,
            timeout=Duration.minutes(10),
            memory_size=256,
            environment={
                "AGENT_ID": agent.attr_agent_id,
                "AGENT_ALIAS_ID": agent_alias.attr_agent_alias_id,
            },
            description="Triggers the KDS Optimizer Bedrock Agent on a schedule",
        )

        # EventBridge rule on schedule
        rule = events.Rule(
            self,
            "ScheduleRule",
            schedule=events.Schedule.expression(report_schedule.value_as_string),
            description="Triggers KDS Optimizer Agent to generate a report",
        )
        rule.add_target(targets.LambdaFunction(scheduler_lambda))

        # --- Outputs ---
        cdk.CfnOutput(self, "ReportBucketOutput", value=report_bucket.bucket_name)
        cdk.CfnOutput(self, "AgentId", value=agent.attr_agent_id)
        cdk.CfnOutput(self, "AgentAliasId", value=agent_alias.attr_agent_alias_id)
        cdk.CfnOutput(self, "LambdaFunctionArn", value=lambda_fn.function_arn)
        cdk.CfnOutput(self, "FoundationModel", value=foundation_model)

    def _scheduler_lambda_code(self) -> str:
        return '''
import json
import os
import boto3

def handler(event, context):
    """Invoke the Bedrock Agent to generate a KDS optimization report."""
    agent_id = os.environ["AGENT_ID"]
    agent_alias_id = os.environ["AGENT_ALIAS_ID"]

    client = boto3.client("bedrock-agent-runtime")

    response = client.invoke_agent(
        agentId=agent_id,
        agentAliasId=agent_alias_id,
        sessionId=f"scheduled-{context.aws_request_id}",
        inputText="Analyze all Kinesis Data Streams in this account and region. Generate a full optimization report and store it in S3. Summarize any high-priority recommendations.",
    )

    # Consume the response stream
    completion = ""
    for event_chunk in response.get("completion", []):
        if "chunk" in event_chunk:
            chunk_data = event_chunk["chunk"]
            if "bytes" in chunk_data:
                completion += chunk_data["bytes"].decode("utf-8")

    print(f"Agent response: {completion[:1000]}")
    return {"statusCode": 200, "body": completion[:5000]}
'''
