#!/usr/bin/env python3
"""CDK app entry point for the KDS Optimizer Agent stack."""

import os

import aws_cdk as cdk

from kds_optimizer_stack import KdsOptimizerStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION"),
)

KdsOptimizerStack(app, "KdsOptimizerAgentStack", env=env)
app.synth()
