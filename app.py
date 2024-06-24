#!/usr/bin/env python3
import os

import aws_cdk as cdk

from sagemaker_autopilot_time_series_monitoring_retraining.resource_stack import ResourceStack

app = cdk.App()
cdk_env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
)

ResourceStack(
    app,
    "ResourceStack",
    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.
    env=cdk_env,
)

app.synth()
