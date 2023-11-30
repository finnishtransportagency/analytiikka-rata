#!/usr/bin/env python3
import os

import aws_cdk as cdk

from analytiikka_rata.analytiikka_rata_stack import AnalytiikkaRataStack


app = cdk.App()

projectname = app.node.try_get_context('project')
dev_account_name = os.environ["CDK_DEFAULT_ACCOUNT"]
region_name = os.environ["CDK_DEFAULT_REGION"]

"""
App

"""
AnalytiikkaRataStack(app, "AnalytiikkaRataStack",
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.

    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.

    #env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),
    env=cdk.Environment(account = dev_account_name, region = region_name),

    # Uncomment the next line if you know exactly what Account and Region you
    # want to deploy the stack to. */

    #env=cdk.Environment(account='123456789012', region='us-east-1'),

    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    )

# Yhteiset tagit kaikille
# Environment tag lisätään kaikille stagessa (dev/prod)
_tags_lst = app.node.try_get_context("tags")
if _tags_lst:
    for _t in _tags_lst:
        for k, v in _t.items():
            cdk.Tags.of(app).add(k, v, apply_to_launched_instances = True, priority = 300)


app.synth()
