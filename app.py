#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stack.analytiikka_stack import AnalytiikkaStack

from stack.helper_tags import add_tags


"""
App

Sama kaikille projekteille

"""
app = cdk.App()

projectname = app.node.try_get_context('project')
dev_account_name = os.environ["CDK_DEFAULT_ACCOUNT"]
region_name = os.environ["CDK_DEFAULT_REGION"]

AnalytiikkaStack(app, 
                     f"{projectname}-stack",
                     env = cdk.Environment(account = dev_account_name, region = region_name)
                )

#
# Yhteiset tagit kaikille
# HUOM: Environment tag lisätään kaikille vasta stagessa (dev/prod)
tags = app.node.try_get_context("tags")
add_tags(app, tags)


app.synth()
