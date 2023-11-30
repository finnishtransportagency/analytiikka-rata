from aws_cdk import (
    Stack,
    Environment,
    aws_secretsmanager,
    aws_codepipeline_actions,
    aws_iam
)

from aws_cdk.pipelines import (
    CodePipeline, 
    CodePipelineSource, 
    ShellStep, 
    CodeBuildOptions,
    ManualApprovalStep
)

import aws_cdk.aws_ssm as ssm

from constructs import Construct

from analytiikka_rata.analytiikka_rata_stage import AnalytiikkaRataStage


class AnalytiikkaRataStack(Stack):

    def __init__(self,
                 scope: Construct, 
                 construct_id: str, 
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        devaccount = self.account
        appregion = self.region

        # Projekti == repon nimi
        projectname = self.node.try_get_context('project')
        # Git yhteys
        gitrepo = self.node.try_get_context('gitrepo')
        gitbranch = self.node.try_get_context('gitbranch')
        gittokensecretname = self.node.try_get_context('gittokensecretname')
        prodaccountparameter = self.node.try_get_context('prodaccountparameter')
        prodaccount = ssm.StringParameter.value_from_lookup(self, prodaccountparameter)
        gitsecret = aws_secretsmanager.Secret.from_secret_name_v2(self, "gittoken", secret_name = gittokensecretname)

        # Pipeline
        pipeline =  CodePipeline(self, 
                                 f"{projectname}-pipe",
                                 pipeline_name = f"{projectname}-pipe",
                                 cross_account_keys = True,
                                 synth=ShellStep("Synth",
                                                 input=CodePipelineSource.git_hub(repo_string = gitrepo,
                                                                                  branch = gitbranch,
                                                                                  authentication = gitsecret.secret_value,
                                                                                  trigger = aws_codepipeline_actions.GitHubTrigger("WEBHOOK")
                                                 ),
                                                 
                                                 commands=[
                                                     "npm install -g aws-cdk",
                                                     # "python -m pip install --upgrade pip"
                                                     "python -m pip install -r requirements.txt",
                                                     "npx cdk synth"
                                                 ]
                                                )
                                                ,
                                 code_build_defaults = CodeBuildOptions(
                                     role_policy=[
                                         aws_iam.PolicyStatement(
                                             actions = [ "*" ],
                                             effect = aws_iam.Effect.ALLOW,
                                             resources = [ "*" ]
                                         )
                                     ]
                                 )
                                )


        # Kehitys stage
        dev_stage = pipeline.add_stage(AnalytiikkaRataStage(self,
                                                            f"{projectname}-dev",
                                                            "dev",
                                                            env = Environment(account = devaccount, region = appregion)
                                                            )
                                                           )

        # Tuotanto stage
        prod_stage = pipeline.add_stage(AnalytiikkaRataStage(self,
                                                             f"{projectname}-prod",
                                                             "prod",
                                                             env = Environment(account = prodaccount, region = appregion)
                                                             )
                                                            )

        # Tuotannon manuaalihyväksyntä
        prod_stage.add_pre(ManualApprovalStep('prod-approval'))

