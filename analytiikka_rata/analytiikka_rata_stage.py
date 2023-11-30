from aws_cdk import (
    Stage,
    Environment,
    Tags
)

from constructs import Construct

from analytiikka_rata.analytiikka_rata_services_stack import AnalytiikkaRataServicesStack

"""
Pipeline stage

"""
class AnalytiikkaRataStage(Stage):

    def __init__(self,
                 scope: Construct, 
                 construct_id: str,
                 environment: str,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        projectname = self.node.try_get_context('project')

        account = self.account
        region = self.region

        # print(f"stage {environment}: project = '{projectname}'")
        print(f"stage {environment}: account = '{account}'")
        print(f"stage {environment}: region = '{region}'")
        
        services_stack = AnalytiikkaRataServicesStack(self, 
                                                      f"{projectname}-services-stack-{environment}", 
                                                      environment,
                                                      env = Environment(account = account, region = region )
                                                      )
        
        Tags.of(services_stack).add("Environment", environment, apply_to_launched_instances = True, priority = 300)
        _tags_lst = self.node.try_get_context("tags")
        if _tags_lst:
            for _t in _tags_lst:
                for k, v in _t.items():
                    Tags.of(services_stack).add(k, v, apply_to_launched_instances = True, priority = 300)


