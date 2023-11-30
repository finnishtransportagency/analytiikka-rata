from aws_cdk import (
    aws_lambda,
    aws_lambda_python_alpha,
    aws_events,
    aws_events_targets,
    aws_logs,
    Duration,
    BundlingOutput,
    aws_iam,
    aws_ec2,
    Tags
)

from constructs import Construct


"""
Apukoodit lambdojen luontiin



"""



"""
Lambda parametrit

vpc:  Vpc
securitygroups: Sequence[ISecurityGroup]
timeout:  int, sekunteja
memory: int, byte
environment: dict
tags: dict   Lambda lisätagit vakioiden lisäksi
schedule: str  cron expression


"""
class LambdaProperties:
    def __init__(self, 
                 vpc: aws_ec2.Vpc = None, 
                 securitygroups = None, 
                 timeout_min: int = None, 
                 memory_mb: int = None, 
                 environment: dict = None, 
                 tags: dict = None, 
                 schedule: str = None):
        self.vpc = vpc
        self.subnets = None
        if vpc != None:
            selected = vpc.select_subnets()
            self.subnets = aws_ec2.SubnetSelection(subnets = selected.subnets)
        self.securitygroups = securitygroups
        self.timeout_min = Duration.minutes(timeout_min)
        self.memory_mb = memory_mb
        self.environment = environment
        self.tags = tags
        self.schedule = schedule



"""
Lisää tagit
"""
def add_tags(function, tags):
    if tags:
        for _t in tags:
            for k, v in _t.items():
                Tags.of(function).add(k, v, apply_to_launched_instances = True, priority = 300)


"""
Lisää ajastus
"""
def add_schedule(self, function, id, schedule):
    if schedule != None and schedule != "":
        rule_name = f"{id}-schedule"
        rule = aws_events.Rule(self,
                               rule_name,
                               rule_name = rule_name,
                               schedule = aws_events.Schedule.expression(f"cron({schedule})")
        )
        rule.add_target(aws_events_targets.LambdaFunction(function))


def get_pythonruntime(runtime: str):
    lambda_runtime = aws_lambda.Runtime.PYTHON_3_11
    if runtime != None:
        if runtime == "3.7":
            lambda_runtime = aws_lambda.Runtime.PYTHON_3_7
        elif runtime == "3.8":
            lambda_runtime = aws_lambda.Runtime.PYTHON_3_8
        elif runtime == "3.9":
            lambda_runtime = aws_lambda.Runtime.PYTHON_3_9
        elif runtime == "3.10":
            lambda_runtime = aws_lambda.Runtime.PYTHON_3_10
        elif runtime == "3.11":
            lambda_runtime = aws_lambda.Runtime.PYTHON_3_11
        elif runtime == "3.12":
            lambda_runtime = aws_lambda.Runtime.PYTHON_3_12
    return(lambda_runtime)

def get_noderuntime(runtime: str):
    lambda_runtime = aws_lambda.Runtime.NODEJS_18_X
    if runtime != None:
        if runtime == "10":
            lambda_runtime = aws_lambda.Runtime.NODEJS_10_X
        elif runtime == "12":
            lambda_runtime = aws_lambda.Runtime.NODEJS_12_X
        elif runtime == "14":
            lambda_runtime = aws_lambda.Runtime.NODEJS_14_X
        elif runtime == "16":
            lambda_runtime = aws_lambda.Runtime.NODEJS_16_X
        elif runtime == "18":
            lambda_runtime = aws_lambda.Runtime.NODEJS_18_X
        elif runtime == "20":
            lambda_runtime = aws_lambda.Runtime.NODEJS_20_X
        elif runtime == "LATEST":
            lambda_runtime = aws_lambda.Runtime.NODEJS_LATEST
    return(lambda_runtime)


def get_javaruntime(runtime: str):
    lambda_runtime = aws_lambda.Runtime.JAVA_11
    if runtime != None:
        if runtime == "8":
            lambda_runtime = aws_lambda.Runtime.JAVA_8
        elif runtime == "11":
            lambda_runtime = aws_lambda.Runtime.JAVA_11
        elif runtime == "17":
            lambda_runtime = aws_lambda.Runtime.JAVA_17
        elif runtime == "21":
            lambda_runtime = aws_lambda.Runtime.JAVA_21
    return(lambda_runtime)






"""
Python lambda, 3.11

Jos tarvitaan layer: https://github.com/aws-samples/aws-cdk-examples/blob/master/python/lambda-layer/app.py


"""
class PythonLambdaFunction(Construct):

    def __init__(self,
                 scope: Construct, 
                 id: str, 
                 path: str,
                 index: str,
                 handler: str,
                 description: str,
                 role: aws_iam.Role,
                 props: LambdaProperties,
                 runtime: str = None
                 ):
        super().__init__(scope, id)



        self.function = aws_lambda_python_alpha.PythonFunction(
            self, 
            id,
            function_name = id,
            description = description,
            runtime = get_pythonruntime(runtime),
            entry = path,
            index = index,
            handler = handler,
            role = role,
            vpc = props.vpc,
            security_groups = props.securitygroups,
            timeout = props.timeout_min,
            memory_size = props.memory_mb,
            environment = props.environment,
            log_retention = aws_logs.RetentionDays.THREE_MONTHS
            )

        add_tags(self.function, props.tags)
        add_schedule(self, self.function, id, props.schedule)






"""
Java lambda

HUOM: olettaa että on maven- projekti, java 11

"""
class JavaLambdaFunction(Construct):

    def __init__(self,
                 scope: Construct, 
                 id: str, 
                 description: str,
                 path: str,
                 jarname: str,
                 handler: str,
                 role: aws_iam.Role,
                 props: LambdaProperties,
                 runtime: str = None
                 ):
        super().__init__(scope, id)

        func_code = aws_lambda.Code.from_asset(path = path,
                                               bundling = {
                                                   "command": [
                                                       "bash",
                                                       "-c",
                                                       f"mvn clean install && cp ./target/{jarname} /asset-output/",
                                                    ],
                                                    "image": aws_lambda.Runtime.JAVA_11.bundling_image,
                                                    "user": "root",
                                                    "output_type": BundlingOutput.ARCHIVED
                                               }
                                              )
        
        self.function = aws_lambda.Function(self,
                                            id,
                                            function_name = id,
                                            description = description,
                                            code = func_code,
                                            vpc = props.vpc,
                                            vpc_subnets = props.subnets,
                                            security_groups = props.securitygroups,
                                            log_retention = aws_logs.RetentionDays.THREE_MONTHS,
                                            handler = handler,
                                            runtime = get_javaruntime(runtime),
                                            timeout = props.timeout_min,
                                            memory_size = props.memory_mb,
                                            environment = props.environment,
                                            role = role
                                           )
        
        add_tags(self.function, props.tags)
        add_schedule(self, self.function, id, props.schedule)



"""
Node.js lambda

Runtime = 18.x

"""
class NodejsLambdaFunction(Construct):

    def __init__(self,
                 scope: Construct, 
                 id: str, 
                 path: str,
                 handler: str,
                 description: str,
                 role: aws_iam.Role,
                 props: LambdaProperties,
                 runtime: str = None
                 ):
        super().__init__(scope, id)

        self.function = aws_lambda.Function(self,
                                            id,
                                            function_name = id,
                                            description = description,
                                            code = aws_lambda.AssetCode(path = path),
                                            vpc = props.vpc,
                                            vpc_subnets = props.subnets,
                                            security_groups = props.securitygroups,
                                            log_retention = aws_logs.RetentionDays.THREE_MONTHS,
                                            handler = handler,
                                            runtime = get_noderuntime(runtime),
                                            timeout = props.timeout_min,
                                            memory_size = props.memory_mb,
                                            environment = props.environment,
                                            role = role
                                           )
        
        add_tags(self.function, props.tags)
        add_schedule(self, self.function, id, props.schedule)



        

