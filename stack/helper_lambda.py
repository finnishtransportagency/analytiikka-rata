from aws_cdk import (
    aws_lambda,
    # aws_lambda_python_alpha,
    aws_events,
    aws_events_targets,
    aws_logs,
    Duration,
    BundlingOutput,
    BundlingOptions,
    ILocalBundling,
    aws_iam,
    aws_ec2
)

from constructs import Construct

from stack.helper_tags import add_tags

import jsii
import subprocess
import os
import glob

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
                 schedule: str = None
                 ):
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


def get_pythonruntime(runtime: str) -> aws_lambda.Runtime:
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




def get_noderuntime(runtime: str) -> aws_lambda.Runtime:
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


def get_javaruntime(runtime: str) -> aws_lambda.Runtime:
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

Lokaali python build

Luokan parametri path = lambdan lokaali polku

TODO: 
TODO: python version tarkastus ?

"""
@jsii.implements(ILocalBundling)
class PythonLambdaBundle:

    def __init__(self, path: str):
        self.sourcepath = path

    def try_bundle(self, output_dir, *, image, entrypoint=None, command=None, volumes=None, volumesFrom=None, environment=None, workingDirectory=None, user=None, local=None, outputType=None, securityOpt=None, network=None, bundlingFileAccess=None, platform=None):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        source_dir = os.path.join(base_dir, self.sourcepath)

        can_run_locally = True # TODO: replace with actual logic
        if can_run_locally:
            # Lokaali build
            print(f"local build lambda '{source_dir}' -> '{output_dir}'")

            print(f"command = 'pip install -r {source_dir}/requirements.txt -t {output_dir}/asset-output'")
            r = subprocess.run(["pip", "install", "-r", f"{source_dir}/requirements.txt", "-t", f"{output_dir}/asset-output"], capture_output = True) 
            print(f"local build lambda, pip done ({r.returncode}): stdout = '{r.stdout}', stderr = '{r.stderr}'")
            if r.returncode != 0:
                return False

            # remove_list = [ "*.dist-info", "boto*", "dateutil", "jmespath", "pytz", "s3transfer", "tzdata", "urllib*" ]
            # for item in remove_list:
            #     print(f"command = 'rm -r {output_dir}/asset-output/{item}'")
            #     r = subprocess.run(["rm", "-r"] + glob.glob(f"{output_dir}/asset-output/{item}"), capture_output = True) 
            #     if r.returncode != 0:
            #         print(f"local build lambda '{source_dir}' -> '{output_dir}': rm failed: stdout = '{r.stdout}', stderr = '{r.stderr}'")
            #         return False

            print(f"command = 'cp -auv {source_dir}/* {output_dir}/asset-output/'")
            r = subprocess.run(["cp", "-auv"] + glob.glob(f"{self.sourcepath}/*") + [f"{output_dir}/asset-output/"], capture_output=True)
            print(f"local build lambda, cp done ({r.returncode}): stdout = '{r.stdout}', stderr = '{r.stderr}'")
            if r.returncode != 0:
                return False

            return True
        return False





"""
Python lambda, 3.11

Jos tarvitaan layer: https://github.com/aws-samples/aws-cdk-examples/blob/master/python/lambda-layer/app.py


"""
class PythonLambdaFunction(Construct):

    def __init__(self,
                 scope: Construct,
                 id: str,
                 path: str,
                 handler: str,
                 description: str,
                 role: aws_iam.Role,
                 props: LambdaProperties,
                 project_tag: str,
                 runtime: str = None,
                 layers: list[aws_lambda.LayerVersion] = None
                 ):
        super().__init__(scope, id)


        python_runtime = get_pythonruntime(runtime)

        """
        Normaalisti local build.
        Jos annetty layer niin poistetaan local build
        Lokaalin kanssa kaatuu asennukseen jos kirjastojen koko on tarpeeksi suuri
        """
        local_bundle = PythonLambdaBundle(path = path)
        if layers != None:
            local_bundle = None

        func_code = aws_lambda.Code.from_asset(path = path,
                                               bundling = BundlingOptions(
                                                   command = [
                                                       "bash",
                                                       "-c",
                                                       "if [ -f requirements.txt ]; then  pip install -r requirements.txt -t /asset-output ; fi && cp -au . /asset-output"
                                                   ],
                                                   image = python_runtime.bundling_image,
                                                   local = local_bundle
                                               )
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
                                            runtime = python_runtime,
                                            timeout = props.timeout_min,
                                            memory_size = props.memory_mb,
                                            environment = props.environment,
                                            role = role,
                                            layers = layers
                                           )

        # self.function = aws_lambda_python_alpha.PythonFunction(
        #     self, 
        #     id,
        #     function_name = id,
        #     description = description,
        #     runtime = python_runtime,
        #     entry = path,
        #     index = index,
        #     handler = handler,
        #     role = role,
        #     vpc = props.vpc,
        #     security_groups = props.securitygroups,
        #     timeout = props.timeout_min,
        #     memory_size = props.memory_mb,
        #     environment = props.environment,
        #     log_retention = aws_logs.RetentionDays.THREE_MONTHS,
        #     layers = layers
        #     )

        add_tags(self.function, props.tags, project_tag = project_tag)
        add_schedule(self, self.function, id, props.schedule)






"""

Lokaali java build

Luokan parametri path = lambdan lokaali polku

TODO: 
TODO: java version tarkastus ?

"""
@jsii.implements(ILocalBundling)
class JavaLambdaBundle:

    def __init__(self, path: str, jarname: str):
        self.sourcepath = path
        self.jarname = jarname

    def try_bundle(self, output_dir, *, image, entrypoint=None, command=None, volumes=None, volumesFrom=None, environment=None, workingDirectory=None, user=None, local=None, outputType=None, securityOpt=None, network=None, bundlingFileAccess=None, platform=None):
        base_dir = os.path.dirname(os.path.dirname(__file__))
        source_dir = os.path.join(base_dir, self.sourcepath)

        can_run_locally = True # TODO: replace with actual logic
        if can_run_locally:
            # Lokaali build
            print(f"local build lambda '{source_dir}' -> '{output_dir}'")

            print(f"command = 'mvn -f {source_dir}/ clean install")
            r = subprocess.run(["mvn", "-f", f"{source_dir}/", "clean", "install"], capture_output = True) 
            print(f"local build lambda, mvn done ({r.returncode}): stdout = '{r.stdout}', stderr = '{r.stderr}'")
            if r.returncode != 0:
                return False

            print(f"command = 'cp -auv {source_dir}/target/{self.jarname} {output_dir}/'")
            r = subprocess.run(["cp", "-auv", f"{source_dir}/target/{self.jarname}", f"{output_dir}/"], capture_output=True)
            print(f"local build lambda, cp done ({r.returncode}): stdout = '{r.stdout}', stderr = '{r.stderr}'")
            if r.returncode != 0:
                return False

            return True
        return False




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
                 project_tag: str,
                 runtime: str = None
                 ):
        super().__init__(scope, id)


        local_bundle = JavaLambdaBundle(path = path, jarname = jarname)
        #if layers != None:
        #    local_bundle = None

        func_code = aws_lambda.Code.from_asset(path = path,
                                               bundling = BundlingOptions(
                                                   command = [
                                                       "bash",
                                                       "-c",
                                                       f"mvn clean install && cp ./target/{jarname} /asset-output/",
                                                   ],
                                                   image = aws_lambda.Runtime.JAVA_11.bundling_image,
                                                   # user = "root",
                                                   output_type = BundlingOutput.ARCHIVED,
                                                   local = local_bundle
                                               )
                                               # bundling = {
                                               #     "command": [
                                               #         "bash",
                                               #         "-c",
                                               #         f"mvn clean install && cp ./target/{jarname} /asset-output/",
                                               #      ],
                                               #      "image": aws_lambda.Runtime.JAVA_11.bundling_image,
                                               #      "user": "root",
                                               #      "output_type": BundlingOutput.ARCHIVED
                                               # }
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
        
        add_tags(self.function, props.tags, project_tag = project_tag)
        add_schedule(self, self.function, id, props.schedule)



"""
Node.js lambda

Runtime = 18.x

HUOM: pitää lisätä kirjastojen asennus ja lokaali build

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
                 project_tag: str,
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
        
        add_tags(self.function, props.tags, project_tag = project_tag)
        add_schedule(self, self.function, id, props.schedule)



        

