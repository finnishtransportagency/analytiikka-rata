from aws_cdk import (
    aws_iam,
    aws_ec2,
    aws_glue,
    aws_glue_alpha,
    aws_s3,
    aws_s3_deployment,
    Duration,
    Tags
)

import os

from constructs import Construct

from stack.helper_tags import add_tags


"""
Apukoodit glue- ajojen luontiin
"""





"""
Worker type muunnos str -> WorkerType
"""
def get_worker_type(worker: str) -> aws_glue_alpha.WorkerType:
    value = aws_glue_alpha.WorkerType.G_1_X
    if worker == "G 2X":
        value = aws_glue_alpha.WorkerType.G_2_X
    elif worker == "G 4X":
        value = aws_glue_alpha.WorkerType.G_4_X
    elif worker == "G 8X":
        value = aws_glue_alpha.WorkerType.G_8_X
    elif worker == "G 025X":
        value = aws_glue_alpha.WorkerType.G_025_X
    elif worker == "Z 2X":
        value = aws_glue_alpha.WorkerType.Z_2_X
    elif worker == "STANDARD":
        value = aws_glue_alpha.WorkerType.STANDARD
    return(value)

"""
Timeout numero -> Duration
"""
def get_timeout(timeout: int) -> Duration:
    value = Duration.minutes(1)
    if timeout != None and timeout > 0:
        value = Duration.minutes(timeout)
    return(value)


"""
Versio str -> GlueVersion
"""
def get_version(version: str) -> aws_glue_alpha.GlueVersion:
    value = aws_glue_alpha.GlueVersion.V4_0
    if version != "" and version != None:
        value = aws_glue_alpha.GlueVersion.of(version)
    return(value)


"""
Polku
"""
def get_path(path: str) -> os.path:
    return(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), path))

#def get_directory(path: str) -> os.path:
#    return(os.path.dirname(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), path)))



"""

Properties:
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-connections.html#aws-glue-api-catalog-connections-Connection

"""
class GlueJdbcConnection(Construct):
    """
    Glue connection
    """
    def __init__(self,
                 scope: Construct,
                 id: str,
                 project_tag: str,
                 description: str = None,
                 vpc: any = None,
                 security_groups: list = None,
                 properties: dict = None,
                 tags: dict = None
                 ):
        super().__init__(scope, id)

        selected = vpc.select_subnets()
        self.subnets = aws_ec2.SubnetSelection(subnets = selected.subnets)

        self.connection = aws_glue_alpha.Connection(self,
                                                    id = id,
                                                    description = description,
                                                    connection_name = id,
                                                    type = aws_glue_alpha.ConnectionType.JDBC,
                                                    properties = properties,
                                                    security_groups = security_groups,
                                                    subnet = self.subnets.subnets[0]
                                                    )
        add_tags(self.connection, tags, project_tag = project_tag)





"""

id: Ajon nimi
path: polku projektissa (= /glue/<jobname>)
timeout: aikaraja minuutteina, oletus = 1
description: Kuvaus
worker: G.1X, G.2X, G.4X, G.8X, G.025X, Z.2X, oletus = G.1X
version: glue versio, oletus = 4.0
role: Glue IAM roolin nimi
tags: Lisätagit
arguments: oletusparametrit
connections: connectit, lista
enable_spark_ui:  spark ui päälle/pois, oletus = pois
schedule: ajastus, cron expressio
schedule_description: ajastuksen kuvaus
"""
class PythonSparkGlueJob(Construct):

    def __init__(self,
                 scope: Construct, 
                 id: str, 
                 path: str,
                 index: str,
                 script_bucket: aws_s3.Bucket,
                 timeout_min: any,
                 project_tag: str,
                 description: str = None,
                 worker: str = None,
                 version: str = None,
                 role: aws_iam.Role = None,
                 tags: dict = None,
                 arguments: dict = None,
                 connections: list = None,
                 enable_spark_ui: bool = False,
                 schedule: str = None,
                 schedule_description: str = None
                 ):
        super().__init__(scope, id)

        """
        https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_glue/CfnJob.html
        execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=123)
        """
        deployment = aws_s3_deployment.BucketDeployment(self, f"{id}-deploy",
            sources = [ aws_s3_deployment.Source.asset(get_path(path)) ],
            destination_bucket = script_bucket,
            destination_key_prefix = path
        )

        self.job = aws_glue_alpha.Job(self, 
                                           id = id,
                                           job_name = id,
                                           spark_ui = aws_glue_alpha.SparkUIProps(
                                               enabled = enable_spark_ui
                                           ),
                                           executable = aws_glue_alpha.JobExecutable.python_etl(
                                               glue_version = get_version(version),
                                               python_version = aws_glue_alpha.PythonVersion.THREE,
                                               #script = aws_glue_alpha.Code.from_asset(get_path(path))
                                               script = aws_glue_alpha.Code.from_bucket(deployment.deployed_bucket, f"{path}/{index}")
                                           ),
                                           description = description,
                                           default_arguments = arguments,
                                           role = role,
                                           worker_type = get_worker_type(worker),
                                           worker_count = 2,
                                           max_retries = 0,
                                           timeout = get_timeout(timeout_min),
                                           max_concurrent_runs = 2,
                                           connections = connections
                                           )

        add_tags(self.job, tags, project_tag = project_tag)

        if schedule != None and schedule != "":
            trigger_name = f"{id}-trigger"
            schedule = f"cron({schedule})"
            self.trigger = aws_glue.CfnTrigger(self,
                                        id = trigger_name,
                                        actions = [aws_glue.CfnTrigger.ActionProperty(
                                            arguments = arguments,
                                            job_name = id,
                                            timeout = timeout_min
                                            )
                                        ],
                                        type = "SCHEDULED",
                                        name = trigger_name,
                                        description = schedule_description,
                                        schedule = schedule,
                                        start_on_creation = False
                                       )
            add_tags(self.trigger, tags, project_tag = project_tag)








class PythonShellGlueJob(Construct):

    def __init__(self,
                 scope: Construct, 
                 id: str, 
                 path: str,
                 index: str,
                 script_bucket: aws_s3.Bucket,
                 timeout_min: int,
                 project_tag: str,
                 description: str = None,
                 role: aws_iam.Role = None,
                 tags: dict = None,
                 arguments: dict = None,
                 connections: list = None,
                 schedule: str = None,
                 schedule_description: str = None,
                 include_standard_libraries: bool = True
                 ):
        super().__init__(scope, id)

        """
        https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_glue/CfnJob.html
        execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=123)
        """

        default_arguments = None
        if arguments != None:
            default_arguments = arguments.copy()
        if include_standard_libraries:
            if default_arguments != None:
                if not "library-set" in default_arguments:
                    default_arguments["library-set"] = "analytics"
            else:
                default_arguments = {
                    "library-set": "analytics"
                    }

        deployment = aws_s3_deployment.BucketDeployment(self, f"{id}-deploy",
            sources = [ aws_s3_deployment.Source.asset(get_path(path)) ],
            destination_bucket = script_bucket,
            destination_key_prefix = path
        )

        self.job = aws_glue_alpha.Job(self, 
                                           id = id,
                                           job_name = id,
                                           executable = aws_glue_alpha.JobExecutable.python_shell(
                                               glue_version = aws_glue_alpha.GlueVersion.V3_0,
                                               python_version = aws_glue_alpha.PythonVersion.THREE_NINE,
                                               #script = aws_glue_alpha.Code.from_asset(get_path(path))
                                               script = aws_glue_alpha.Code.from_bucket(deployment.deployed_bucket, f"{path}/{index}")
                                           ),
                                           description = description,
                                           default_arguments = default_arguments,
                                           role = role,
                                           max_retries = 0,
                                           timeout = get_timeout(timeout_min),
                                           max_concurrent_runs = 1,
                                           connections = connections
                                           )

        add_tags(self.job, tags, project_tag = project_tag)

        if schedule != None and schedule != "":
            trigger_name = f"{id}-trigger"
            schedule = f"cron({schedule})"
            self.trigger = aws_glue.CfnTrigger(self,
                                        id = trigger_name,
                                        actions = [aws_glue.CfnTrigger.ActionProperty(
                                            arguments = arguments,
                                            job_name = id,
                                            timeout = timeout_min
                                            )
                                        ],
                                        type = "SCHEDULED",
                                        name = trigger_name,
                                        description = schedule_description,
                                        schedule = schedule,
                                        start_on_creation = False
                                       )
            add_tags(self.trigger, tags, project_tag = project_tag)


