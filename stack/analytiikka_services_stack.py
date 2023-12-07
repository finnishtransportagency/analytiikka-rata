from aws_cdk import (
    Stack,
    aws_ec2,
    aws_s3,
    aws_iam,
    aws_secretsmanager,
    RemovalPolicy
)

from constructs import Construct

from stack.helper_lambda import *
from stack.helper_glue import *
from stack.helper_parameter import *






"""
Palvelut stack

"""
class AnalytiikkaServicesStack(Stack):

    def __init__(self,
                 scope: Construct, 
                 construct_id: str,
                 environment: str,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, stack_name = construct_id, **kwargs)

        """
        Yhteiset arvot projektilta ja ympäristön mukaan
        """
        print(f"services {environment}: account = '{self.account}', region = '{self.region}'")

        properties = self.node.try_get_context(environment)
        # Yhteinen: buketti jdbc- ajureille, glue skripteille jne.
        # Ajurit esim tyypin mukaan omiin polkuihin ( /oracle-driver/ojdbc8.jar, jne )
        script_bucket_name = properties["script_bucket_name"]
        script_bucket = aws_s3.Bucket.from_bucket_name(self, "script-bucket", bucket_name = script_bucket_name)
        # ADE file bucket
        target_bucket_name = properties["ade_staging_bucket_name"]
        # Yhteinen temp- buketti
        temp_bucket_name = properties["temp_bucket_name"]
        # Yhteinen arkisto- buketti
        archive_bucket_name = properties["archive_bucket_name"]
        # Yhteiskäyttöinen rooli lambdoille
        lambda_role_name = self.node.try_get_context("lambda_role_name")
        # Yhteiskäyttöinen securoty group lambdoille. Sallii akiken koska tilin yhteydet on rajattu operaattorin toimesta
        lambda_security_group_name = self.node.try_get_context("lambda_security_group_name")
        # Yhteiskäyttöinen rooli glue- jobeille
        glue_role_name = self.node.try_get_context("glue_role_name")
        # Yhteiskäyttöinen security group glue- jobeille. Sallii akiken koska tilin yhteydet on rajattu operaattorin toimesta
        glue_security_group_name = self.node.try_get_context("glue_security_group_name")


        # print(f"services {environment}: script bucket = '{script_bucket_name}")
        # print(f"services {environment}: target bucket = '{target_bucket_name}")
        # print(f"services {environment}: temp bucket = '{temp_bucket_name}")
        # print(f"services {environment}: archive bucket = '{archive_bucket_name}")
        # print(f"services {environment}: lambda role = '{lambda_role_name}")
        # print(f"services {environment}: lambda sg = '{lambda_security_group_name}")
        # print(f"services {environment}: glue role = '{glue_role_name}")
        # print(f"services {environment}: glue sg = '{glue_security_group_name}")

        # Vpc lookup
        vpc = aws_ec2.Vpc.from_lookup(self,
                                      id = "VPC",
                                      vpc_name = properties["vpc_name"])

        lambda_securitygroup = aws_ec2.SecurityGroup.from_lookup_by_name(self, 
                                                                         "LambdaSecurityGroup", 
                                                                         security_group_name = lambda_security_group_name, 
                                                                         vpc = vpc)
        lambda_role = aws_iam.Role.from_role_name(self, "LambdaRole", role_name= lambda_role_name)

        glue_securitygroup = aws_ec2.SecurityGroup.from_lookup_by_name(self, 
                                                                       "GlueSecurityGroup", 
                                                                       security_group_name = glue_security_group_name, 
                                                                       vpc = vpc)
        glue_role = aws_iam.Role.from_role_name(self, "GlueRole", role_name = glue_role_name)

        glue_common_jdbc_connection_name = self.node.try_get_context("glue_common_jdbc_connection_name")
        glue_common_jdbc_connection = aws_glue_alpha.Connection.from_connection_name(self, "GlueCommonJdbcConnection", connection_name = glue_common_jdbc_connection_name)
        
        





        #
        # HUOM: Lisää tarvittavat tämän jälkeen. Käytä yllä haettuja asioita tarvittaessa (bukettien nimet, roolit, jne)
        #


        # Esimerkki 1 python lambda
        # HUOM: schedule- määritys: https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents-expressions.html

        # l1 = PythonLambdaFunction(self,
        #                      id = "testi1",
        #                      path = "lambda/testi1",
        #                      index = "testi1.py",
        #                      handler = "testi1.lambda_handler",
        #                      description = "Testilambdan kuvaus",
        #                      role = lambda_role,
        #                      props = LambdaProperties(vpc = vpc,
        #                                               timeout_min = 2, 
        #                                               environment = {
        #                                                   "target_bucket": target_bucket_name,
        #                                                   "dummy_input_value": "10001101101"
        #                                               },
        #                                               tags = [
        #                                                   { "testitag": "jotain" },
        #                                                   { "toinen": "arvo" }
        #                                               ],
        #                                               securitygroups = [ lambda_securitygroup ],
        #                                               schedule = "0 10 20 * ? *"
        #                                              )
        #                     )

        # # Servicenow: sn_customerservice_case
        # servicenow_sn_customerservice_case = JavaLambdaFunction(self,
        #                    id = "servicenow-sn_customerservice_case",
        #                    description = "ServiceNow haku taululle sn_customerservice_case",
        #                    path = "lambda/servicenow",
        #                    jarname = "servicenow-to-s3-lambda-1.0.0-jar-with-dependencies.jar",
        #                    handler = "com.cgi.lambda.apifetch.LambdaFunctionHandler",
        #                    role = lambda_role,
        #                    props = LambdaProperties(vpc = vpc,
        #                                             timeout_min = 15,
        #                                             memory_mb = 2048,
        #                                             environment = {
        #                                                 "secret_name": f"api-servicenow-{environment}",
        #                                                 "query_string_default": "sn_customerservice_case?sysparm_query=sys_updated_onBETWEENjavascript%3Ags.daysAgoStart(3)%40javascript%3Ags.endOfYesterday()%5EORsys_created_onBETWEENjavascript%3Ags.daysAgoStart(3)%40javascript%3Ags.endOfYesterday()&sysparm_display_value=true",
        #                                                 "query_string_date": "sn_customerservice_case?sysparm_query=sys_created_onON{DATEFILTER}@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27start%27)@javascript:gs.dateGenerate(%27{DATEFILTER}%27,%27end%27)&sysparm_display_value=true",
        #                                                 "output_split_limit": "1500",
        #                                                 "api_limit": "600",
        #                                                 "output_bucket": target_bucket_name,
        #                                                 "output_path": "servicenow",
        #                                                 "output_filename": "sn_customerservice_case",
        #                                                 "coordinate_transform": "true",
        #                                                 "fullscans":""
        #                                             },
        #                                             tags = None,
        #                                             securitygroups = [ lambda_securitygroup ],
        #                                             schedule = get_parameter(path = "lambda/servicenow", environment = environment, name = "sn_customerservice_case-schedule")
        #                                            )
        #                   )


        # # Trex extra tags
        # trex_tags = [
        #     { "project": "trex" }
        # ]
        # 
        # # Trex reader, glue
        # trex_api_reader_glue = PythonShellGlueJob(self,
        #                                      id = "trex-api-read-glue-job", 
        #                                      path = "glue/trex_api_reader",
        #                                      index = "trex_api_glue_job_script.py",
        #                                      script_bucket = script_bucket,
        #                                      timeout_min = 300,
        #                                      description = "Get data from trex API to S3",
        #                                      role = glue_role,
        #                                      tags = trex_tags,
        #                                      connections = [ glue_common_jdbc_connection.connection ]
        #                                      )
        # 
        # # Trex reader, lambda
        # trex_api_reader_lambda = PythonLambdaFunction(self,
        #                      id = "trex-api-reader",
        #                      path = "lambda/trex_api_reader",
        #                      index = "trex_api_reader.py",
        #                      # HUOM: handler = vain metodi
        #                      handler = "lambda_handler",
        #                      description = "Read Trex API and if needed start Glue Job to read API",
        #                      role = lambda_role,
        #                      runtime = "3.7",
        #                      props = LambdaProperties(vpc = vpc,
        #                                               timeout_min = 15,
        #                                               memory_mb = 512, 
        #                                               environment = {
        #                                                   "FILE_LOAD_BUCKET": target_bucket_name,
        #                                                   "API_STATE_BUCKET": temp_bucket_name,
        #                                                   "GLUE_JOB_NAME": "trex-api-read-glue-job",
        #                                                   "TREX_API_URL": "https://api.vayla.fi/trex/rajapinta/taitorakenne/v1/",
        #                                                   "RAKENTEET": "silta",
        #                                                   "PUBLIC_API_URL": "https://avoinapi.vaylapilvi.fi/vaylatiedot/wfs?request=getfeature&typename=taitorakenteet:silta&SRSNAME=EPSG:4326&outputFormat=csv",
        #                                                   "TIIRA_API_URL": "https://api.vayla.fi/trex/rajapinta/tiira/1.0/"
        #                                               },
        #                                               tags = trex_tags,
        #                                               securitygroups = [ lambda_securitygroup ],
        #                                               schedule = "15 0 * * ? *"
        #                                              )
        #                     )

        
        
        
        # l2 = NodejsLambdaFunction(self,
        #                      id = "testi2",
        #                      path = "lambda/testi2",
        #                      handler = "testi2.lambda_handler",
        #                      description = "Testilambdan kuvaus",
        #                      role = lambda_role,
        #                      props = LambdaProperties(vpc = vpc,
        #                                               timeout = 2, 
        #                                               environment = {
        #                                                   "target_bucket": target_bucket_name,
        #                                               },
        #                                               tags = None,
        #                                               securitygroups = [ lambda_securitygroup ],
        #                                               schedule = "0 10 20 * ? *"
        #                                              )
        #                     )



        # glue_sampo_oracle_connection = GlueJdbcConnection(self,
        #                         id = "sampo-jdbc-oracle-connection",
        #                         vpc = vpc,
        #                         security_groups = [ glue_securitygroup ],
        #                         properties = {
        #                             "JDBC_CONNECTION_URL": "jdbc:oracle:thin:@//<host>:<port>/<sid>",
        #                             "JDBC_DRIVER_CLASS_NAME": "oracle.jdbc.driver.OracleDriver",
        #                             "JDBC_DRIVER_JAR_URI": f"s3://{script_bucket_name}/drivers/oracle/ojdbc8.jar",
        #                             "SECRET_ID": f"db-sampo-oracle-{environment}"
        #                         })
        # g1 = PythonSparkGlueJob(self,
        #          id = "testi3", 
        #          path = "glue/testi3",
        #          index = "testi3.py",
        #          script_bucket = script_bucket,
        #          timeout_min = 1,
        #          description = "Glue jobin kuvaus",
        #          worker = "G 1X",
        #          version = None,
        #          role = glue_role,
        #          tags = None,
        #          arguments = None,
        #          connections = [ glue_sampo_oracle_connection.connection ],
        #          enable_spark_ui = False,
        #          schedule = "0 12 24 * ? *",
        #          schedule_description = "Normaali ajastus"
        # )


