from aws_cdk import (
    Stack,
    aws_ec2,
    aws_s3,
    aws_iam,
    aws_secretsmanager,
    RemovalPolicy
)


from aws_cdk.aws_iam import ServicePrincipal

from constructs import Construct

from analytiikka_rata.helper_lambda import *
from analytiikka_rata.helper_glue import *


# TODO: dev/prod parametrien haku jostain



"""
Palvelut stack

"""
class AnalytiikkaRataServicesStack(Stack):

    def __init__(self,
                 scope: Construct, 
                 construct_id: str,
                 environment: str,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        """
        Yhteiset arvot projektilta ja ympäristön mukaan
        """
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
        lambda_role_name = self.node.try_get_context('lambda_role_name')
        # Yhteiskäyttöinen securoty group lambdoille. Sallii akiken koska tilin yhteydet on rajattu operaattorin toimesta
        lambda_security_group_name = self.node.try_get_context('lambda_security_group_name')
        # Yhteiskäyttöinen rooli glue- jobeille
        glue_role_name = self.node.try_get_context('glue_role_name')
        # Yhteiskäyttöinen security group glue- jobeille. Sallii akiken koska tilin yhteydet on rajattu operaattorin toimesta
        glue_security_group_name = self.node.try_get_context('glue_security_group_name')

        # print(f"services {environment}: project = '{projectname}'")
        # print(f"services {environment}: account = '{self.account}'")
        # print(f"services {environment}: region = '{self.region}'")
        # print(f"services {environment}: properties = '{properties}'")

        # Lookup: VPC
        vpc_name = properties["vpc_name"]
        vpc = aws_ec2.Vpc.from_lookup(self, "VPC", vpc_name = vpc_name)
        # print(f"services {environment}: vpc = '{vpc}'")
        
        # Lookup: Lambda security group
        lambda_securitygroup = aws_ec2.SecurityGroup.from_lookup_by_name(self, "LambdaSecurityGroup", security_group_name = lambda_security_group_name, vpc = vpc)
        # Lookup: Lambda rooli
        lambda_role = aws_iam.Role.from_role_arn(self, "LambdaRole", f"arn:aws:iam::{self.account}:role/{lambda_role_name}", mutable = False)
        # Lookup: Glue security group
        glue_securitygroup = aws_ec2.SecurityGroup.from_lookup_by_name(self, "GlueSecurityGroup", security_group_name = glue_security_group_name, vpc = vpc)
        # Lookup: Glue rooli
        glue_role = aws_iam.Role.from_role_arn(self, "GlueRole", f"arn:aws:iam::{self.account}:role/{glue_role_name}", mutable = False)




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
        #                                               timeout = 2, 
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




