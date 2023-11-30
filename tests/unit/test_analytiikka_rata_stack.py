import aws_cdk as core
import aws_cdk.assertions as assertions

from analytiikka_rata.analytiikka_rata_stack import AnalytiikkaRataStack

# example tests. To run these tests, uncomment this file along with the example
# resource in analytiikka_rata/analytiikka_rata_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AnalytiikkaRataStack(app, "analytiikka-rata")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
