from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sagemaker as sagemaker,
    aws_sns as sns,
    aws_sns_subscriptions as sns_sub,
    aws_ssm as ssm,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
import aws_cdk as core


from constructs import Construct


class ResourceStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Retrieve the email address from the context variables
        email_address = self.node.try_get_context("email")

        # Create an S3 bucket for model and data
        bucket = s3.Bucket(
            self,
            "Bucket",
            bucket_name=f"solar-power-forecast-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,  # Consider using RETAIN for production
        )

        # Create SSM parameter for storing threshold
        ssm.StringParameter(
            self,
            "ThresholdStringParameter",
            parameter_name="rmse",
            string_value="50",
        )

        # Lambda execution role with AmazonS3FullAccess policy
        lambda_role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSStepFunctionsFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonDynamoDBFullAccess"
                ),
            ],
        )

        # Additional policy for Lambda role
        additional_policy_statement = iam.PolicyStatement(
            actions=["sns:Publish", "ssm:GetParameter"], resources=["*"]
        )
        lambda_role.add_to_policy(additional_policy_statement)

        # Lambda function to evaluate the yesterday prediction result
        perform_evaluation = lambda_.Function(
            self,
            "perform_evaluation",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="app.handler",
            code=lambda_.Code.from_asset(
                "lambda_functions/perform_evaluation",
            ),
            role=lambda_role,
            timeout=Duration.minutes(3),
        )

        # SageMaker execution role with necessary policies
        sm_role = iam.Role(
            self,
            "SaegMakerExecutionRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"
                ),
            ],
        )

        # Lambda function to execute Retraining Step Function
        start_retrain = lambda_.Function(
            self,
            "start_retrain",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="app.handler",
            code=lambda_.Code.from_asset(
                "lambda_functions/start_retrain",
            ),
            role=lambda_role,
            environment={
                "SM_ROLE": sm_role.role_arn
            },  # Pass SageMaker role ARN as environment variable
        )

        # Lambda function to check the status of the Autopilot V2 training job
        check_status = lambda_.Function(
            self,
            "check_status",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="app.handler",
            code=lambda_.Code.from_asset("lambda_functions/check_status"),
            role=lambda_role,
        )

        # Lambda function to get the best model
        get_best_model = lambda_.Function(
            self,
            "get_best_model",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="app.handler",
            code=lambda_.Code.from_asset("lambda_functions/get_best_model"),
            role=lambda_role,
            environment={
                "SM_ROLE": sm_role.role_arn
            },  # Pass SageMaker role ARN as environment variable
        )

        # Define an SNS topic for notifications
        sns_topic = sns.Topic(self, "sns_topic", display_name="Notification to DS")

        # Add subscription to the SNS topic for email notifications
        sns_topic.add_subscription(sns_sub.EmailSubscription(email_address))

        # Lambda function to send notification to data scientist
        send_notification = lambda_.Function(
            self,
            "send_notification",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="app.handler",
            code=lambda_.Code.from_asset("lambda_functions/send_notification"),
            environment={
                "SNS_TOPIC_ARN": sns_topic.topic_arn
            },  # Pass SNS topic ARN as environment variable
            role=lambda_role,
        )

        # IAM role for Step Functions
        sfn_role = iam.Role(
            self,
            "StepFunctionExecutionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "StepFunctionExecutionPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["lambda:InvokeFunction"],
                            resources=["*"],
                        )
                    ]
                )
            },
        )

        # Define Step Functions State Machine
        perform_evaluation_step = tasks.LambdaInvoke(
            self,
            "Evaluate Model Performance",
            lambda_function=perform_evaluation,
            output_path="$.Payload",
        )

        start_retrain_step = tasks.LambdaInvoke(
            self,
            "No, Start New AutoML Job",
            lambda_function=start_retrain,
            output_path="$.Payload",
        )

        # Choice state to determine whether to start AutopilotV2 for new model or not
        retrain_choice_state = sfn.Choice(self, "Is Evaluation Passed?")

        # If evaluation passed, keep the current model, otherwise start a new AutopilotV2 job
        success_step = sfn.Pass(self, "Yes, Keep Current Model")
        retrain_choice_state.when(
            sfn.Condition.string_equals("$.eval_result", "YES"), success_step
        )
        retrain_choice_state.otherwise(start_retrain_step)

        # Wait for 5 minutes
        wait_state = sfn.Wait(
            self, "Wait 5 Minutes", time=sfn.WaitTime.duration(Duration.minutes(5))
        )

        # Task to check the status of the AutopilotV2 job
        check_status_task = tasks.LambdaInvoke(
            self,
            "Check AutoML Status",
            lambda_function=check_status,
            output_path="$.Payload",
        )

        # Task to get the best model
        get_best_model_step = tasks.LambdaInvoke(
            self,
            "AutoML Completed. Get the Best Model",
            lambda_function=get_best_model,
            output_path="$.Payload",
        )

        # Decision based on the new training job status
        status_check_choice = sfn.Choice(self, "Is AutoML Complete?")
        status_check_choice.when(
            sfn.Condition.string_equals("$.job_run_status", "Completed"),
            get_best_model_step,
        )

        status_check_choice.otherwise(wait_state)

        start_retrain_step.next(wait_state).next(check_status_task).next(
            status_check_choice
        )

        send_notification_step = tasks.LambdaInvoke(
            self,
            "Send Email for Model Review",
            lambda_function=send_notification,
            output_path="$.Payload",
        )

        get_best_model_step.next(send_notification_step)
        success_step.next(send_notification_step)

        definition = perform_evaluation_step.next(retrain_choice_state)

        state_machine = sfn.StateMachine(
            self,
            "StateMachine",
            definition=definition,
            state_machine_name="Evaluation-Retraining",
        )

        # Lambda function to execute Step Function by S3 event
        execute_sfn = lambda_.Function(
            self,
            "execute_sfn",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="app.handler",
            code=lambda_.Code.from_asset("lambda_functions/execute_sfn"),
            environment={"STATE_MACHINE_ARN": state_machine.state_machine_arn},
            role=lambda_role,
        )

        # S3 event to trigger Lambda
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(execute_sfn),
            s3.NotificationKeyFilter(prefix="data/hist"),
        )
