import json
import boto3
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Initialize AWS clients
sfn_client = boto3.client("stepfunctions")
ssm = boto3.client("ssm")

# Get environment variable for the destination bucket
state_machine_arn = os.environ.get("STATE_MACHINE_ARN")


def get_rmse_threshold():
    try:
        # Retrieve the performance threshold from rmse
        parameter = ssm.get_parameter(Name="rmse")
        # Access the parameter's value
        return float(parameter["Parameter"]["Value"])
    except Exception as e:
        logger.error(f"Failed to retrieve parameter: {e}")
        return None


def construct_input_event(bucket_name, hist_key, pred_key, date, threshold):
    return {
        "bucket_name": bucket_name,
        "hist_path": f"s3://{bucket_name}/{hist_key}",
        "hist_key": hist_key,
        "pred_key": pred_key,
        "date": date,
        "threshold": threshold,
        "metric": "RMSE",
        "autopilot_job_max_number": 1,
        "autopilot_output_path": f"s3://{bucket_name}/autopilot/train_output",
        "transform_output_path": f"s3://{bucket_name}/transform/output",
    }


def handler(event, context):
    try:
        # Parse the S3 object info from the event
        s3_event = event["Records"][0]["s3"]
        bucket_name = s3_event["bucket"]["name"]
        hist_key = s3_event["object"]["key"]
        pred_key = hist_key.replace("hist", "pred")
        date = hist_key.split("/")[-2]

        threshold = get_rmse_threshold()
        if threshold is None:
            raise ValueError("Failed to retrieve RMSE threshold from SSM parameter.")

        input_event = construct_input_event(
            bucket_name, hist_key, pred_key, date, threshold
        )

        # Start execution of the state machine
        response = sfn_client.start_execution(
            stateMachineArn=state_machine_arn, input=json.dumps(input_event)
        )

        return {
            "statusCode": 200,
            "body": json.dumps("State machine execution started successfully!"),
        }
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps("Internal Server Error occurred."),
        }
