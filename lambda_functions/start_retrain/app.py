import json
import boto3
import os
import logging
from time import gmtime, strftime

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
sm = boto3.client("sagemaker")

# Get environment variable
sm_role = os.environ["SM_ROLE"]


def handler(event, context):
    try:

        # Extract relevant information from the event
        bucket_name = event.get("bucket_name")
        metric = event.get("metric")
        hist_path = event.get("hist_path")
        autopilot_job_max_number = event.get("autopilot_job_max_number")
        autopilot_output_path = event.get("autopilot_output_path")

        # Generate a unique name for the AutoML job
        timestamp_suffix = strftime("%Y%m%d-%H%M%S", gmtime())
        auto_ml_job_name = "ts-" + timestamp_suffix

        # Define input data configuration
        input_data_config = [
            {
                "ChannelType": "training",
                "ContentType": "text/csv;header=present",
                "CompressionType": "None",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": hist_path,
                    }
                },
            }
        ]

        # Define output data configuration
        output_data_config = {"S3OutputPath": autopilot_output_path}

        # Define optimization metric configuration
        optimizaton_metric_config = {"MetricName": metric}

        # Define AutoML problem type configuration for time series forecasting
        automl_problem_type_config = {
            "TimeSeriesForecastingJobConfig": {
                "CompletionCriteria": {"MaxCandidates": autopilot_job_max_number},
                "ForecastFrequency": "15min",
                "ForecastHorizon": 96,
                "ForecastQuantiles": ["p50"],
                "TimeSeriesConfig": {
                    "TargetAttributeName": "actual_power",
                    "TimestampAttributeName": "timestamp",
                    "ItemIdentifierAttributeName": "id",
                },
            },
        }

        # Create the AutoML job
        sm.create_auto_ml_job_v2(
            AutoMLJobName=auto_ml_job_name,
            AutoMLJobInputDataConfig=input_data_config,
            OutputDataConfig=output_data_config,
            AutoMLProblemTypeConfig=automl_problem_type_config,
            AutoMLJobObjective=optimizaton_metric_config,
            RoleArn=sm_role,
        )

        # input_event = {
        #     "bucket_name": bucket_name,
        #     "hist_key": hist_key,
        #     "pred_key": pred_key,
        #     "date": date,
        #     "metric": metric,
        #     "average_rmse": average_rmse,
        #     "eval_result": eval_result,
        #     "auto_ml_job_name":auto_ml_job_name
        # }

        # Update the event with the AutoML job name
        event["auto_ml_job_name"] = auto_ml_job_name

        # Return the result
        return event

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
