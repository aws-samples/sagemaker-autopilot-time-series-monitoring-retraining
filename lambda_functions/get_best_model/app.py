import json
import boto3
import os
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
sm = boto3.client("sagemaker")

# Get environment variable
sm_role = os.environ["SM_ROLE"]


def handler(event, context):
    try:
        threshold = event.get("threshold")
        auto_ml_job_name = event.get("auto_ml_job_name")

        # timestamp_suffix = strftime("%Y%m%d-%H%M%S", gmtime())
        # transform_job_name = "transform-" + timestamp_suffix
        # print("TransformJobName: " + transform_job_name)

        # Describe the best candidate for the AutoML job
        best_candidate = sm.describe_auto_ml_job_v2(AutoMLJobName=auto_ml_job_name)[
            "BestCandidate"
        ]
        best_candidate_containers = best_candidate["InferenceContainers"]
        best_candidate_name = best_candidate["CandidateName"]
        best_candidate_metric_value = best_candidate["FinalAutoMLJobObjectiveMetric"][
            "Value"
        ]

        # Create a SageMaker model using the best candidate
        response = sm.create_model(
            ModelName=best_candidate_name,
            ExecutionRoleArn=sm_role,
            Containers=best_candidate_containers,
        )

        # if threshold > float(best_candidate_metric_value):
        #     new_eval_result = "YES"
        # else:
        #     new_eval_result = "No"

        # Update event with best model information
        event["best_candidate_name"] = best_candidate_name
        event["best_candidate_metric_value"] = float(best_candidate_metric_value)

        return event
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
