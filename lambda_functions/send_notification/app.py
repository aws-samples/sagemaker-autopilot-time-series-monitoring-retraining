import json
import boto3
import os
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Initialize AWS clients
sns = boto3.client("sns")

# Get environment variable
sns_topic_arn = os.environ["SNS_TOPIC_ARN"]


def handler(event, context):
    try:
        # Extract relevant information from the event
        eval_result = event.get("eval_result")
        metric = event.get("metric").upper()
        average_rmse = event.get("average_rmse")
        threshold = event.get("threshold")
        date = event.get("date")
        best_candidate_name = event.get("best_candidate_name")
        best_candidate_metric_value = event.get("best_candidate_metric_value")

        # Construct the message
        subject = "Solar Power Model Daily Report"

        if eval_result == "YES":  # if the current model perform well
            message = (
                f"Hello, Today is {date}\n"
                + f"The current solar power forecasting model perform well with {metric}: {average_rmse}."
            )

        elif best_candidate_metric_value >= threshold:
            message = (
                f"Hello, Today is {date}\n"
                + f"The current solar power forecasting model perform with {metric}: {average_rmse}, which is higher than threshold: {threshold}\n\n"
                + f"Model is retrained.\n\n"
                + f"The Autopilot newly trained model {best_candidate_name} perform with {metric}: {best_candidate_metric_value}, which is still higher than threshold. \n\n"
                + f"Please manual retrain the model"
            )

        else:
            message = (
                f"Hello, Today is {date}\n"
                + f"The current solar power forecasting model perform with {metric}: {average_rmse}, which is higher than threshold: {threshold}\n\n"
                + f"Model is retrained.\n\n"
                + f"The Autopilot newly trained model {best_candidate_name} perform with {metric}: {best_candidate_metric_value}, which is lower than threshold. \n\n"
                + f"Please review the new model and choose if replace the current one."
            )

        # Publish the message to SNS
        response = sns.publish(TopicArn=sns_topic_arn, Message=message, Subject=subject)

        # Return the result
        return event

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
