import boto3
import logging

# Initialize AWS clients
sm = boto3.client("sagemaker")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    # Extract AutoML job name from the event
    auto_ml_job_name = event.get("auto_ml_job_name")

    # Check if AutoML job name is provided in the event
    if not auto_ml_job_name:
        return {
            "statusCode": 400,
            "body": "AutoML job name is missing in the event.",
        }

    try:
        # Describe the AutoML job to get its current status
        describe_response = sm.describe_auto_ml_job_v2(AutoMLJobName=auto_ml_job_name)
        job_run_status = describe_response["AutoMLJobStatus"]

        # Handle the case where the AutoML job has failed or is in a stopping state
        if job_run_status in ("Failed", "Stopped", "Stopping"):
            logger.error(f"Autopilot Job {auto_ml_job_name} failed or stopped.")
            return {
                "statusCode": 400,
                "body": f"Autopilot Job {auto_ml_job_name} failed or stopped. Please check the logs for more information.",
            }
        else:
            # Update the event with the job status
            event["job_run_status"] = job_run_status
            logger.info(
                f"Autopilot Job {auto_ml_job_name} is in {job_run_status} state."
            )
            # Return the result
            return event
    except Exception as e:
        # Handle any exceptions that occur during the process
        logger.error(f"Error occurred while describing AutoML job: {str(e)}")
        return {
            "statusCode": 500,
            "body": "Internal Server Error occurred while describing AutoML job.",
        }
