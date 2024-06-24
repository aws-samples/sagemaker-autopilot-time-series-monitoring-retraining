import json
import boto3
import logging
from io import StringIO
from datetime import datetime
from math import sqrt

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Initialize AWS clients
s3 = boto3.client("s3")


# Helper function to parse CSV from S3 into a list of dictionaries
def load_csv_from_s3(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = StringIO(response["Body"].read().decode("utf-8"))
        lines = content.readlines()
        headers = lines[0].strip().split(",")
        data = [dict(zip(headers, line.strip().split(","))) for line in lines[1:]]
        for item in data:
            item["timestamp"] = datetime.strptime(
                item["timestamp"], "%Y-%m-%d %H:%M:%S"
            )
        return data
    except Exception as e:
        logger.error(f"Failed to load CSV from S3: {e}")
        raise


# Function to calculate RMSE
def calculate_rmse(actuals, predictions):
    try:
        n = len(actuals)
        mse = (
            sum(
                (float(act) - float(pred)) ** 2
                for act, pred in zip(actuals, predictions)
            )
            / n
        )
        return sqrt(mse)
    except Exception as e:
        logger.error(f"Failed to calculate RMSE: {e}")
        raise


def handler(event, context):
    try:
        # Parameters from the event
        threshold = event.get("threshold")
        metric = event.get("metric", "RMSE").upper()
        bucket_name = event.get("bucket_name")
        hist_key = event.get("hist_key")
        pred_key = event.get("pred_key")
        date = event.get("date")
        target_date = datetime.strptime(date, "%Y-%m-%d").date()

        # Load data
        hist_data = load_csv_from_s3(bucket_name, hist_key)
        pred_data = load_csv_from_s3(bucket_name, pred_key)

        # Filter data by target date
        filtered_hist = [
            row for row in hist_data if row["timestamp"].date() == target_date
        ]
        filtered_pred = [
            row for row in pred_data if row["timestamp"].date() == target_date
        ]

        # Match records by 'id' and 'timestamp'
        merged_data = {}
        for h in filtered_hist:
            for p in filtered_pred:
                if h["id"] == p["id"] and h["timestamp"] == p["timestamp"]:
                    if h["id"] in merged_data:
                        merged_data[h["id"]].append((h["actual_power"], p["p50"]))
                    else:
                        merged_data[h["id"]] = [(h["actual_power"], p["p50"])]

        # Calculate RMSE per ID
        results = {}
        total_rmse = 0
        count_ids = 0
        if metric == "RMSE": # Replace this code block if you want to use other metrics
            for id, pairs in merged_data.items():
                actuals, predictions = zip(*pairs)
                rmse = calculate_rmse(actuals, predictions)
                results[id] = rmse
                total_rmse += rmse
                count_ids += 1

            average_rmse = total_rmse / count_ids if count_ids > 0 else 0
            result_text = f"Average RMSE for all IDs: {average_rmse}"
        else:
            return {
                "statusCode": 400,
                "body": 'Invalid metric specified. Please use "RMSE".',
            }

        # # Select your DynamoDB table
        # table = dynamodb.Table('model-daily-performance')

        # # Define the item to add to the table
        # item = {
        #     'model_id': 'solar-power-forecasting',
        #     'version_id':formatted_time,
        #     'RMSE': '5'
        # }

        # Determine evaluation result based on threshold
        eval_result = "YES" if threshold > average_rmse else "NO"

        # Update event with calculated values
        event["average_rmse"] = average_rmse
        event["eval_result"] = eval_result

        # Return the result
        return event
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
