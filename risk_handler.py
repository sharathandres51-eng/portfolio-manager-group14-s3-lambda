import os
import io
import json
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd
import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# ----------------------------------------------
# Environment Variables
# ----------------------------------------------
PREDICTIONS_TABLE = os.environ["PREDICTIONS_TABLE"]

# ----------------------------------------------
# Compute 30-day historical volatility
# ----------------------------------------------
def compute_historical_volatility(df):
    """
    df must contain columns: date, close
    """

    # Sort by date
    df = df.sort_values("date").reset_index(drop=True)

    # Compute daily returns
    df["return"] = df["close"].pct_change()

    # Use last 30 trading days
    returns_30d = df["return"].tail(30)

    if returns_30d.isna().sum() > 5:
        raise ValueError("Not enough valid price points for volatility calculation.")

    # Compute standard deviation of daily returns
    daily_vol = returns_30d.std()

    # Annualize volatility
    annualized_vol = np.sqrt(252) * daily_vol

    return float(annualized_vol)


# ----------------------------------------------
# Lambda Handler
# ----------------------------------------------
def lambda_handler(event, context):
    try:
        logger.info("EVENT RECEIVED:")
        logger.info(json.dumps(event))

        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        logger.info(f"Triggered by s3://{bucket}/{key}")

        # Ignore files not in expected structure
        parts = key.split("/")
        if len(parts) < 4 or parts[0] != "market-data" or parts[1] != "stocks":
            return {"statusCode": 200}

        ticker = parts[2].upper()

        # Load stock CSV
        obj = s3.get_object(Bucket=bucket, Key=key)
        df_stock = pd.read_csv(io.BytesIO(obj["Body"].read()))

        # Parse & sort dates
        df_stock["date"] = pd.to_datetime(df_stock["date"])
        df_stock = df_stock.sort_values("date")

        if len(df_stock) < 30:
            logger.warning(f"Not enough history for {ticker}. Need at least 40 rows.")
            return {"statusCode": 200}

        # Compute volatility
        predicted_vol = compute_historical_volatility(df_stock)

        # Prepare DynamoDB entry
        table = dynamodb.Table(PREDICTIONS_TABLE)
        latest_date = df_stock["date"].max().strftime("%d.%m.%Y")

        item = {
            "ticker": ticker,
            "date": latest_date,
            "predicted_volatility": str(predicted_vol),
            "timestamp": str(int(datetime.now(timezone.utc).timestamp())),
            "s3_key": key
        }

        # Store in DynamoDB
        table.put_item(Item=item)

        logger.info(f"[{ticker}] Volatility = {predicted_vol}")

        return {"statusCode": 200, "body": json.dumps({"prediction": item})}

    except Exception as e:
        logger.error(f"ERROR: {e}", exc_info=True)
        return {"statusCode": 500, "error": str(e)}
