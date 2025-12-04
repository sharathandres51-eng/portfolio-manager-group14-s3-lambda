import os
import io
import json
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd
import numpy as np

#setting up logging for cloudwatch and local debugging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#initializing aws clients for s3 and dynamodb
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# retrieving dynamodb tab name from environment variables defined in lambda configuration
PREDICTIONS_TABLE = os.environ["PREDICTIONS_TABLE"]

#computing 30-day historical volatility
def compute_historical_volatility(df):
    #df must contain columns: date, close
    
    #sorting data by date to ensure returns and rolling metrics are calculated chronologically
    df = df.sort_values("date").reset_index(drop=True)

    #computing daily perc change in closing price, this gives us the daily returns aka the basis for volatility
    df["return"] = df["close"].pct_change()

    #using only last 30 trading days
    returns_30d = df["return"].tail(30)

    #we raise an error to prevent inaccurate calculations
    if returns_30d.isna().sum() > 5:
        raise ValueError("Not enough valid price points for volatility calculation.")

    #computing standard deviation of daily returns, core for historical volatility
    daily_vol = returns_30d.std()

    #annualizing daily volatility (multiplying by sq. root of 252 = approx. trading days in a year)
    #this makes the result comparable across different assets and timeframes
    annualized_vol = np.sqrt(252) * daily_vol

    return float(annualized_vol)


#Lambda Handler
def lambda_handler(event, context):
    try:
        #logging event for debugging and monitoring
        logger.info("EVENT RECEIVED:")
        logger.info(json.dumps(event))

        #extracting s3 event details from the trigger payload
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        logger.info(f"Triggered by s3://{bucket}/{key}")

        #ignoring files not in expected structure
        parts = key.split("/")
        if len(parts) < 4 or parts[0] != "market-data" or parts[1] != "stocks":
            return {"statusCode": 200}
        
        #extracting stock ticker from the s3 key
        ticker = parts[2].upper()

        #downloading csv file content from s3
        obj = s3.get_object(Bucket=bucket, Key=key)
        df_stock = pd.read_csv(io.BytesIO(obj["Body"].read()))

        #converting date column to datetime objects for accurate sorting
        df_stock["date"] = pd.to_datetime(df_stock["date"])
        df_stock = df_stock.sort_values("date")

        #checking if enough data for a valid 30-day calculation
        if len(df_stock) < 30:
            logger.warning(f"Not enough history for {ticker}. Need at least 40 rows.")
            return {"statusCode": 200}

        #compute 30 day historical volatility
        predicted_vol = compute_historical_volatility(df_stock)

        #prepare dynamodb entry
        table = dynamodb.Table(PREDICTIONS_TABLE)
        latest_date = df_stock["date"].max().strftime("%d.%m.%Y")

        #constructing item payload to be stored in dynamodb
        item = {
            "ticker": ticker,
            "date": latest_date,
            "predicted_volatility": str(predicted_vol),
            "timestamp": str(int(datetime.now(timezone.utc).timestamp())),
            "s3_key": key
        }

        #storing volatility prediction in dynamodb table
        table.put_item(Item=item)

        logger.info(f"[{ticker}] Volatility = {predicted_vol}")

        return {"statusCode": 200, "body": json.dumps({"prediction": item})}

    except Exception as e:
        logger.error(f"ERROR: {e}", exc_info=True)
        return {"statusCode": 500, "error": str(e)}
