import os
import io
import json
import logging
import pickle
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
MODEL_BUCKET = os.environ["MODEL_BUCKET"]
MODEL_KEY = os.environ["MODEL_KEY"]
PREDICTIONS_TABLE = os.environ["PREDICTIONS_TABLE"]
SP500_PREFIX = os.environ["SP500_PREFIX"]

MODEL_CACHE = {
    "loaded": False,
    "model": None,
    "feature_columns": None,
}

# ----------------------------------------------
# Load ML MODEL from S3
# ----------------------------------------------
def load_model_from_s3():
    global MODEL_CACHE

    if MODEL_CACHE["loaded"]:
        return MODEL_CACHE["model"], MODEL_CACHE["feature_columns"]

    local_path = "/tmp/volatility_model.pkl"
    if not os.path.exists(local_path):
        logger.info(f"Downloading model from s3://{MODEL_BUCKET}/{MODEL_KEY}")
        s3.download_file(MODEL_BUCKET, MODEL_KEY, local_path)

    with open(local_path, "rb") as f:
        pkg = pickle.load(f)

    MODEL_CACHE["model"] = pkg["model"]
    MODEL_CACHE["feature_columns"] = pkg["feature_columns"]
    MODEL_CACHE["loaded"] = True

    return MODEL_CACHE["model"], MODEL_CACHE["feature_columns"]


# ----------------------------------------------
# Load latest S&P 500 benchmark file
# ----------------------------------------------
def get_latest_sp500_csv(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])

    if not contents:
        raise RuntimeError("No benchmark files found.")

    latest = max(contents, key=lambda x: x["LastModified"])
    key = latest["Key"]

    logger.info(f"Using S&P500 file: {key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


# ----------------------------------------------
# Feature engineering (matches training)
# ----------------------------------------------
def compute_features(df_ticker, df_sp):

    WINDOW = 30
    df = df_ticker.sort_values("date").set_index("date")

    if "adj_close" not in df.columns or df["adj_close"].isna().all():
        df["adj_close"] = df["close"]

    df["return"] = df["adj_close"].pct_change()

    df["volatility_30d"] = df["return"].rolling(WINDOW).std()
    df["momentum_30d"] = df["adj_close"].pct_change(WINDOW)

    roll_max = df["adj_close"].rolling(WINDOW).max()
    df["max_drawdown_30d"] = (df["adj_close"] - roll_max) / roll_max

    df["prev_close"] = df["adj_close"].shift(1)
    df["tr"] = df.apply(
        lambda r: max(
            r["high"] - r["low"],
            abs(r["high"] - r["prev_close"]),
            abs(r["low"] - r["prev_close"])
        ),
        axis=1
    )
    df["atr_30d"] = df["tr"].rolling(WINDOW).mean()

    df["avg_volume_30d"] = df["volume"].rolling(WINDOW).mean()
    df["volume_volatility_30d"] = df["volume"].rolling(WINDOW).std()

    # ---- Benchmark joins ----
    sp = df_sp.copy()
    if "adj_close" not in sp.columns:
        sp["adj_close"] = sp["close"]

    sp["return_sp"] = sp["adj_close"].pct_change()
    sp = sp.set_index("date")

    combined = df[["return"]].join(sp[["return_sp"]], how="left")

    df["covar"] = combined["return"].rolling(WINDOW).cov(combined["return_sp"])
    df["var_sp"] = combined["return_sp"].rolling(WINDOW).var()

    df["beta_30d"] = df["covar"] / df["var_sp"]

    df["range_ratio"] = (df["high"] - df["low"]) / df["adj_close"]

    df["up"] = (df["return"] > 0).astype(int)
    df["down"] = (df["return"] < 0).astype(int)
    df["up_down_ratio"] = df["up"].rolling(WINDOW).sum() / df["down"].rolling(WINDOW).sum()

    # Final columns
    feature_cols = [
        "volatility_30d",
        "momentum_30d",
        "max_drawdown_30d",
        "atr_30d",
        "avg_volume_30d",
        "volume_volatility_30d",
        "beta_30d",
        "range_ratio",
        "up_down_ratio",
    ]

    features = df[feature_cols].copy()

    # Safe NaN handling
    features = features.fillna({
        "beta_30d": 0,
        "momentum_30d": 0,
        "max_drawdown_30d": 0,
        "atr_30d": features["atr_30d"].mean(),
        "avg_volume_30d": features["avg_volume_30d"].mean(),
        "volume_volatility_30d": 0,
        "range_ratio": 0,
        "up_down_ratio": 1,
    })

    return features.tail(1)


# ----------------------------------------------
# Lambda Handler
# ----------------------------------------------
def lambda_handler(event, context):
    logger.info("EVENT RECEIVED:")
    logger.info(json.dumps(event))

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info(f"Triggered by: s3://{bucket}/{key}")

    parts = key.split("/")

    if len(parts) < 4 or parts[0] != "market-data" or parts[1] != "stocks":
        return {"statusCode": 200}

    ticker = parts[2].upper()

    obj = s3.get_object(Bucket=bucket, Key=key)
    df_stock = pd.read_csv(io.BytesIO(obj["Body"].read()))
    df_stock["date"] = pd.to_datetime(df_stock["date"])
    df_stock = df_stock.sort_values("date")

    df_recent = df_stock.tail(30)

    if len(df_recent) < 30:
        logger.warning(f"[ERROR] Only {len(df_recent)} rows for {ticker}")
        return {"statusCode": 200}

    df_sp = get_latest_sp500_csv(bucket, SP500_PREFIX).tail(120)

    df_features = compute_features(df_recent, df_sp)

    if df_features.empty:
        logger.warning("No features generated.")
        return {"statusCode": 200}

    latest_date = df_features.index.max()
    X = df_features.loc[[latest_date]]

    model, feature_cols = load_model_from_s3()
    X = X.reindex(columns=feature_cols)

    predicted_vol = float(model.predict(X)[0])
    logger.info(f"[{ticker}] Predicted 30d volatility = {predicted_vol}")

    table = dynamodb.Table(PREDICTIONS_TABLE)

    item = {
        "ticker": ticker,
        "date": latest_date.strftime("%d.%m.%Y"),
        "predicted_volatility": str(predicted_vol),
        "timestamp": str(int(datetime.now(timezone.utc).timestamp())),
        "s3_key": key
    }

    table.put_item(Item=item)

    return {"statusCode": 200, "body": json.dumps({"prediction": item})}
