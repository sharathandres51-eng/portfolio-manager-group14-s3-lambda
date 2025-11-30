import os
import json
import io
import csv
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

# env vars:
# TICKERS   = "AAPL,MSFT,GOOG"
# RANGE     = "1mo"   (valid: 1d,5d,1mo,3mo,6mo,1y,5y, max)
# INTERVAL  = "1d"    (valid: 1m,5m,15m,1h,1d,1wk,1mo)
# BUCKET_NAME
# PREFIX    = "raw-data/"

YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={range}&interval={interval}&events=div,splits"


def fetch_ticker_data(ticker: str, range_: str, interval: str):
    """
    Call Yahoo Finance chart API for a single ticker and return
    a list of rows: dict(date, open, high, low, close, adj_close, volume, ticker)
    """
    url = YF_CHART_URL.format(ticker=ticker, range=range_, interval=interval)
    logger.info(f"Fetching URL: {url}")

    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
    except HTTPError as e:
        logger.error(f"HTTP error for {ticker}: {e.code} {e.reason}")
        return []
    except URLError as e:
        logger.error(f"URL error for {ticker}: {e.reason}")
        return []

    try:
        data = json.loads(body)
        result = data["chart"]["result"][0]
    except Exception as e:
        logger.exception(f"Error parsing JSON for {ticker}")
        return []

    timestamps = result.get("timestamp", [])
    if not timestamps:
        logger.warning(f"No timestamps for {ticker}")
        return []

    quote = result["indicators"]["quote"][0]
    opens = quote.get("open", [])
    highs = quote.get("high", [])
    lows = quote.get("low", [])
    closes = quote.get("close", [])
    volumes = quote.get("volume", [])

    # adjclose is sometimes in a separate structure
    adj = result["indicators"].get("adjclose", [{}])[0].get("adjclose", [])

    rows = []
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "date": dt,
            "open": opens[i] if i < len(opens) else None,
            "high": highs[i] if i < len(highs) else None,
            "low": lows[i] if i < len(lows) else None,
            "close": closes[i] if i < len(closes) else None,
            "adj_close": adj[i] if i < len(adj) else None,
            "volume": volumes[i] if i < len(volumes) else None,
            "ticker": ticker,
        }
        rows.append(row)

    return rows


def lambda_handler(event, context):
    tickers_str = os.getenv("TICKERS", "AAPL,MSFT")
    range_ = os.getenv("RANGE", "1mo")
    interval = os.getenv("INTERVAL", "1d")
    bucket_name = os.getenv("BUCKET_NAME")
    prefix = os.getenv("PREFIX", "raw-data/")

    if not bucket_name:
        logger.error("BUCKET_NAME environment variable is required")
        raise ValueError("BUCKET_NAME environment variable is required")

    tickers = [t.strip().upper() for t in tickers_str.split(",") if t.strip()]
    logger.info(f"Tickers={tickers}, range={range_}, interval={interval}")

    all_rows = []

    for ticker in tickers:
        rows = fetch_ticker_data(ticker, range_, interval)
        if rows:
            all_rows.extend(rows)

    if not all_rows:
        logger.warning("No data collected for any ticker")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "No data collected"})
        }

    # Build CSV in memory
    csv_buffer = io.StringIO()
    fieldnames = ["date", "open", "high", "low", "close", "adj_close", "volume", "ticker"]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in all_rows:
        writer.writerow(row)

    # Generate S3 key with timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{prefix}yahoo_prices_{timestamp}.csv"

    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
        )
        logger.info(f"Uploaded CSV to s3://{bucket_name}/{s3_key}")
    except Exception:
        logger.exception("Error while uploading CSV to S3")
        raise

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Data fetched and stored successfully",
            "bucket": bucket_name,
            "key": s3_key,
            "tickers": tickers,
        }),
    }
