#Lambda 1: Data collection from Yahoo Finance API into CSV

import os
import json
import io
import csv
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import boto3

#setting up logging for CloudWatch visibility
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#initializing S3 client using boto3
s3 = boto3.client("s3")

#defining URL template for the Yahoo Finance API to fetch historical chart data
YF_CHART_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    "?range={range}&interval={interval}&events=div,splits"
)

#creating the function to download data (Open, High, Low, Close, and Volume = OHLCV) for a stock the API
def fetch_ticker_data(ticker: str, range_: str, interval: str):
    url = YF_CHART_URL.format(ticker=ticker, range=range_, interval=interval)  #formatting URL with ticker, range, and interval
    logger.info(f"Fetching URL: {url}")

    #mimiciking a browser request to avoid potential blocking by API
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(req, timeout=10) as resp:  #setting timeout
            body = resp.read().decode("utf-8")
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return []

    try:
        data = json.loads(body)
        result = data["chart"]["result"][0]   #navigating JSON structure to get relevant data
    except Exception as e:
        logger.error(f"Error parsing JSON for {ticker}: {e}")
        return []
    
    #extracting core data components
    timestamps = result.get("timestamp", [])
    if not timestamps:
        logger.warning(f"No timestamps for {ticker}")
        return []

    quote = result["indicators"]["quote"][0]
    adj = result["indicators"].get("adjclose", [{}])[0].get("adjclose", [])

    #creating dictionary
    rows = []
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")   #converting to readable date-time format
        rows.append({
            "date": dt,
            "open": quote.get("open", [None])[i],
            "high": quote.get("high", [None])[i],
            "low": quote.get("low", [None])[i],
            "close": quote.get("close", [None])[i],
            "adj_close": adj[i] if i < len(adj) else None,
            "volume": quote.get("volume", [None])[i],
            "ticker": ticker,
        })

    return rows

#setting handler to manage configuration, iterate through tickers, and handle S3 upload process
def lambda_handler(event, context):
    #loading parameters from lambda environment variables for flexibility
    tickers_str  = os.getenv("TICKERS")
    range_       = os.getenv("RANGE")  #time range for history
    interval     = os.getenv("INTERVAL")
    bucket_name  = os.getenv("BUCKET_NAME")  #target S3 bucket for data storage
    prefix       = os.getenv("PREFIX")  #folder path within bucket

    if not tickers_str:
        raise ValueError("TICKERS environment variable is required")
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable is required")
    if not prefix:
        raise ValueError("PREFIX environment variable is required")
    
    #making tickers into a python list
    tickers = [t.strip() for t in tickers_str.split(",") if t.strip()]

    #setting market benchmarks (S&P 500, Russell 2000)
    BENCHMARKS = ["^GSPC", "^RUT"]

    logger.info(f"[CONFIG] Tickers={tickers}, range={range_}, interval={interval}")
    #making unique timestamp for S3 file key
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    uploaded_files = []

    for ticker in tickers:
        rows = fetch_ticker_data(ticker, range_, interval)   #getting data
        if not rows:
            logger.warning(f"No data fetched for ticker {ticker}")
            continue

        #we optimize by building the CSV string in memory to avoid lambda's temporary disk constraints
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=[
            "date", "open", "high", "low", "close", "adj_close", "volume", "ticker"
        ])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

        #cleaning folder name from ^
        sanitized = ticker.replace("^", "")

        #separating stocks from benchmarks in S3
        if ticker in BENCHMARKS:
            folder = f"{prefix}benchmarks/{sanitized}/"
        else:
            folder = f"{prefix}stocks/{sanitized}/"

        #creating final S3 path
        s3_key = f"{folder}{sanitized}_{timestamp}.csv"

        #uploading in-memory CSV string to S3
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv",
            )
            uploaded_files.append(s3_key)
            logger.info(f"Uploaded → s3://{bucket_name}/{s3_key}")
        except Exception as e:
            logger.error(f"Error uploading {ticker}: {e}")
    
    #returning success code and list of uploaded files
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Data fetched for tickers",
            "uploaded_files": uploaded_files
        }),
    }
