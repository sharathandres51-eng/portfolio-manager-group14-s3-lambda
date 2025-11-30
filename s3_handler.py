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

# Yahoo Finance API URL template
YF_CHART_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    "?range={range}&interval={interval}&events=div,splits"
)

def fetch_ticker_data(ticker: str, range_: str, interval: str):
    """Fetch OHLCV data for one ticker from Yahoo Finance."""
    url = YF_CHART_URL.format(ticker=ticker, range=range_, interval=interval)
    logger.info(f"Fetching URL: {url}")

    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return []

    try:
        data = json.loads(body)
        result = data["chart"]["result"][0]
    except Exception as e:
        logger.error(f"Error parsing JSON for {ticker}: {e}")
        return []

    timestamps = result.get("timestamp", [])
    if not timestamps:
        logger.warning(f"No timestamps for {ticker}")
        return []

    quote = result["indicators"]["quote"][0]
    adj = result["indicators"].get("adjclose", [{}])[0].get("adjclose", [])

    rows = []
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
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


def lambda_handler(event, context):
    # -------- Environment Variables --------
    tickers_str  = os.getenv("TICKERS")
    range_       = os.getenv("RANGE")
    interval     = os.getenv("INTERVAL")
    bucket_name  = os.getenv("BUCKET_NAME")
    prefix       = os.getenv("PREFIX")

    # Validation
    if not tickers_str:
        raise ValueError("TICKERS environment variable is required")
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable is required")
    if not prefix:
        raise ValueError("PREFIX environment variable is required")

    tickers = [t.strip() for t in tickers_str.split(",") if t.strip()]

    logger.info(f"[CONFIG] Tickers={tickers}, range={range_}, interval={interval}")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    uploaded_files = []

    for ticker in tickers:
        rows = fetch_ticker_data(ticker, range_, interval)
        if not rows:
            logger.warning(f"No data fetched for ticker {ticker}")
            continue

        # Build CSV for this ticker
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=[
            "date", "open", "high", "low", "close", "adj_close", "volume", "ticker"
        ])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

        # Sanitize ticker folder name
        sanitized = ticker.replace("^", "")

        # Final S3 key (no date subfolders)
        s3_key = f"{prefix}{sanitized}/{sanitized}_{timestamp}.csv"

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

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Data fetched for tickers",
            "uploaded_files": uploaded_files
        }),
    }
