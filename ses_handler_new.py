import os
import json
import time
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# ----------------- Configuration ----------------- #

CLIENT_TABLE_NAME      = os.getenv("CLIENT_TABLE", "Client_risk_tolerance")
PRED_TABLE_NAME        = os.getenv("PREDICTIONS_TABLE", "PredictedVolatility")
VOL_TOL_DEFAULT        = float(os.getenv("VOL_TOLERANCE_DEFAULT", "0.10"))

SES_REGION             = os.getenv("SES_REGION", "us-east-1")
EMAIL_SUBJECT          = os.getenv("EMAIL_SUBJECT", "Daily Portfolio Volatility Update")
SENDER_EMAIL           = os.getenv("SENDER_EMAIL")

# NEW: cooldown in seconds to avoid multiple emails per client in a short time
EMAIL_COOLDOWN_SECONDS = int(os.getenv("EMAIL_COOLDOWN_SECONDS", "300"))

dynamodb = boto3.resource("dynamodb")
client_table = dynamodb.Table(CLIENT_TABLE_NAME)
pred_table   = dynamodb.Table(PRED_TABLE_NAME)

ses = boto3.client("ses", region_name=SES_REGION)
deserializer = TypeDeserializer()


# ----------------- Helper functions ----------------- #

def ddb_stream_image_to_python(ddb_item: dict) -> dict:
    """Convert a DynamoDB Stream image to plain Python types."""
    return {k: deserializer.deserialize(v) for k, v in ddb_item.items()}


def to_float(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return float(x)
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def to_decimal(x):
    """Safe float -> Decimal for DynamoDB."""
    if x is None:
        return None
    return Decimal(str(x))


def pct_str(x):
    x = to_float(x)
    return f"{x:.2%}" if x is not None else "N/A"


def get_vol_tolerance_for_client(client: dict) -> float:
    """
    Option 3 (hybrid):
    - if client has volTolerance -> use it
    - else -> use VOL_TOL_DEFAULT
    """
    vt = client.get("volTolerance")
    if vt is not None:
        vt = to_float(vt)
        if vt is not None:
            return vt
    return VOL_TOL_DEFAULT


def get_latest_vol_for_ticker(ticker: str):
    """
    Query PredictedVolatility for the latest item for this ticker.
    Assumes PK = ticker, SK = date (String), newest date = latest.
    """
    if not ticker:
        return None

    resp = pred_table.query(
        KeyConditionExpression=Key("ticker").eq(ticker),
        ScanIndexForward=False,  # newest date first
        Limit=1
    )
    items = resp.get("Items", [])
    if not items:
        return None

    return to_float(items[0].get("predicted_volatility"))


def compute_portfolio_vol(holdings: list):
    """
    Compute quantity-weighted portfolio volatility and return:
    - holdings_with_vol: [{ticker, quantity, vol}, ...]
    - portfolio_vol: float or None
    """
    holdings_with_vol = []
    total_qty = 0.0
    weighted_sum = 0.0

    for h in holdings or []:
        ticker = h.get("ticker")
        qty = to_float(h.get("quantity"))
        if not ticker or qty is None or qty <= 0:
            continue

        vol = get_latest_vol_for_ticker(ticker)
        holdings_with_vol.append({
            "ticker": ticker,
            "quantity": qty,
            "vol": vol
        })

        if vol is not None:
            total_qty += qty
            weighted_sum += qty * vol

    if total_qty > 0:
        portfolio_vol = weighted_sum / total_qty
    else:
        portfolio_vol = None

    return holdings_with_vol, portfolio_vol


def build_email_bodies(
    client: dict,
    holdings_with_vol: list,
    portfolio_vol: float,
    target_vol: float,
    lower_band: float,
    upper_band: float,
    within_band: bool
):
    client_id = client.get("ClientID", "N/A")
    name      = client.get("name", "Client")
    email     = client.get("email", "N/A")

    status_text = "WITHIN" if within_band else "OUTSIDE"
    status_color = "green" if within_band else "red"

    # ---------- TEXT BODY ---------- #
    lines = [
        f"Hello {name},",
        "",
        f"This is your daily portfolio volatility update for client ID {client_id}.",
        "",
        "Holdings (predicted 30-day volatility):"
    ]

    if holdings_with_vol:
        lines.append("  Ticker   Qty    Predicted Vol")
        for h in holdings_with_vol:
            lines.append(
                f"  {h['ticker']:<7} {int(h['quantity']):<5} {pct_str(h['vol'])}"
            )
    else:
        lines.append("  (No holdings or no volatility data available.)")

    lines.extend([
        "",
        f"Portfolio predicted 30-day volatility: {pct_str(portfolio_vol)}",
        f"Target volatility:                    {pct_str(target_vol)}",
        f"Accepted range:                       {pct_str(lower_band)} – {pct_str(upper_band)}",
        "",
        f"Risk status: You are currently {status_text} your agreed volatility band.",
        "",
        "Best regards,",
        "Your Portfolio Monitoring System"
    ])

    text_body = "\n".join(lines)

    # ---------- HTML BODY ---------- #
    if holdings_with_vol:
        rows = []
        for h in holdings_with_vol:
            rows.append(
                f"<tr>"
                f"<td>{h['ticker']}</td>"
                f"<td style='text-align:right;'>{int(h['quantity'])}</td>"
                f"<td style='text-align:right;'>{pct_str(h['vol'])}</td>"
                f"</tr>"
            )
        holdings_html = (
            "<table border='1' cellpadding='4' cellspacing='0'>"
            "<tr><th>Ticker</th><th>Qty</th><th>Predicted 30d Vol</th></tr>"
            + "".join(rows) +
            "</table>"
        )
    else:
        holdings_html = "<p>No holdings or volatility data available.</p>"

    html_body = f"""
    <html>
    <body style="font-family: Arial; font-size:14px;">
        <p>Hello {name},</p>
        <p>This is your daily portfolio volatility update for client ID <b>{client_id}</b>.</p>

        <h3>Holdings (predicted 30-day volatility)</h3>
        {holdings_html}

        <h3>Portfolio volatility summary</h3>
        <p><b>Portfolio predicted 30-day volatility:</b> {pct_str(portfolio_vol)}<br/>
           <b>Target volatility:</b> {pct_str(target_vol)}<br/>
           <b>Accepted range:</b> {pct_str(lower_band)} – {pct_str(upper_band)}<br/>
           <b>Status:</b> <span style="color:{status_color};">{status_text} band</span>
        </p>

        <p>Best regards,<br/><i>Your Portfolio Monitoring System</i></p>
    </body>
    </html>
    """

    return text_body, html_body


def send_email(recipient: str, text_body: str, html_body: str):
    if not recipient:
        print("No recipient email found – skipping.")
        return

    if not SENDER_EMAIL:
        print("SENDER_EMAIL not configured – skipping send.")
        return

    try:
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [recipient]},
            Message={
                "Subject": {"Data": EMAIL_SUBJECT},
                "Body": {
                    "Text": {"Data": text_body},
                    "Html": {"Data": html_body}
                }
            }
        )
        print(f"Email sent to {recipient}. MessageId: {response['MessageId']}")
    except ClientError as e:
        print("SES error:", e.response["Error"]["Message"])


# ------------- NEW: /tmp-based dedup per client ------------- #

def should_send_email_for_client(client_id: str) -> bool:
    """
    Deduplicate across multiple Lambda invocations using /tmp.
    Only send if we haven't emailed this client in the last
    EMAIL_COOLDOWN_SECONDS.
    """
    if not client_id:
        return True  # no ID, no dedup possible

    path = f"/tmp/email_sent_{client_id}"

    try:
        if os.path.exists(path):
            last_mtime = os.path.getmtime(path)
            if time.time() - last_mtime < EMAIL_COOLDOWN_SECONDS:
                print(f"Skipping email for {client_id}: cooldown not expired.")
                return False

        # Update marker file (or create if missing)
        with open(path, "w") as f:
            f.write(str(time.time()))
    except Exception as e:
        # If anything goes wrong, fail open (better to send than silently drop)
        print(f"Error in dedup for {client_id}: {e}")

    return True


# ----------------- Main Lambda handler ----------------- #

def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    # 1. Collect all tickers updated in this stream batch
    updated_tickers = set()

    for record in event.get("Records", []):
        if record.get("eventName") not in ("INSERT", "MODIFY"):
            continue

        new_image = record.get("dynamodb", {}).get("NewImage")
        if not new_image:
            continue

        item = ddb_stream_image_to_python(new_image)
        ticker = item.get("ticker")
        if ticker:
            updated_tickers.add(ticker)

    if not updated_tickers:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "processed_clients": 0,
                "emails_sent": 0,
                "updated_tickers": []
            })
        }

    print("Updated tickers in this batch:", list(updated_tickers))

    # 2. Scan all clients and find who holds any updated ticker
    scan_kwargs = {}
    clients = []

    while True:
        resp = client_table.scan(**scan_kwargs)
        clients.extend(resp.get("Items", []))
        if "LastEvaluatedKey" in resp:
            scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        else:
            break

    processed_clients = 0
    emails_sent = 0
    now_ts = int(time.time())

    for client in clients:
        holdings = client.get("holdings", [])
        holding_tickers = {h.get("ticker") for h in holdings if isinstance(h, dict)}

        # Only process client if any of their tickers was updated
        if not (holding_tickers & updated_tickers):
            continue

        # Compute portfolio volatility
        holdings_with_vol, portfolio_vol = compute_portfolio_vol(holdings)
        if portfolio_vol is None:
            print(f"Client {client.get('ClientID')} has no usable volatility data – skipping.")
            continue

        target_vol = to_float(client.get("targetRisk"))
        if target_vol is None:
            # If no target set, default to current portfolio vol to avoid nonsense bands
            target_vol = portfolio_vol

        vol_tol = get_vol_tolerance_for_client(client)
        lower = target_vol * (1.0 - vol_tol)
        upper = target_vol * (1.0 + vol_tol)
        within = lower <= portfolio_vol <= upper

        # 3. Update client record with currentVol + timestamp
        try:
            client_table.update_item(
                Key={"ClientID": client["ClientID"]},
                UpdateExpression="SET currentVol = :cv, lastUpdated = :ts",
                ExpressionAttributeValues={
                    ":cv": to_decimal(portfolio_vol),
                    ":ts": to_decimal(now_ts)
                }
            )
        except ClientError as e:
            print(f"Error updating client {client.get('ClientID')}:",
                  e.response["Error"]["Message"])

        # 4. Dedup: only send email if cooldown expired
        client_id = client.get("ClientID")
        if not should_send_email_for_client(client_id):
            continue

        # 5. Build and send email
        text_body, html_body = build_email_bodies(
            client,
            holdings_with_vol,
            portfolio_vol,
            target_vol,
            lower,
            upper,
            within
        )
        send_email(client.get("email"), text_body, html_body)

        processed_clients += 1
        emails_sent += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed_clients": processed_clients,
            "emails_sent": emails_sent,
            "updated_tickers": list(updated_tickers)
        })
    }
