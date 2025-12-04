import os
import json
import time
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# ----------------- Configuration ----------------- #

CLIENT_TABLE_NAME      = os.getenv("CLIENT_TABLE")
PRED_TABLE_NAME        = os.getenv("PREDICTIONS_TABLE")
VOL_TOL_DEFAULT        = float(os.getenv("VOL_TOLERANCE_DEFAULT"))

SES_REGION             = os.getenv("SES_REGION")
EMAIL_SUBJECT          = os.getenv("EMAIL_SUBJECT")
SENDER_EMAIL           = os.getenv("SENDER_EMAIL")

# Email cooldown (seconds)
EMAIL_COOLDOWN_SECONDS = int(os.getenv("EMAIL_COOLDOWN_SECONDS"))

dynamodb = boto3.resource("dynamodb")
client_table = dynamodb.Table(CLIENT_TABLE_NAME)
pred_table   = dynamodb.Table(PRED_TABLE_NAME)

ses = boto3.client("ses", region_name=SES_REGION)
deserializer = TypeDeserializer()


# ----------------- Helper functions ----------------- #

def ddb_stream_image_to_python(ddb_item: dict) -> dict:
    return {k: deserializer.deserialize(v) for k, v in ddb_item.items()}


def to_float(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return float(x)
    try:
        return float(x)
    except:
        return None


def to_decimal(x):
    if x is None:
        return None
    return Decimal(str(x))


def pct_str(x):
    x = to_float(x)
    return f"{x:.2%}" if x is not None else "N/A"


def get_vol_tolerance_for_client(client: dict) -> float:
    vt = client.get("volTolerance")
    if vt is not None:
        vt = to_float(vt)
        if vt is not None:
            return vt
    return VOL_TOL_DEFAULT


def get_latest_vol_for_ticker(ticker: str):
    """Query PredictedVolatility for latest volatility value."""
    resp = pred_table.query(
        KeyConditionExpression=Key("ticker").eq(ticker),
        ScanIndexForward=False,
        Limit=1
    )
    items = resp.get("Items", [])
    if not items:
        return None
    return to_float(items[0].get("predicted_volatility"))


def compute_portfolio_vol(holdings: list):
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

    portfolio_vol = (weighted_sum / total_qty) if total_qty > 0 else None
    return holdings_with_vol, portfolio_vol


# ------------------------------------------------------------------------------
# Dedup logic using DynamoDB conditional update
# Only allow one email per client per cooldown window
# ------------------------------------------------------------------------------

def try_mark_email_sent(client_id: str, cooldown_seconds: int) -> bool:
    now_ts = int(time.time())
    threshold = now_ts - cooldown_seconds

    try:
        client_table.update_item(
            Key={"ClientID": client_id},
            UpdateExpression="SET lastEmailSent = :now",
            ConditionExpression="attribute_not_exists(lastEmailSent) OR lastEmailSent < :threshold",
            ExpressionAttributeValues={
                ":now": Decimal(str(now_ts)),
                ":threshold": Decimal(str(threshold))
            }
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f"[DEDUP] Email already sent recently for client {client_id} — skipping.")
            return False
        else:
            raise


# ----------------- Email formatting ----------------- #

def build_email_bodies(client, holdings_with_vol, portfolio_vol, target_vol, lower, upper, within):
    client_id = client.get("ClientID", "N/A")
    name      = client.get("name", "Client")

    status_text = "WITHIN" if within else "OUTSIDE"
    status_color = "green" if within else "red"

    # ---- TEXT ----
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
        f"Accepted range:                       {pct_str(lower)} – {pct_str(upper)}",
        "",
        f"Risk status: You are currently {status_text} your agreed volatility band.",
        "",
        "Best regards,",
        "Your Portfolio Monitoring System"
    ])

    text_body = "\n".join(lines)

    # ---- HTML ----
    if holdings_with_vol:
        rows = "".join([
            f"<tr><td>{h['ticker']}</td><td align='right'>{int(h['quantity'])}</td>"
            f"<td align='right'>{pct_str(h['vol'])}</td></tr>"
            for h in holdings_with_vol
        ])
        holdings_html = (
            "<table border='1' cellpadding='4' cellspacing='0'>"
            "<tr><th>Ticker</th><th>Qty</th><th>Predicted 30d Vol</th></tr>"
            + rows + "</table>"
        )
    else:
        holdings_html = "<p>No holdings or volatility data available.</p>"

    html_body = f"""
    <html><body style="font-family: Arial; font-size:14px;">
        <p>Hello {name},</p>
        <p>This is your daily portfolio volatility update for client ID <b>{client_id}</b>.</p>

        <h3>Holdings (predicted 30-day volatility)</h3>
        {holdings_html}

        <h3>Portfolio volatility summary</h3>
        <p><b>Portfolio predicted 30-day volatility:</b> {pct_str(portfolio_vol)}<br/>
           <b>Target volatility:</b> {pct_str(target_vol)}<br/>
           <b>Accepted range:</b> {pct_str(lower)} – {pct_str(upper)}<br/>
           <b>Status:</b> <span style="color:{status_color};">{status_text} band</span></p>

        <p>Best regards,<br/><i>Your Portfolio Monitoring System</i></p>
    </body></html>
    """

    return text_body, html_body


def send_email(recipient, text_body, html_body):
    if not recipient or not SENDER_EMAIL:
        print("Missing email or sender — skipping.")
        return

    try:
        resp = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [recipient]},
            Message={
                "Subject": {"Data": EMAIL_SUBJECT},
                "Body": {"Text": {"Data": text_body}, "Html": {"Data": html_body}}
            }
        )
        print("Email sent →", recipient, "MessageId:", resp["MessageId"])
    except ClientError as e:
        print("SES error:", e.response["Error"]["Message"])


# ----------------- Main Handler ----------------- #

def lambda_handler(event, context):

    print("Event:", json.dumps(event))

    updated_tickers = set()

    # Find tickers updated in PredictedVolatility
    for record in event.get("Records", []):
        if record.get("eventName") not in ("INSERT", "MODIFY"):
            continue

        new_img = record.get("dynamodb", {}).get("NewImage")
        if not new_img:
            continue

        item = ddb_stream_image_to_python(new_img)
        if "ticker" in item:
            updated_tickers.add(item["ticker"])

    if not updated_tickers:
        return {"statusCode": 200, "body": json.dumps({"msg": "No tickers updated"})}

    print("Updated tickers:", updated_tickers)

    # Scan ALL clients
    scan_kwargs = {}
    clients = []
    while True:
        resp = client_table.scan(**scan_kwargs)
        clients.extend(resp.get("Items", []))
        if "LastEvaluatedKey" in resp:
            scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        else:
            break

    processed = 0
    emails_sent = 0

    for client in clients:
        holdings = client.get("holdings", [])
        holding_tickers = {h.get("ticker") for h in holdings if isinstance(h, dict)}

        # Client affected?
        if not (holding_tickers & updated_tickers):
            continue

        client_id = client.get("ClientID")

        # --- NEW: Dedup using DynamoDB conditional update ---
        if not try_mark_email_sent(client_id, EMAIL_COOLDOWN_SECONDS):
            continue  # skip sending

        holdings_with_vol, portfolio_vol = compute_portfolio_vol(holdings)
        if portfolio_vol is None:
            continue

        target_vol = to_float(client.get("targetRisk")) or portfolio_vol
        tol = get_vol_tolerance_for_client(client)
        lower = target_vol * (1 - tol)
        upper = target_vol * (1 + tol)
        within = lower <= portfolio_vol <= upper

        text_body, html_body = build_email_bodies(
            client, holdings_with_vol, portfolio_vol,
            target_vol, lower, upper, within
        )
        send_email(client.get("email"), text_body, html_body)

        processed += 1
        emails_sent += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed_clients": processed,
            "emails_sent": emails_sent,
            "updated_tickers": list(updated_tickers)
        })
    }
