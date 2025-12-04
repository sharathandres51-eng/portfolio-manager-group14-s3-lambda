import os
import json
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# ----------------- Configuration ----------------- #

# Other settings can remain environment-based or default:
SES_REGION     = os.getenv("SES_REGION", "us-east-1")
EMAIL_SUBJECT  = os.getenv("EMAIL_SUBJECT", "Daily Portfolio Update")
RISK_TOLERANCE = float(os.getenv("RISK_TOLERANCE", "0.05"))
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
ses = boto3.client("ses", region_name=SES_REGION)
deserializer = TypeDeserializer()


# ----------------- Helper functions ----------------- #

def ddb_to_python(ddb_item: dict) -> dict:
    return {k: deserializer.deserialize(v) for k, v in ddb_item.items()}


def to_float(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return float(x)
    return float(x)


def pct_str(x):
    x = to_float(x)
    return f"{x:.2%}" if x is not None else "N/A"


def get_client_risk_band(client: dict):
    target = client.get("targetRisk")
    if target is None:
        return None, None
    target = to_float(target)

    tol = client.get("riskTolerance")
    if tol is not None:
        tol = to_float(tol)
    else:
        tol = RISK_TOLERANCE

    return target - tol, target + tol


def compute_risk_status(client: dict):
    current_risk = to_float(client.get("currentRisk"))
    target_risk  = to_float(client.get("targetRisk"))
    lower, upper = get_client_risk_band(client)

    if current_risk is None or lower is None or upper is None:
        txt = "Risk status: Unknown (missing or incomplete risk settings)."
        html = "<p><b>Risk status:</b> Unknown (missing or incomplete risk settings).</p>"
        return txt, html, False

    within = lower <= current_risk <= upper

    if within:
        txt = ("You are currently WITHIN your agreed risk boundary. "
               "No immediate action is required.")
        html = ("<p><b>Risk status:</b> "
                "<span style='color:green;'>Within your agreed risk boundary.</span> "
                "No immediate action is required.</p>")
    else:
        txt = ("You are currently OUTSIDE your agreed risk boundary. "
               "We recommend reviewing your portfolio and considering rebalancing.")
        html = ("<p><b>Risk status:</b> "
                "<span style='color:red;'>Outside your agreed risk boundary.</span> "
                "We recommend reviewing your portfolio and considering rebalancing.</p>")

    return txt, html, within


def build_email_bodies(client: dict):
    client_id = client.get("ClientID", client.get("clientId", "N/A"))
    name      = client.get("name", "Client")

    holdings  = client.get("holdings", [])
    portfolio_ret = client.get("portfolioDailyReturn")

    current_risk = client.get("currentRisk")
    target_risk  = client.get("targetRisk")

    risk_txt, risk_html, _ = compute_risk_status(client)

    # -------- TEXT BODY --------
    text_lines = [
        f"Hello {name},",
        "",
        "Here is your daily portfolio update:",
        "",
        "Position performance today:"
    ]

    if holdings:
        text_lines.append("  Ticker   Qty   Daily Return")
        for h in holdings:
            text_lines.append(
                f"  {h.get('ticker','N/A'):<7} {h.get('quantity','N/A'):<4} {pct_str(h.get('dailyReturn'))}"
            )
    else:
        text_lines.append("  (No holdings information available.)")

    text_lines.extend([
        "",
        f"Overall portfolio performance today: {pct_str(portfolio_ret)}",
        "",
        "Risk overview:",
        f"  Current risk: {pct_str(current_risk)}",
        f"  Target risk:  {pct_str(target_risk)}",
        "",
        risk_txt,
        "",
        "Best regards,",
        "Your Portfolio Monitoring System"
    ])

    text_body = "\n".join(text_lines)

    # -------- HTML BODY --------

    if holdings:
        rows = [
            f"<tr><td>{h.get('ticker','N/A')}</td>"
            f"<td style='text-align:right;'>{h.get('quantity','N/A')}</td>"
            f"<td style='text-align:right;'>{pct_str(h.get('dailyReturn'))}</td></tr>"
            for h in holdings
        ]
        holdings_html = (
            "<table border='1' cellpadding='4' cellspacing='0'>"
            "<tr><th>Ticker</th><th>Qty</th><th>Daily Return</th></tr>"
            + "".join(rows) +
            "</table>"
        )
    else:
        holdings_html = "<p>No holdings information available.</p>"

    html_body = f"""
    <html>
    <body style="font-family: Arial; font-size:14px;">
        <p>Hello {name},</p>
        <p>Here is your daily portfolio update for client ID <b>{client_id}</b>:</p>

        <h3>Position performance today</h3>
        {holdings_html}

        <h3>Overall portfolio performance</h3>
        <p><b>Today:</b> {pct_str(portfolio_ret)}</p>

        <h3>Risk overview</h3>
        <p><b>Current risk:</b> {pct_str(current_risk)}<br/>
           <b>Target risk:</b> {pct_str(target_risk)}</p>
        {risk_html}

        <p>Best regards,<br/><i>Your Portfolio Monitoring System</i></p>
    </body>
    </html>
    """

    return text_body, html_body


# ----------------- EMAIL SENDING ----------------- #

def send_email(recipient: str, text_body: str, html_body: str):
    """Send email via SES using the hardcoded sender address."""
    
    if not recipient:
        print("No recipient email found – skipping.")
        return

    print("DEBUG SENDER_EMAIL:", SENDER_EMAIL)

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


# ----------------- LAMBDA HANDLER ----------------- #

def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    processed = 0
    emailed   = 0

    for record in event["Records"]:
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue

        new_image = record["dynamodb"].get("NewImage")
        if not new_image:
            continue

        client = ddb_to_python(new_image)
        processed += 1

        text_body, html_body = build_email_bodies(client)
        send_email(client.get("email"), text_body, html_body)
        emailed += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed_records": processed,
            "emails_sent": emailed
        })
    }
