# Portfolio Manager – Automated Daily Risk Monitoring System

## Overview

This system automates the lifecycle of portfolio risk monitoring. It pulls market data, computes volatility, and evaluates client risk against pre-defined tolerance levels. If a client's portfolio drift exceeds their approved range, the system triggers a consolidated email alert.

**Key Features:**
* **Automated Ingestion:** Pulls fresh market data from Yahoo Finance.
* **Risk Computation:** Calculates asset-specific volatility and portfolio-level risk.
* **Compliance Monitoring:** Compares current risk against client target volatility.
* **Smart Alerting:** Sends a single, deduplicated email via AWS SES when risk bands are breached.

**Tech Stack:**
* **Compute:** AWS Lambda (Python)
* **Storage:** AWS S3, AWS DynamoDB
* **Messaging:** AWS SES (Simple Email Service)
* **Orchestration:** CloudWatch Events, DynamoDB Streams

---

## Architecture

The system operates on a fully event-driven architecture:

```mermaid
graph TD
    CW[CloudWatch Scheduler] -->|Daily Trigger| L1[Lambda 1: s3_handler]
    L1 -->|Write CSV| S3[S3 Bucket]
    S3 -->|S3 Event Trigger| L2[Lambda 2: risk_handler]
    L2 -->|Write Volatility| DDB[DynamoDB: PredictedVolatility]
    DDB -->|Stream Event| L3[Lambda 3: ses_handler]
    L3 -->|Send Alert| SES[AWS SES]
