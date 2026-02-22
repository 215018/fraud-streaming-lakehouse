# Real-Time Fraud Detection Pipeline (Databricks + Delta Lake)

## Overview
This project simulates a transaction processing pipeline using Databricks and Delta Lake. 
It demonstrates ingestion, transformation, feature engineering, and fraud detection scoring.

## Architecture

Bronze Layer:
- Raw transaction ingestion (simulated streaming batches)

Feature Engineering:
- Transaction count per user
- Total amount per user
- Country-based risk indicator

Fraud Scoring:
Transactions flagged if:
- Amount > 300
- Foreign country transaction
- High transaction frequency

## Technologies
- Python
- PySpark
- Delta Lake
- Databricks

## Example Outputs
- Fraud rate summary
- Fraud per country
- Top risky users

## How to Run
1. Create schema fraud_demo
2. Run ingestion cell
3. Run feature engineering cell
4. Run fraud scoring cell
