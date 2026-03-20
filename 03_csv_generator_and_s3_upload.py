"""
FINSERV DEMO — Step 03: CSV Generator & S3 Upload
Generates synthetic financial data as CSV files and uploads to S3.

Usage:
    pip install boto3 faker
    python3 03_csv_generator_and_s3_upload.py

Configuration:
    Set environment variables or modify the CONFIG dict below.
    - S3_BUCKET:     Target S3 bucket name
    - S3_PREFIX:     Folder prefix in the bucket
    - AWS_PROFILE:   (optional) AWS CLI profile to use
"""

import csv
import io
import json
import os
import random
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

CONFIG = {
    "s3_bucket": os.environ.get("S3_BUCKET", "your-finserv-bucket"),
    "s3_prefix": os.environ.get("S3_PREFIX", "finserv-demo/"),
    "output_dir": os.environ.get("OUTPUT_DIR", "./csv_output"),
    "customers_count": 500,
    "transactions_count": 5000,
    "risk_assessments_count": 500,
}

CITIES = [
    ("New York", "NY", "USA"), ("London", "England", "UK"),
    ("Singapore", "Central", "Singapore"), ("Tokyo", "Kanto", "Japan"),
    ("Sydney", "NSW", "Australia"), ("Toronto", "ON", "Canada"),
    ("Mumbai", "MH", "India"), ("Dubai", "Dubai", "UAE"),
    ("Sao Paulo", "SP", "Brazil"), ("Berlin", "Berlin", "Germany"),
]

MERCHANTS = [
    "Amazon", "Walmart", "Starbucks", "Shell Gas", "Target",
    "Apple Store", "Netflix", "Uber", "Delta Air", "Costco",
    "Home Depot", "Whole Foods", "Chase Transfer", "Wire Transfer", "ATM",
]

CATEGORIES = [
    "GROCERIES", "DINING", "SHOPPING", "FUEL", "ENTERTAINMENT",
    "TRAVEL", "TRANSFER", "UTILITIES", "HEALTHCARE", "INVESTMENT",
]

CHANNELS = ["ONLINE", "POS", "MOBILE", "ATM", "BRANCH"]


# ---------------------------------------------------------------------------
# DATA GENERATORS
# ---------------------------------------------------------------------------

def generate_customers(count: int) -> list[dict]:
    """Generate customer records."""
    rows = []
    for i in range(1, count + 1):
        city, state, country = random.choice(CITIES)
        dob = datetime.now() - timedelta(days=random.randint(7300, 25550))
        signup = datetime.now() - timedelta(days=random.randint(0, 730))
        rows.append({
            "FIRST_NAME": f"CSV_First_{i}",
            "LAST_NAME": f"CSV_Last_{random.randint(1, 500)}",
            "EMAIL": f"csv_user_{i}@example.com",
            "PHONE": f"+1-{random.randint(200,999)}-{random.randint(1000,9999)}",
            "DATE_OF_BIRTH": dob.strftime("%Y-%m-%d"),
            "CITY": city,
            "STATE": state,
            "COUNTRY": country,
            "ANNUAL_INCOME": round(random.uniform(25000, 500000), 2),
            "EMPLOYMENT_STATUS": random.choice(
                ["EMPLOYED", "SELF_EMPLOYED", "RETIRED", "STUDENT", "UNEMPLOYED"]
            ),
            "CREDIT_SCORE": random.randint(300, 850),
            "SIGNUP_DATE": signup.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return rows


def generate_transactions(count: int) -> list[dict]:
    """Generate transaction records."""
    rows = []
    for _ in range(count):
        txn_date = datetime.now() - timedelta(seconds=random.randint(0, 15552000))
        rows.append({
            "ACCOUNT_ID": random.randint(1, 3000),
            "TXN_DATE": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "TXN_TYPE": random.choice(["DEBIT", "CREDIT", "TRANSFER"]),
            "AMOUNT": round(random.uniform(0.1, 5000), 2),
            "MERCHANT_NAME": random.choice(MERCHANTS),
            "CATEGORY": random.choice(CATEGORIES),
            "CHANNEL": random.choice(CHANNELS),
            "IS_FLAGGED": random.random() < 0.03,
        })
    return rows


def generate_risk_assessments(count: int) -> list[dict]:
    """Generate risk assessment records with JSON RISK_DATA column."""
    rows = []
    for _ in range(count):
        assessed = datetime.now() - timedelta(days=random.randint(0, 365))
        risk_data = {
            "risk_score": random.randint(1, 100),
            "credit_history": random.choice(["EXCELLENT", "GOOD", "FAIR", "POOR"]),
            "debt_to_income": round(random.uniform(0.05, 0.80), 2),
            "risk_factors": [
                {"factor": "payment_history", "score": random.randint(1, 100)},
                {"factor": "credit_utilization", "score": random.randint(1, 100)},
                {"factor": "account_age", "score": random.randint(1, 100)},
            ],
            "assessment_type": random.choice(["STANDARD", "ENHANCED", "EXPEDITED"]),
            "model_version": f"v2.3.{random.randint(0, 9)}",
        }
        rows.append({
            "CUSTOMER_ID": random.randint(1, 2000),
            "ASSESSED_AT": assessed.strftime("%Y-%m-%d %H:%M:%S"),
            "RISK_DATA": json.dumps(risk_data),
        })
    return rows


# ---------------------------------------------------------------------------
# CSV WRITER
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict], filename: str, output_dir: str) -> str:
    """Write rows to a CSV file and return the file path."""
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    if not rows:
        print(f"  No rows to write for {filename}")
        return filepath

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Wrote {len(rows):,} rows to {filepath}")
    return filepath


def rows_to_csv_bytes(rows: list[dict]) -> bytes:
    """Convert rows to CSV bytes for direct S3 upload."""
    if not rows:
        return b""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


# ---------------------------------------------------------------------------
# S3 UPLOAD
# ---------------------------------------------------------------------------

def upload_to_s3(filepath: str, bucket: str, key: str) -> None:
    """Upload a local file to S3."""
    try:
        import boto3
    except ImportError:
        print("  boto3 not installed. Skipping S3 upload.")
        print(f"  Install with: pip install boto3")
        return

    profile = os.environ.get("AWS_PROFILE")
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3")

    try:
        s3.upload_file(filepath, bucket, key)
        print(f"  Uploaded to s3://{bucket}/{key}")
    except Exception as e:
        print(f"  S3 upload failed: {e}")
        print(f"  File saved locally at: {filepath}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("FINSERV DEMO — CSV Generator & S3 Upload")
    print("=" * 60)

    output_dir = CONFIG["output_dir"]
    bucket = CONFIG["s3_bucket"]
    prefix = CONFIG["s3_prefix"]

    # Generate data
    print("\n[1/3] Generating customers...")
    customers = generate_customers(CONFIG["customers_count"])
    f1 = write_csv(customers, "customers.csv", output_dir)

    print("\n[2/3] Generating transactions...")
    transactions = generate_transactions(CONFIG["transactions_count"])
    f2 = write_csv(transactions, "transactions.csv", output_dir)

    print("\n[3/3] Generating risk assessments...")
    risk = generate_risk_assessments(CONFIG["risk_assessments_count"])
    f3 = write_csv(risk, "risk_assessments.csv", output_dir)

    # Upload to S3
    print("\n" + "-" * 60)
    print("Uploading to S3...")
    print(f"  Bucket: {bucket}")
    print(f"  Prefix: {prefix}")

    for filepath, name in [(f1, "customers.csv"), (f2, "transactions.csv"), (f3, "risk_assessments.csv")]:
        upload_to_s3(filepath, bucket, f"{prefix}{name}")

    print("\n" + "=" * 60)
    print("Done! Files ready for Snowpipe ingestion.")
    print(f"Local files saved in: {output_dir}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
