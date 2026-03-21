"""
FINSERV DEMO — Step 03: CSV Generator & S3 Upload
Generates realistic CSV data for 3 tables (TRANSACTIONS, SUPPORT_TICKETS,
RISK_ASSESSMENTS) and uploads to S3 for Snowpipe auto-ingest.

These tables are NOT populated by SQL INSERT in file 02.
Instead, data flows: CSV → S3 → Snowpipe → landing tables → MERGE into BASE.

Usage:
    pip install faker boto3
    # Upload to S3 (requires AWS credentials):
    python3 03_csv_generator_and_s3_upload.py
    # Local-only mode (no S3, writes to ./csv_output/):
    python3 03_csv_generator_and_s3_upload.py --local

Environment variables:
    S3_BUCKET   — S3 bucket name (default: your-finserv-bucket)
    S3_PREFIX   — S3 key prefix (default: finserv-demo/)
    AWS_PROFILE — AWS CLI profile (default: default)
    OUTPUT_DIR  — Local output directory (default: ./csv_output)
"""

import argparse
import csv
import json
import os
import random
import sys
from datetime import datetime, timedelta

from faker import Faker

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

CONFIG = {
    "output_dir": os.environ.get("OUTPUT_DIR", "./csv_output"),
    "s3_bucket": os.environ.get("S3_BUCKET", "your-finserv-bucket"),
    "s3_prefix": os.environ.get("S3_PREFIX", "finserv-demo/"),
    "aws_profile": os.environ.get("AWS_PROFILE", "default"),
    "transactions_count": 10000,
    "support_tickets_count": 1000,
    "risk_assessments_count": 2000,
    "seed": 42,
}

# Weighted locale mix for international realism
LOCALES = ["en_US", "en_GB", "en_AU", "en_CA", "en_IN", "ja_JP", "de_DE", "pt_BR", "fr_FR"]
fake = Faker(LOCALES)
Faker.seed(CONFIG["seed"])
random.seed(CONFIG["seed"])

# ---------------------------------------------------------------------------
# REFERENCE DATA — correlated merchants, categories, and amounts
# ---------------------------------------------------------------------------

# Merchant -> (category, typical_amount_range, common_channels)
MERCHANT_PROFILES = {
    "Amazon":           ("SHOPPING",      (5, 500),     ["ONLINE", "MOBILE"]),
    "Walmart":          ("GROCERIES",     (15, 250),    ["POS", "ONLINE"]),
    "Costco":           ("GROCERIES",     (50, 400),    ["POS"]),
    "Whole Foods":      ("GROCERIES",     (20, 180),    ["POS", "ONLINE"]),
    "Trader Joe's":     ("GROCERIES",     (15, 120),    ["POS"]),
    "Starbucks":        ("DINING",        (3, 15),      ["POS", "MOBILE"]),
    "Chipotle":         ("DINING",        (8, 25),      ["POS", "MOBILE"]),
    "DoorDash":         ("DINING",        (15, 65),     ["ONLINE", "MOBILE"]),
    "Shell":            ("FUEL",          (20, 90),     ["POS"]),
    "BP":               ("FUEL",          (25, 85),     ["POS"]),
    "Target":           ("SHOPPING",      (10, 300),    ["POS", "ONLINE"]),
    "Apple":            ("SHOPPING",      (50, 2000),   ["POS", "ONLINE"]),
    "Best Buy":         ("SHOPPING",      (30, 1500),   ["POS", "ONLINE"]),
    "Netflix":          ("ENTERTAINMENT", (7, 23),      ["ONLINE"]),
    "Spotify":          ("ENTERTAINMENT", (5, 17),      ["ONLINE"]),
    "AMC Theatres":     ("ENTERTAINMENT", (12, 50),     ["POS", "ONLINE"]),
    "Uber":             ("TRAVEL",        (8, 60),      ["MOBILE"]),
    "Lyft":             ("TRAVEL",        (7, 55),      ["MOBILE"]),
    "Delta Air Lines":  ("TRAVEL",        (150, 1200),  ["ONLINE"]),
    "United Airlines":  ("TRAVEL",        (180, 1400),  ["ONLINE"]),
    "Marriott":         ("TRAVEL",        (100, 500),   ["ONLINE"]),
    "Home Depot":       ("UTILITIES",     (15, 600),    ["POS", "ONLINE"]),
    "Verizon":          ("UTILITIES",     (40, 200),    ["ONLINE", "POS"]),
    "CVS Pharmacy":     ("HEALTHCARE",    (5, 150),     ["POS"]),
    "Walgreens":        ("HEALTHCARE",    (5, 100),     ["POS"]),
    "Chase Transfer":   ("TRANSFER",      (100, 5000),  ["ONLINE", "MOBILE", "BRANCH"]),
    "Zelle":            ("TRANSFER",      (20, 2000),   ["MOBILE", "ONLINE"]),
    "Wire Transfer":    ("TRANSFER",      (500, 5000),  ["BRANCH", "ONLINE"]),
    "Schwab":           ("INVESTMENT",    (500, 5000),  ["ONLINE"]),
    "Fidelity":         ("INVESTMENT",    (200, 5000),  ["ONLINE"]),
    "ATM Withdrawal":   ("TRANSFER",      (20, 500),    ["ATM"]),
}

MERCHANT_NAMES = list(MERCHANT_PROFILES.keys())

# Support ticket reference data
TICKET_SUBJECTS = [
    "Account access issue",
    "Unauthorized transaction reported",
    "Request for credit limit increase",
    "Mobile app not loading",
    "Wire transfer delay",
    "Incorrect balance displayed",
    "Card declined at merchant",
    "Interest rate dispute",
    "Lost debit card",
    "Duplicate charge on statement",
    "Account closure request",
    "PIN reset needed",
    "Foreign transaction fee inquiry",
    "Direct deposit not received",
    "Fraud alert triggered",
]

TICKET_BODIES = [
    "I have been unable to access my account for the past 24 hours. Every time I try to log in, I receive an error message saying my credentials are invalid even though I am certain they are correct. I have tried resetting my password twice but the reset email never arrives. This is extremely urgent as I need to make a payment today.",
    "I noticed a transaction on my statement that I did not authorize. The charge is for $2,500 from an online retailer I have never used. I need this investigated immediately and the funds returned to my account. I have not shared my card details with anyone.",
    "I would like to request an increase to my credit card limit. My current limit is $10,000 and I am requesting $25,000. My income has increased significantly in the past year and I have maintained a perfect payment history.",
    "The mobile banking app has been crashing every time I try to view my account summary. I have tried uninstalling and reinstalling the app, clearing the cache, and restarting my phone. I am using the latest version of the app on iOS.",
    "I initiated a wire transfer 5 business days ago and the recipient has not received the funds. The transfer was for $15,000 to a domestic account. The funds have already been debited from my account but the recipient bank says they have no record of the incoming transfer.",
    "My account balance shows $5,000 less than what I calculated based on my recent transactions. I have gone through each transaction in my statement and cannot find the discrepancy. I need someone to review my account history.",
    "My debit card was declined at a grocery store today even though I have sufficient funds in my account. This is the third time this has happened this month. It is very embarrassing and I need this resolved immediately.",
    "I believe the interest rate on my savings account is incorrect. My agreement states 3.5% APY but I am only receiving 2.1%. I have been a customer for over 10 years and I expect this to be corrected retroactively.",
    "I lost my debit card while traveling abroad. I need it cancelled immediately and a replacement sent to my home address. I also need to check if there have been any unauthorized transactions since I lost it yesterday.",
    "I have been charged twice for the same transaction at a restaurant. Both charges are for $85.50 and appeared on the same day. I need one of these charges reversed.",
]

PRIORITIES = ["LOW", "MEDIUM", "MEDIUM", "HIGH", "CRITICAL"]  # weighted toward MEDIUM
RESOLUTIONS = ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED", "ESCALATED"]
ASSIGNED_TEAMS = ["Support Team", "Fraud Team", "Card Services", "Tech Support", "Compliance"]


# ---------------------------------------------------------------------------
# DATA GENERATORS
# ---------------------------------------------------------------------------

def generate_transactions(count: int) -> list[dict]:
    """Generate realistic transaction records with correlated merchants/amounts."""
    rows = []
    for _ in range(count):
        merchant = random.choice(MERCHANT_NAMES)
        category, (amt_low, amt_high), channels = MERCHANT_PROFILES[merchant]
        amount = round(random.triangular(amt_low, amt_high, (amt_low + amt_high) / 2), 2)
        channel = random.choice(channels)

        # Transaction type based on category
        if category == "TRANSFER":
            txn_type = "TRANSFER"
        elif category == "INVESTMENT":
            txn_type = random.choice(["DEBIT", "CREDIT"])
        else:
            txn_type = random.choices(["DEBIT", "CREDIT"], weights=[0.85, 0.15], k=1)[0]

        # Flagged transactions: higher amounts more likely flagged
        flag_prob = 0.01 if amount < 500 else (0.05 if amount < 2000 else 0.10)
        is_flagged = random.random() < flag_prob

        # Spread transactions over ~6 months
        txn_date = datetime.now() - timedelta(seconds=random.randint(0, 15552000))

        rows.append({
            "ACCOUNT_ID": random.randint(1, 3000),
            "TXN_DATE": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "TXN_TYPE": txn_type,
            "AMOUNT": amount,
            "MERCHANT_NAME": merchant,
            "CATEGORY": category,
            "CHANNEL": channel,
            "IS_FLAGGED": is_flagged,
        })
    return rows


def generate_support_tickets(count: int) -> list[dict]:
    """Generate realistic support ticket records with varied subjects and bodies."""
    rows = []
    for _ in range(count):
        created = datetime.now() - timedelta(hours=random.randint(0, 4380))  # ~6 months

        rows.append({
            "CUSTOMER_ID": random.randint(1, 2000),
            "CREATED_AT": created.strftime("%Y-%m-%d %H:%M:%S"),
            "SUBJECT": random.choice(TICKET_SUBJECTS),
            "PRIORITY": random.choice(PRIORITIES),
            "BODY": random.choice(TICKET_BODIES),
            "RESOLUTION_STATUS": random.choice(RESOLUTIONS),
            "ASSIGNED_TO": random.choice(ASSIGNED_TEAMS),
        })
    return rows


def generate_risk_assessments(count: int) -> list[dict]:
    """Generate risk assessment records with realistic JSON RISK_DATA."""
    factor_pool = [
        "payment_history", "credit_utilization", "account_age",
        "debt_to_income_ratio", "recent_inquiries", "public_records",
        "credit_mix", "new_accounts",
    ]
    rows = []
    for _ in range(count):
        assessed = datetime.now() - timedelta(days=random.randint(0, 365))
        credit_history = random.choices(
            ["EXCELLENT", "GOOD", "FAIR", "POOR"],
            weights=[0.20, 0.40, 0.25, 0.15],
            k=1,
        )[0]

        # Risk score correlates with credit history
        score_ranges = {"EXCELLENT": (10, 30), "GOOD": (25, 50), "FAIR": (40, 70), "POOR": (60, 95)}
        s_lo, s_hi = score_ranges[credit_history]
        risk_score = random.randint(s_lo, s_hi)

        # Pick 3 factors from the pool
        chosen_factors = random.sample(factor_pool, 3)
        risk_factors = [
            {"factor": f, "score": random.randint(1, 100)}
            for f in chosen_factors
        ]

        risk_data = {
            "risk_score": risk_score,
            "credit_history": credit_history,
            "debt_to_income": round(random.triangular(0.05, 0.80, 0.35), 2),
            "risk_factors": risk_factors,
            "assessment_type": random.choices(
                ["STANDARD", "ENHANCED", "EXPEDITED"],
                weights=[0.60, 0.25, 0.15], k=1,
            )[0],
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


# ---------------------------------------------------------------------------
# S3 UPLOAD
# ---------------------------------------------------------------------------

def upload_to_s3(filepath: str, s3_subfolder: str, bucket: str, prefix: str, profile: str) -> None:
    """Upload a local CSV file to S3 for Snowpipe auto-ingest."""
    try:
        import boto3
    except ImportError:
        print("  ERROR: boto3 not installed. Run: pip install boto3")
        sys.exit(1)

    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")
    filename = os.path.basename(filepath)
    s3_key = f"{prefix}{s3_subfolder}/{filename}"

    print(f"  Uploading {filepath} → s3://{bucket}/{s3_key}")
    s3.upload_file(filepath, bucket, s3_key)
    print(f"  Uploaded successfully.")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FINSERV CSV Generator & S3 Upload")
    parser.add_argument(
        "--local", action="store_true",
        help="Local-only mode: generate CSVs without uploading to S3",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("FINSERV DEMO — CSV Generator & S3 Upload")
    print("=" * 60)
    if args.local:
        print("Mode: LOCAL (no S3 upload)")
    else:
        print(f"Mode: S3 (bucket={CONFIG['s3_bucket']}, prefix={CONFIG['s3_prefix']})")

    output_dir = CONFIG["output_dir"]

    # --- Transactions ---
    print(f"\n[1/3] Generating {CONFIG['transactions_count']:,} transactions...")
    transactions = generate_transactions(CONFIG["transactions_count"])
    txn_path = write_csv(transactions, "transactions.csv", output_dir)
    if not args.local:
        upload_to_s3(txn_path, "transactions", CONFIG["s3_bucket"], CONFIG["s3_prefix"], CONFIG["aws_profile"])

    # --- Support Tickets ---
    print(f"\n[2/3] Generating {CONFIG['support_tickets_count']:,} support tickets...")
    tickets = generate_support_tickets(CONFIG["support_tickets_count"])
    tkt_path = write_csv(tickets, "support_tickets.csv", output_dir)
    if not args.local:
        upload_to_s3(tkt_path, "support_tickets", CONFIG["s3_bucket"], CONFIG["s3_prefix"], CONFIG["aws_profile"])

    # --- Risk Assessments ---
    print(f"\n[3/3] Generating {CONFIG['risk_assessments_count']:,} risk assessments...")
    risk = generate_risk_assessments(CONFIG["risk_assessments_count"])
    risk_path = write_csv(risk, "risk_assessments.csv", output_dir)
    if not args.local:
        upload_to_s3(risk_path, "risk_assessments", CONFIG["s3_bucket"], CONFIG["s3_prefix"], CONFIG["aws_profile"])

    # --- Summary ---
    print("\n" + "=" * 60)
    print("Done! CSV files generated:")
    print(f"  {os.path.abspath(output_dir)}/transactions.csv      ({CONFIG['transactions_count']:,} rows)")
    print(f"  {os.path.abspath(output_dir)}/support_tickets.csv   ({CONFIG['support_tickets_count']:,} rows)")
    print(f"  {os.path.abspath(output_dir)}/risk_assessments.csv  ({CONFIG['risk_assessments_count']:,} rows)")
    if args.local:
        print("\nLocal mode — files NOT uploaded to S3.")
        print("To upload, run without --local flag (requires boto3 + AWS credentials).")
    else:
        print(f"\nFiles uploaded to s3://{CONFIG['s3_bucket']}/{CONFIG['s3_prefix']}")
        print("Snowpipe auto-ingest will pick up the files automatically.")
    print("Next step: See 04_s3_stage_and_snowpipe.sql")
    print("=" * 60)


if __name__ == "__main__":
    main()
