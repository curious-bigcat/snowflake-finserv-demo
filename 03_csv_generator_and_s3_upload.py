"""
FINSERV DEMO — Step 03: CSV Generator
Generates realistic financial services data as CSV files for Snowflake ingestion
via internal stage + COPY INTO (see 04_s3_stage_and_snowpipe.sql).

Usage:
    pip install faker
    python3 03_csv_generator_and_s3_upload.py

Output:
    ./csv_output/customers.csv
    ./csv_output/transactions.csv
    ./csv_output/risk_assessments.csv
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta

from faker import Faker

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

CONFIG = {
    "output_dir": os.environ.get("OUTPUT_DIR", "./csv_output"),
    "customers_count": 500,
    "transactions_count": 5000,
    "risk_assessments_count": 500,
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

LOCATIONS = [
    ("New York", "NY", "USA"),        ("Los Angeles", "CA", "USA"),
    ("Chicago", "IL", "USA"),         ("Houston", "TX", "USA"),
    ("Miami", "FL", "USA"),           ("London", "England", "UK"),
    ("Manchester", "England", "UK"),  ("Singapore", "Central", "Singapore"),
    ("Tokyo", "Kanto", "Japan"),      ("Sydney", "NSW", "Australia"),
    ("Melbourne", "VIC", "Australia"),("Toronto", "ON", "Canada"),
    ("Vancouver", "BC", "Canada"),    ("Mumbai", "MH", "India"),
    ("Dubai", "Dubai", "UAE"),        ("Sao Paulo", "SP", "Brazil"),
    ("Berlin", "Berlin", "Germany"),  ("Frankfurt", "Hessen", "Germany"),
    ("Paris", "IDF", "France"),       ("Zurich", "ZH", "Switzerland"),
    ("Hong Kong", "HK", "Hong Kong"), ("Seoul", "Seoul", "South Korea"),
]

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

# Employment status weights (realistic distribution)
EMPLOYMENT_WEIGHTS = {
    "EMPLOYED": 0.55, "SELF_EMPLOYED": 0.15, "RETIRED": 0.15,
    "STUDENT": 0.08, "UNEMPLOYED": 0.07,
}

# Income ranges by employment status (min, mode, max) for triangular distribution
INCOME_BY_EMPLOYMENT = {
    "EMPLOYED":      (30000, 75000, 350000),
    "SELF_EMPLOYED": (20000, 85000, 500000),
    "RETIRED":       (25000, 55000, 250000),
    "STUDENT":       (5000, 18000, 45000),
    "UNEMPLOYED":    (0, 12000, 35000),
}

# Email domain weights
EMAIL_DOMAINS = [
    ("gmail.com", 0.40), ("yahoo.com", 0.15), ("outlook.com", 0.12),
    ("icloud.com", 0.08), ("hotmail.com", 0.07), ("protonmail.com", 0.05),
    ("aol.com", 0.03), ("mail.com", 0.03), ("zoho.com", 0.02),
    ("fastmail.com", 0.02), ("hey.com", 0.01), ("gmx.com", 0.02),
]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def weighted_choice(options: dict) -> str:
    """Pick from a dict of {value: weight}."""
    items = list(options.keys())
    weights = list(options.values())
    return random.choices(items, weights=weights, k=1)[0]


def pick_email_domain() -> str:
    domains, weights = zip(*EMAIL_DOMAINS)
    return random.choices(domains, weights=weights, k=1)[0]


def realistic_email(first: str, last: str, birth_year: int) -> str:
    """Generate a realistic-looking email from name parts."""
    first_l = first.lower().replace(" ", "").replace("'", "")
    last_l = last.lower().replace(" ", "").replace("'", "")
    domain = pick_email_domain()
    pattern = random.choices(
        ["fl", "f.l", "f_l", "fl_yr", "f.l_yr", "first"],
        weights=[0.30, 0.25, 0.15, 0.12, 0.10, 0.08],
        k=1,
    )[0]
    yr = str(birth_year)[-2:]
    if pattern == "fl":
        local = f"{first_l}{last_l}"
    elif pattern == "f.l":
        local = f"{first_l}.{last_l}"
    elif pattern == "f_l":
        local = f"{first_l}_{last_l}"
    elif pattern == "fl_yr":
        local = f"{first_l}{last_l}{yr}"
    elif pattern == "f.l_yr":
        local = f"{first_l}.{last_l}{yr}"
    else:
        local = f"{first_l}{random.randint(1, 999)}"
    return f"{local}@{domain}"


def realistic_phone(country: str) -> str:
    """Generate a phone number matching the country."""
    formats = {
        "USA":          "+1-{}{}-{}{}{}-{}{}{}{}",
        "UK":           "+44-{}{}{}-{}{}{}-{}{}{}{}",
        "Canada":       "+1-{}{}{}-{}{}{}-{}{}{}{}",
        "Australia":    "+61-{}-{}{}{}{}-{}{}{}{}",
        "Japan":        "+81-{}{}-{}{}{}{}-{}{}{}{}",
        "Germany":      "+49-{}{}{}-{}{}{}{}{}{}",
        "India":        "+91-{}{}{}{}{}-{}{}{}{}{}",
        "Singapore":    "+65-{}{}{}{}-{}{}{}{}",
        "UAE":          "+971-{}{}-{}{}{}-{}{}{}{}",
        "Brazil":       "+55-{}{}-{}{}{}{}{}-{}{}{}{}",
        "France":       "+33-{}-{}{}-{}{}-{}{}-{}{}",
        "Switzerland":  "+41-{}{}-{}{}{}-{}{}-{}{}",
        "Hong Kong":    "+852-{}{}{}{}-{}{}{}{}",
        "South Korea":  "+82-{}{}-{}{}{}{}-{}{}{}{}",
    }
    fmt = formats.get(country, formats["USA"])
    digits = [str(random.randint(1 if i == 0 else 0, 9)) for i in range(15)]
    return fmt.format(*digits)


# ---------------------------------------------------------------------------
# DATA GENERATORS
# ---------------------------------------------------------------------------

def generate_customers(count: int) -> list[dict]:
    """Generate realistic customer records."""
    rows = []
    for _ in range(count):
        first = fake.first_name()
        last = fake.last_name()
        city, state, country = random.choice(LOCATIONS)
        dob = fake.date_of_birth(minimum_age=20, maximum_age=70)
        signup = datetime.now() - timedelta(days=random.randint(0, 730))

        emp_status = weighted_choice(EMPLOYMENT_WEIGHTS)
        inc_low, inc_mode, inc_high = INCOME_BY_EMPLOYMENT[emp_status]
        income = round(random.triangular(inc_low, inc_high, inc_mode), 2)

        # Credit score correlates loosely with income
        base_score = int(500 + (income / 500000) * 250)
        credit_score = max(300, min(850, base_score + random.randint(-80, 80)))

        rows.append({
            "FIRST_NAME": first,
            "LAST_NAME": last,
            "EMAIL": realistic_email(first, last, dob.year),
            "PHONE": realistic_phone(country),
            "DATE_OF_BIRTH": dob.strftime("%Y-%m-%d"),
            "CITY": city,
            "STATE": state,
            "COUNTRY": country,
            "ANNUAL_INCOME": income,
            "EMPLOYMENT_STATUS": emp_status,
            "CREDIT_SCORE": credit_score,
            "SIGNUP_DATE": signup.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return rows


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

        # Spread transactions over ~6 months with weekday bias
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
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("FINSERV DEMO — CSV Generator (Realistic Data)")
    print("=" * 60)

    output_dir = CONFIG["output_dir"]

    print("\n[1/3] Generating customers...")
    customers = generate_customers(CONFIG["customers_count"])
    write_csv(customers, "customers.csv", output_dir)

    print("\n[2/3] Generating transactions...")
    transactions = generate_transactions(CONFIG["transactions_count"])
    write_csv(transactions, "transactions.csv", output_dir)

    print("\n[3/3] Generating risk assessments...")
    risk = generate_risk_assessments(CONFIG["risk_assessments_count"])
    write_csv(risk, "risk_assessments.csv", output_dir)

    print("\n" + "=" * 60)
    print("Done! CSV files ready for Snowflake ingestion.")
    print(f"Files saved in: {os.path.abspath(output_dir)}/")
    print("\nNext step: PUT files to internal stage and COPY INTO")
    print("  See: 04_s3_stage_and_snowpipe.sql")
    print("=" * 60)


if __name__ == "__main__":
    main()
