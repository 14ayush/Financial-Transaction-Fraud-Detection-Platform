#!/usr/bin/env python3
"""
Synthetic transaction data generator for the Fraud Detection Pipeline.

Generates one day's worth of transactions per run, written as a CSV into a
date-partitioned folder (date=YYYY-MM-DD/transactions_YYYY-MM-DD.csv).

Fraud labels are NOT random — they are injected with correlated feature
signal (unusual amount, new device, distant location, off-hours, velocity
bursts) so the resulting dataset is actually learnable by a downstream
ML classifier, with noise added so it isn't trivially separable.

Usage:
    python generate_transactions.py --date 2026-08-01 --num-transactions 5000 --output-dir ./data
    python generate_transactions.py --date 2026-08-02 --num-transactions 5000 --output-dir ./data --seed 42
"""

import argparse
import csv
import ipaddress
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
import os




# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
NUM_USERS = random.randint(3000,8000)
NUM_MERCHANTS = random.randint(300,500)
FRAUD_RATE = random.uniform(0.01,0.05)  # ~2% of transactions are fraud — realistic order of magnitude

PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", "DIGITAL_WALLET","UPI"]
DEVICE_TYPES = ["MOBILE", "DESKTOP", "TABLET"]

COUNTRIES = ["US", "GB", "CA", "DE", "FR", "IN", "AU", "BR", "NG", "JP"]
# Weighted so a handful of countries dominate, like a real user base would
COUNTRY_WEIGHTS = [15, 12, 10, 8, 8, 20, 6, 6, 5, 5]

FIELDNAMES = [
    "TransactionID", "UserID", "MerchantID", "DeviceID", "TransactionAmount",
    "TransactionDate", "PaymentMethod", "DeviceType", "IP_Address",
    "LocationLat", "LocationLon", "IP_Country", "BillingCountry", "IsFraud",
]


# ---------------------------------------------------------------------------
# Reference pools: users, merchants
#
# Built deterministically from an index-derived seed, so a given user_id
# has the same "home" behavior profile every time you run the generator —
# this matters because fraud signals (new device, distant location, etc.)
# are defined *relative to* a user's normal behavior, so that behavior
# needs to be stable across days, not re-randomized every run.
# ---------------------------------------------------------------------------

def build_user_pool(num_users):
    users = []
    for i in range(num_users):
        rnd = random.Random(i)  # deterministic per user, independent of global seed
        home_country = rnd.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        users.append({
            "user_id": f"U{i:06d}",
            "home_country": home_country,
            "home_lat": rnd.uniform(-55.0, 65.0),
            "home_lon": rnd.uniform(-170.0, 170.0),
            "avg_amount": rnd.uniform(20, 300),
            "std_amount": rnd.uniform(5, 60),
            "active_hour_start": rnd.randint(6, 10),
            "active_hour_end": rnd.randint(18, 23),
            "known_devices": [
                str(uuid.UUID(int=rnd.getrandbits(128))) for _ in range(rnd.randint(1, 3))
            ],
            "preferred_payment": rnd.choice(PAYMENT_METHODS),
        })
    return users


def build_merchant_pool(num_merchants):
    merchants = []
    for i in range(num_merchants):
        rnd = random.Random(100_000 + i)
        # most merchants are low-risk; a small tail is high-risk (feeds the
        # merchant-risk feature downstream in go_merchant_risk_assessment)
        base_fraud_rate = rnd.choices([0.005, 0.02, 0.08], weights=[80, 15, 5])[0]
        merchants.append({"merchant_id": f"M{i:05d}", "base_fraud_rate": base_fraud_rate})
    return merchants


# ---------------------------------------------------------------------------
# Transaction generation
# ---------------------------------------------------------------------------

def random_timestamp_on(date_obj, hour_start=0, hour_end=23):
    hour = random.randint(hour_start, hour_end)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(date_obj.year, date_obj.month, date_obj.day, hour, minute, second, tzinfo=timezone.utc)


def random_ip():
    return str(ipaddress.IPv4Address(random.randint(0, 2**32 - 1)))


def jitter_location(lat, lon, max_km=25):
    """Small local jitter around a home location — used for legit transactions."""
    delta = max_km / 111.0  # rough km-to-degree conversion
    new_lat = max(-90.0, min(90.0, lat + random.uniform(-delta, delta)))
    new_lon = max(-180.0, min(180.0, lon + random.uniform(-delta, delta)))
    return new_lat, new_lon


def generate_transaction(user, merchant, date_obj, force_fraud=False):
    is_fraud = force_fraud

    if is_fraud:
        # --- inject correlated fraud signal ---
        amount = user["avg_amount"] + user["std_amount"] * random.uniform(4, 9)
        ts = random_timestamp_on(date_obj, hour_start=0, hour_end=23)  # ignores user's normal active hours

        if random.random() < 0.75:
            device_id = str(uuid.uuid4())  # brand-new, unseen device (strong signal)
        else:
            device_id = random.choice(user["known_devices"])  # subtle fraud, known device

        lat = max(-90.0, min(90.0, user["home_lat"] + random.uniform(-40, 40)))
        lon = max(-180.0, min(180.0, user["home_lon"] + random.uniform(-40, 40)))
        ip_country = random.choice([c for c in COUNTRIES if c != user["home_country"]])
        billing_country = user["home_country"]
        payment_method = user["preferred_payment"] if random.random() < 0.5 else random.choice(PAYMENT_METHODS)
    else:
        # --- legitimate transaction, with natural variance ---
        amount = max(1.0, random.gauss(user["avg_amount"], user["std_amount"]))
        ts = random_timestamp_on(date_obj, user["active_hour_start"], user["active_hour_end"])
        device_id = random.choice(user["known_devices"])
        lat, lon = jitter_location(user["home_lat"], user["home_lon"])
        ip_country = user["home_country"]
        billing_country = user["home_country"]
        payment_method = user["preferred_payment"]

        # rare legit outlier — keeps the model honest, prevents a trivially
        # separable dataset where "unusual amount" always equals fraud
        if random.random() < 0.03:
            amount *= random.uniform(2, 4)

    return {
        "TransactionID": str(uuid.uuid4()),
        "UserID": user["user_id"],
        "MerchantID": merchant["merchant_id"],
        "DeviceID": device_id,
        "TransactionAmount": round(amount, 2),
        "TransactionDate": ts.isoformat(),
        "PaymentMethod": payment_method,
        "DeviceType": random.choice(DEVICE_TYPES),
        "IP_Address": random_ip(),
        "LocationLat": round(lat, 6),
        "LocationLon": round(lon, 6),
        "IP_Country": ip_country,
        "BillingCountry": billing_country,
        "IsFraud": is_fraud,
    }


def generate_daily_batch(date_obj, num_transactions, users, merchants):
    transactions = []
    num_fraud = int(num_transactions * FRAUD_RATE)
    num_legit = num_transactions - num_fraud

    # merchant weighting for legit transactions: favor low-risk merchants,
    # since fraud disproportionately routes through high-risk merchants
    legit_weights = [1.0 / (m["base_fraud_rate"] + 0.01) for m in merchants]

    for _ in range(num_legit):
        user = random.choice(users)
        merchant = random.choices(merchants, weights=legit_weights, k=1)[0]
        transactions.append(generate_transaction(user, merchant, date_obj, force_fraud=False))

    # fraud clustering: some fraud happens as velocity bursts from the same
    # user in a short window, feeding the velocity-check feature downstream
    fraud_generated = 0
    while fraud_generated < num_fraud:
        user = random.choice(users)
        merchant = random.choice(merchants)
        burst_size = 1 if random.random() < 0.6 else random.randint(2, 4)
        for _ in range(min(burst_size, num_fraud - fraud_generated)):
            transactions.append(generate_transaction(user, merchant, date_obj, force_fraud=True))
            fraud_generated += 1

    random.shuffle(transactions)
    return transactions


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(transactions, date_obj, output_dir):
    day_dir = Path(output_dir) / f"date={date_obj.isoformat()}"
    day_dir.mkdir(parents=True, exist_ok=True)
    out_path = day_dir / f"transactions_{date_obj.isoformat()}.csv"

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(transactions)

    return out_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate a daily batch of synthetic fraud-detection transactions as CSV."
    )
    parser.add_argument(
        "--date", type=str, default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Business date for this batch, format YYYY-MM-DD",
    )
    parser.add_argument(
        "--num-transactions", type=int, default=5000,
        help="Number of transactions to generate for this date",
    )
    parser.add_argument(
        "--output-dir", type=str, default="./data",
        help="Root output directory (a date=YYYY-MM-DD subfolder is created inside it)",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Optional random seed, for reproducible batches (useful for testing)",
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    date_obj = datetime.strptime(args.date, "%Y-%m-%d").date()

    print(f"Building reference pools ({NUM_USERS} users, {NUM_MERCHANTS} merchants)...")
    users = build_user_pool(NUM_USERS)
    merchants = build_merchant_pool(NUM_MERCHANTS)

    print(f"Generating {args.num_transactions} transactions for {date_obj}...")
    transactions = generate_daily_batch(date_obj, args.num_transactions, users, merchants)

    out_path = write_csv(transactions, date_obj, args.output_dir)
    fraud_count = sum(1 for t in transactions if t["IsFraud"])

    print(f"Wrote {len(transactions)} transactions to {out_path}")
    print(f"Fraud transactions: {fraud_count} ({fraud_count / len(transactions):.2%})")


if __name__ == "__main__":
    main()