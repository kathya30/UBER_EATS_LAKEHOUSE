"""
Generates customer master data as daily snapshots.
Customer tier (Standard/Silver/Gold/Platinum) changes over time based on
order volume -> SCD2 tracking target for customers.
"""
import csv
import os
import random
from faker import Faker
from config import SEED, NUM_CUSTOMERS, DAY_DATES, CITIES

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "raw_data", "customers")

TIERS = ["Standard", "Silver", "Gold", "Platinum"]
TIER_UPGRADE_PROB_PER_DAY = 0.025
ADDRESS_CHANGE_PROB_PER_DAY = 0.01


def build_initial_customers():
    customers = {}
    for i in range(1, NUM_CUSTOMERS + 1):
        cust_id = f"CUST{str(i).zfill(5)}"
        city, state = random.choice(CITIES)
        customers[cust_id] = {
            "customer_id": cust_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "city": city,
            "state": state,
            "tier": "Standard",
            "signup_date": fake.date_between(start_date="-2y", end_date="-30d").isoformat(),
        }
    return customers


def evolve_customers(customers, day_index):
    for cust in customers.values():
        if day_index == 0:
            continue
        if random.random() < TIER_UPGRADE_PROB_PER_DAY:
            idx = TIERS.index(cust["tier"])
            if idx < len(TIERS) - 1:
                cust["tier"] = TIERS[idx + 1]
        if random.random() < ADDRESS_CHANGE_PROB_PER_DAY:
            city, state = random.choice(CITIES)
            cust["city"] = city
            cust["state"] = state
    return customers


def write_snapshot(customers, snapshot_date):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"customers_{snapshot_date.isoformat()}.csv")
    fieldnames = [
        "customer_id", "first_name", "last_name", "email",
        "city", "state", "tier", "signup_date", "snapshot_date",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for cust in customers.values():
            row = dict(cust)
            row["snapshot_date"] = snapshot_date.isoformat()
            writer.writerow(row)
    print(f"Wrote {len(customers)} customer rows -> {path}")


if __name__ == "__main__":
    customers = build_initial_customers()
    for day_index, snapshot_date in enumerate(DAY_DATES):
        customers = evolve_customers(customers, day_index)
        write_snapshot(customers, snapshot_date)
