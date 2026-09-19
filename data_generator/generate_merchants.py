"""
Generates merchant master data as daily snapshots.
Merchant status (Active/Suspended/Reinstated/Closed) and rating change over
time -> the core SCD2 tracking target for merchants. A suspended merchant's
historical orders still need to reflect "suspended" as-of that order date.
"""
import csv
import os
import random
from faker import Faker
from config import SEED, NUM_MERCHANTS, DAY_DATES, CITIES, CUISINE_CATEGORIES

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "raw_data", "merchants")

STATUSES = ["Active", "Suspended", "Closed"]
STATUS_CHANGE_PROB_PER_DAY = 0.04
RATING_DRIFT_PROB_PER_DAY = 0.20

# Build the same location_id list as generate_reference_data.py
LOCATION_IDS = []
loc_num = 1
for city, state in CITIES:
    for zone in ["Downtown", "Suburbs"]:
        LOCATION_IDS.append(f"LOC{str(loc_num).zfill(3)}")
        loc_num += 1

CUISINE_IDS = [f"CUIS{str(i).zfill(2)}" for i in range(1, len(CUISINE_CATEGORIES) + 1)]


def build_initial_merchants():
    merchants = {}
    for i in range(1, NUM_MERCHANTS + 1):
        merch_id = f"MERCH{str(i).zfill(5)}"
        merchants[merch_id] = {
            "merchant_id": merch_id,
            "merchant_name": fake.company(),
            "cuisine_category_id": random.choice(CUISINE_IDS),
            "location_id": random.choice(LOCATION_IDS),
            "status": "Active",
            "rating": round(random.uniform(3.3, 4.9), 2),
            "onboarded_date": fake.date_between(start_date="-2y", end_date="-30d").isoformat(),
        }
    return merchants


def evolve_merchants(merchants, day_index):
    for m in merchants.values():
        if day_index == 0:
            continue
        if m["status"] == "Closed":
            continue  # closed merchants stay closed

        if RATING_DRIFT_PROB_PER_DAY and random.random() < RATING_DRIFT_PROB_PER_DAY:
            delta = round(random.uniform(-0.15, 0.15), 2)
            m["rating"] = max(1.0, min(5.0, round(m["rating"] + delta, 2)))

        if random.random() < STATUS_CHANGE_PROB_PER_DAY:
            if m["status"] == "Active":
                m["status"] = random.choice(["Suspended", "Suspended", "Closed"])
            elif m["status"] == "Suspended":
                m["status"] = random.choice(["Active", "Closed"])
    return merchants


def write_snapshot(merchants, snapshot_date):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"merchants_{snapshot_date.isoformat()}.csv")
    fieldnames = [
        "merchant_id", "merchant_name", "cuisine_category_id", "location_id",
        "status", "rating", "onboarded_date", "snapshot_date",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in merchants.values():
            row = dict(m)
            row["snapshot_date"] = snapshot_date.isoformat()
            writer.writerow(row)
    print(f"Wrote {len(merchants)} merchant rows -> {path}")


if __name__ == "__main__":
    merchants = build_initial_merchants()
    for day_index, snapshot_date in enumerate(DAY_DATES):
        merchants = evolve_merchants(merchants, day_index)
        write_snapshot(merchants, snapshot_date)
