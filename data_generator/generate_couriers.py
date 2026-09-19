"""
Generates courier master data as daily snapshots.
Courier status (Active/OnDelivery/Offline/Deactivated) and vehicle type
change over time -> SCD2 tracking target for the courier fleet.
"""
import csv
import os
import random
from faker import Faker
from config import SEED, NUM_COURIERS, DAY_DATES, CITIES, VEHICLE_TYPES

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "raw_data", "couriers")

STATUSES = ["Active", "Offline", "Deactivated"]
STATUS_CHANGE_PROB_PER_DAY = 0.25
DEACTIVATE_PROB_PER_DAY = 0.004

LOCATION_IDS = []
loc_num = 1
for city, state in CITIES:
    for zone in ["Downtown", "Suburbs"]:
        LOCATION_IDS.append(f"LOC{str(loc_num).zfill(3)}")
        loc_num += 1


def build_initial_couriers():
    couriers = {}
    for i in range(1, NUM_COURIERS + 1):
        cour_id = f"COUR{str(i).zfill(5)}"
        couriers[cour_id] = {
            "courier_id": cour_id,
            "courier_name": fake.name(),
            "home_location_id": random.choice(LOCATION_IDS),
            "vehicle_type": random.choice(VEHICLE_TYPES),
            "status": "Active",
            "joined_date": fake.date_between(start_date="-540d", end_date="-14d").isoformat(),
        }
    return couriers


def evolve_couriers(couriers, day_index):
    for c in couriers.values():
        if day_index == 0:
            continue
        if c["status"] == "Deactivated":
            continue

        if random.random() < DEACTIVATE_PROB_PER_DAY:
            c["status"] = "Deactivated"
            continue

        if random.random() < STATUS_CHANGE_PROB_PER_DAY:
            c["status"] = random.choice(["Active", "Offline"])

        if random.random() < 0.01:
            c["vehicle_type"] = random.choice(VEHICLE_TYPES)
    return couriers


def write_snapshot(couriers, snapshot_date):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"couriers_{snapshot_date.isoformat()}.csv")
    fieldnames = [
        "courier_id", "courier_name", "home_location_id", "vehicle_type",
        "status", "joined_date", "snapshot_date",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in couriers.values():
            row = dict(c)
            row["snapshot_date"] = snapshot_date.isoformat()
            writer.writerow(row)
    print(f"Wrote {len(couriers)} courier rows -> {path}")


if __name__ == "__main__":
    couriers = build_initial_couriers()
    for day_index, snapshot_date in enumerate(DAY_DATES):
        couriers = evolve_couriers(couriers, day_index)
        write_snapshot(couriers, snapshot_date)
