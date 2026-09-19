"""
Generates static reference data: locations (city zones) and cuisine categories.
These are SCD Type 1 dimensions - simple, rarely-changing lookup data.
"""
import csv
import os
from config import CITIES, CUISINE_CATEGORIES

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "raw_data")


def build_locations():
    rows = []
    loc_num = 1
    for city, state in CITIES:
        # 2 zones per city (e.g. Downtown, Suburbs) to give delivery variety
        for zone in ["Downtown", "Suburbs"]:
            rows.append({
                "location_id": f"LOC{str(loc_num).zfill(3)}",
                "city": city,
                "state": state,
                "zone": zone,
            })
            loc_num += 1
    return rows


def build_cuisine_categories():
    return [
        {"category_id": f"CUIS{str(i).zfill(2)}", "category_name": name}
        for i, name in enumerate(CUISINE_CATEGORIES, start=1)
    ]


def write_csv(filename, rows, fieldnames):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {path}")


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    write_csv("locations.csv", build_locations(), ["location_id", "city", "state", "zone"])
    write_csv("cuisine_categories.csv", build_cuisine_categories(), ["category_id", "category_name"])
