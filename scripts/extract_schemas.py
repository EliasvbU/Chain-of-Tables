import os
import json
import pandas as pd

CSV_ROOT = "Data/raw/csv"
OUT_PATH = "Data/processed/schemas.json"


def extract_schemas():
    schemas = {}

    for root, _, files in os.walk(CSV_ROOT):
        for f in files:
            if not f.endswith(".csv"):
                continue

            csv_path = os.path.join(root, f)

            try:
                df = pd.read_csv(csv_path, nrows=0)
                cols = [c.strip().lower() for c in df.columns]

                rel_path = os.path.relpath(csv_path, "Data/raw")
                schemas[rel_path] = cols

            except Exception as e:
                print(f"[WARN] Skipping {csv_path}: {e}")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(schemas, f, indent=2)

    print(f"Extracted schemas for {len(schemas)} tables")


if __name__ == "__main__":
    extract_schemas()
