import csv
import os
import json
import random
from scripts.load_wtq import load_wtq
from scripts.build_multitable_dataset import build_multitable_dataset



# --------------------------------------------------
# Save JSON
# --------------------------------------------------
def save_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# --------------------------------------------------
# MAIN
# --------------------------------------------------
DATA_DIR = "Data"

def main():
    print("=== Chain-of-Tables: Dataset Construction ===")

    # 1. Load WTQ
    wtq_tsv = "Data/raw/data/training.tsv"
    wtq_examples = load_wtq(wtq_tsv)
    print(f"Loaded {len(wtq_examples)} examples")

    # 2. Load schemas
    with open("Data/processed/schemas.json", "r") as f:
        schemas = json.load(f)
    print(f"Loaded schemas for {len(schemas)} tables")

    # 3. Build multi-table dataset
    multitable = build_multitable_dataset(
        wtq_examples,
        schemas,
        2,
        0.0
    )

    print(f"Created {len(multitable)} multi-table examples")

    # 4. Save
    out_path = "Data/processed/multitable_2tables.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(multitable, f, indent=2)

    print("Done.")
    print("Example:")
    print(multitable[0])


if __name__ == "__main__":
    main()
