import os
import pandas as pd
import json

TABLE_DIR = os.path.join(os.getcwd(), "data", "raw", "csv")
SCHEMA_DIR = os.path.join(os.getcwd(), "data", "raw", "schemas")
os.makedirs(SCHEMA_DIR, exist_ok=True)

csv_files = []
for root, dirs, files in os.walk(TABLE_DIR):
    for f in files:
        if f.endswith(".csv"):
            csv_files.append(os.path.join(root, f))

print(f"Found {len(csv_files)} CSV files.")

success_count = 0
fail_count = 0

for csv_file in csv_files:
    try:
        df = pd.read_csv(csv_file, on_bad_lines='skip')  # Problematische Zeilen überspringen
        schema = {
            "columns": [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
        }
        base_name = os.path.basename(csv_file).replace(".csv", ".json")
        schema_path = os.path.join(SCHEMA_DIR, base_name)
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)
        success_count += 1
    except Exception as e:
        print(f"Failed to process {csv_file}: {e}")
        fail_count += 1

print(f"Schemas generated for {success_count} tables. Failed: {fail_count}")
print(f"Schemas are in {SCHEMA_DIR}")
