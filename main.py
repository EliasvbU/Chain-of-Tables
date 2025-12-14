import os
import csv
from scripts.load_schemas import load_schemas
from scripts.build_multitable_datasets import build_multitable_dataset
from scripts.save_json import save_json

def load_wtq(tsv_path):
    """
    Load WikiTableQuestions TSV file into a list of examples.
    Each example is a dict with id, question, table_id, and answer.
    """
    examples = []
    if not os.path.exists(tsv_path):
        raise FileNotFoundError(f"{tsv_path} not found. Make sure the file exists.")

    with open(tsv_path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            examples.append({
                "id": row.get("id", ""),
                "question": row.get("question", ""),
                "table_id": row.get("table_id", ""),
                "answer": row.get("answer", None)
            })
    return examples

def main():
    print("=== Chain-of-Tables Pipeline ===")

    # 1️⃣ Load WikiTableQuestions
    wtq_tsv = "Data/raw/data/training.tsv"
    print("[1] Loading WikiTableQuestions...")
    wtq_examples = load_wtq(wtq_tsv)
    print(f"Loaded {len(wtq_examples)} examples.")

    # 2️⃣ Load schemas
    print("[2] Loading table schemas...")
    schemas = load_schemas()
    print(f"Loaded {len(schemas)} table schemas.")

    # 3️⃣ Build multi-table dataset
    print("[3] Building multi-table dataset...")
    multitable_data = build_multitable_dataset(wtq_examples, schemas, tables_per_example=2)
    print(f"Built {len(multitable_data)} multi-table examples.")

    # 4️⃣ Save to JSON
    print("[4] Saving dataset to JSON...")
    save_json(multitable_data)
    print("Done! Dataset saved to data/processed/multitable_dataset.json")

if __name__ == "__main__":
    main()
