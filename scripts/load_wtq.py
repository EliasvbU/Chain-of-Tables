import csv
import os

def load_wtq(tsv_path):
    if not os.path.exists(tsv_path):
        raise FileNotFoundError(f"{tsv_path} not found")

    examples = []
    with open(tsv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            examples.append({
                "id": row["id"],
                "question": row["utterance"],
                "table_id": row["context"],
                "answer": row["targetValue"]
            })
    return examples