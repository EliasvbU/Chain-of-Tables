import json
import os

def save_json(data, path="data/processed/multitable_dataset.json"):
    """
    Save data as JSON to the given path.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
