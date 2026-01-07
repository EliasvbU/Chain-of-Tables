import json
from pathlib import Path
from typing import List, Dict, Any


def load_multitable_dataset(path: Path, limit: int | None = None) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Dataset JSON not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if limit is not None:
        data = data[:limit]

    # basic sanity checks
    for ex in data[:5]:
        if "question" not in ex or "tables" not in ex:
            raise ValueError("Dataset entries must contain 'question' and 'tables' fields.")

    return data
