import argparse
from pathlib import Path

from scripts.config import DEFAULT_DATASET_JSON
from scripts.dataset_loader import load_multitable_dataset
from scripts.table_loader import load_tables
from scripts.union_engine import union_tables
from scripts.baseline_reasoner import baseline_predict
from scripts.evaluator import evaluate_predictions


def run_pipeline(dataset_json: Path, limit: int | None = None, union_mode: str = "all"):
    print("=== Chain-of-Tables: Execution Pipeline ===")
    print(f"Dataset: {dataset_json}")
    print(f"Union mode: {union_mode}")

    dataset = load_multitable_dataset(dataset_json, limit=limit)
    print(f"Loaded {len(dataset)} examples")

    preds = []
    gts = []

    for i, ex in enumerate(dataset):
        # 1) Load tables
        dfs = load_tables(ex["tables"])

        # 2) Union policy
        # union_mode="all" -> union everything immediately
        # union_mode="none" -> reasoner gets list of dfs
        if union_mode == "all":
            merged = union_tables(dfs)
            pred = baseline_predict(ex["question"], merged)
        elif union_mode == "none":
            pred = baseline_predict(ex["question"], dfs)  # baseline can ignore union if you implement it
        else:
            raise ValueError("union_mode must be 'all' or 'none'")

        preds.append(pred)
        gts.append(ex.get("answer"))

        if (i + 1) % 200 == 0:
            print(f"Processed {i+1}/{len(dataset)}")

    # 3) Evaluate
    report = evaluate_predictions(preds, gts)
    print("\n=== Results ===")
    print(report)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, default=str(DEFAULT_DATASET_JSON))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--union_mode", type=str, default="all", choices=["all", "none"])
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(Path(args.dataset), limit=args.limit, union_mode=args.union_mode)
