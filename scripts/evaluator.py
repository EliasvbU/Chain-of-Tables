from typing import List, Any


def normalize(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip().lower()


def evaluate_predictions(preds: List[Any], gts: List[Any]) -> str:
    assert len(preds) == len(gts)

    correct = 0
    for p, gt in zip(preds, gts):
        if normalize(p) == normalize(gt):
            correct += 1

    acc = correct / len(preds) if preds else 0.0
    return f"Examples: {len(preds)}\nExact Match Accuracy: {acc:.4f}"
