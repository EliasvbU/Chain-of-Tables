import random
from pathlib import Path


def build_multitable_dataset(wtq_examples, tables_per_example=2):
    all_tables = list({ex["table_id"] for ex in wtq_examples})
    multitable = []

    for ex in wtq_examples:
        base_table = ex["table_id"]

        # pick a random second table (different from base)
        candidates = [t for t in all_tables if t != base_table]
        if not candidates:
            continue

        second_table = random.choice(candidates)
        tables = [base_table, second_table]
        random.shuffle(tables)

        multitable.append({
            "id": ex["id"],
            "question": ex["question"],
            "tables": tables,
            "relevant_table": base_table,
            "answer": ex["answer"]
        })

    return multitable