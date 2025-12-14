from scripts.utilities import find_compatible_table
import random

def build_multitable_dataset(
    wtq_examples,
    schemas,
    tables_per_example=2,
    min_schema_overlap=0.5
):
    """
    Construct multi-table examples from single-table questions.
    """
    data = []

    for ex in wtq_examples:
        base = ex["table_id"]
        second = find_compatible_table(base, schemas, min_schema_overlap)

        if second is None:
            continue

        tables = [base, second]
        random.shuffle(tables)

        data.append({
            "id": ex["id"],
            "question": ex["question"],
            "tables": tables,
            "relevant_table": base,
            "answer": ex.get("answer", None)
        })

    return data
