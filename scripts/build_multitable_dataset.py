# scripts/build_multitable_pairs.py
import random

def find_compatible_table(base_table, schemas, min_schema_overlap=0.5):
    """
    Find a table that shares at least min_schema_overlap columns with base_table.
    Returns the table id or None if no compatible table found.
    """
    base_schema = set(schemas.get(base_table, []))
    candidates = []

    for table_id, schema_cols in schemas.items():
        if table_id == base_table:
            continue
        overlap = base_schema.intersection(set(schema_cols))
        if len(overlap) / max(len(base_schema), 1) >= min_schema_overlap:
            candidates.append(table_id)

    if not candidates:
        return None
    return random.choice(candidates)


def build_multitable_dataset(
    wtq_examples,
    schemas,
    tables_per_example=2,
    min_schema_overlap=0.5
):
    """
    Builds a multi-table dataset from single-table WTQ examples.

    Parameters:
    - wtq_examples: list of WTQ examples (dict with id, question, table_id, answer)
    - schemas: dict mapping table_id -> list of column names
    - tables_per_example: how many tables per multi-table example
    - min_schema_overlap: minimum proportion of columns that must overlap for table pairing

    Returns:
    - List of examples with fields: id, question, tables, relevant_table, answer
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
            "answer": ex.get("answer")
        })

    return data
