import random
from scripts.utilities import schema_overlap


def find_compatible_table(
    base_table,
    schemas,
    min_overlap=0.5,
    max_trials=50
):
    base_schema = schemas.get(base_table)
    if base_schema is None:
        return None

    candidates = list(schemas.keys())
    random.shuffle(candidates)

    for cand in candidates[:max_trials]:
        if cand == base_table:
            continue

        overlap = schema_overlap(base_schema, schemas[cand])
        if overlap >= min_overlap:
            return cand

    return None
