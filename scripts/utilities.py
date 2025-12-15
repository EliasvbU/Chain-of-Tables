def schema_overlap(schema_a, schema_b):
    if not schema_a or not schema_b:
        return 0.0

    set_a = set(schema_a)
    set_b = set(schema_b)

    return len(set_a & set_b) / len(set_a)
