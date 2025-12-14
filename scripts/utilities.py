def find_compatible_table(base_table_id, schemas, min_schema_overlap=0.5):
    """
    Dummy function to find a compatible table based on schema overlap.
    Replace with your actual logic for multi-table selection.
    """
    for table_id, schema in schemas.items():
        if table_id != base_table_id:
            # Example logic: randomly accept some table
            import random
            if random.random() > 0.5:
                return table_id
    return None
