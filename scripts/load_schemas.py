import os
import json

def load_schemas(schema_folder="data/raw/schemas"):
    """
    Loads table schemas from a folder.
    Returns a dictionary: {table_id: schema_info}
    """
    schemas = {}
    for filename in os.listdir(schema_folder):
        if filename.endswith(".json"):
            table_id = filename.replace(".json", "")
            with open(os.path.join(schema_folder, filename), "r", encoding="utf-8") as f:
                schemas[table_id] = json.load(f)
    return schemas
