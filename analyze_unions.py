import os
import json
import csv
import sqlite3

# -------------------------------------------------
# Paths: resolve relative to THIS script location
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TRAIN_JSON = os.path.join(BASE_DIR, "train_spider.json")
DEV_JSON = os.path.join(BASE_DIR, "dev.json")
TABLES_JSON = os.path.join(BASE_DIR, "tables.json")
DB_ROOT = os.path.join(BASE_DIR, "database")  # optional, only needed if you later execute SQL

# -------------------------------------------------
# Load Spider files
# -------------------------------------------------
with open(TRAIN_JSON, "r", encoding="utf-8") as f:
    train = json.load(f)

with open(DEV_JSON, "r", encoding="utf-8") as f:
    dev = json.load(f)

with open(TABLES_JSON, "r", encoding="utf-8") as f:
    tables_json = json.load(f)

# db_id -> list of table names (original)
db_to_table_names = {db["db_id"]: db["table_names_original"] for db in tables_json}

# -------------------------------------------------
# AST helpers
# -------------------------------------------------
def count_unions_ast(sql_ast) -> int:
    """Count how many UNION operators appear (chained) in Spider AST."""
    if not isinstance(sql_ast, dict):
        return 0

    count = 0
    if sql_ast.get("union") is not None:
        count += 1
        count += count_unions_ast(sql_ast["union"])

    # UNIONs can also appear nested inside except/intersect branches
    if sql_ast.get("except") is not None:
        count += count_unions_ast(sql_ast["except"])
    if sql_ast.get("intersect") is not None:
        count += count_unions_ast(sql_ast["intersect"])

    return count

def has_union_ast(sql_ast) -> bool:
    return count_unions_ast(sql_ast) > 0

def has_join_ast(sql_ast) -> bool:
    """
    In Spider AST, joins usually show up as non-empty from.conds.
    Recurse to catch joins in union/except/intersect parts too.
    """
    if not isinstance(sql_ast, dict):
        return False

    from_part = sql_ast.get("from")
    if isinstance(from_part, dict) and from_part.get("conds"):
        return True

    for key in ["union", "except", "intersect"]:
        if sql_ast.get(key) is not None and has_join_ast(sql_ast[key]):
            return True

    return False

def extract_table_ids(sql_ast) -> set[int]:
    """Collect all table_unit ids used in FROM across the whole set-operation tree."""
    ids = set()
    if not isinstance(sql_ast, dict):
        return ids

    from_part = sql_ast.get("from")
    if isinstance(from_part, dict):
        for tu in from_part.get("table_units", []):
            # Example: ["table_unit", 6]
            if isinstance(tu, list) and len(tu) == 2 and tu[0] == "table_unit":
                ids.add(tu[1])

    for key in ["union", "except", "intersect"]:
        if sql_ast.get(key) is not None:
            ids |= extract_table_ids(sql_ast[key])

    return ids

def extract_tables_from_ast(sql_ast, db_id: str) -> list[str]:
    """Map table_unit ids -> table names using tables.json."""
    table_names = db_to_table_names.get(db_id, [])
    ids = extract_table_ids(sql_ast)
    out = []
    for i in sorted(ids):
        if 0 <= i < len(table_names):
            out.append(table_names[i])
    return out

# -------------------------------------------------
# Collect all UNION queries (JOIN allowed)
# -------------------------------------------------
all_examples = [("train", x) for x in train] + [("dev", x) for x in dev]
union_any = [(split, ex) for split, ex in all_examples if has_union_ast(ex["sql"])]

union_with_join = [(s, ex) for s, ex in union_any if has_join_ast(ex["sql"])]
union_without_join = [(s, ex) for s, ex in union_any if not has_join_ast(ex["sql"])]

print("All UNION queries (JOIN allowed):", len(union_any))
print("  UNION queries WITH JOIN:", len(union_with_join))
print("  UNION queries WITHOUT JOIN:", len(union_without_join))

multi_union = [(s, ex) for s, ex in union_any if count_unions_ast(ex["sql"]) > 1]
print("Multi-UNION (chained UNION) queries:", len(multi_union))

# -------------------------------------------------
# Print a few examples (optional)
# -------------------------------------------------
print("\n--- Examples (first 5 UNION queries) ---")
for i, (split, ex) in enumerate(union_any[:5], 1):
    print(f"\n#{i} [{split}] db_id={ex['db_id']}")
    print("UNION count:", count_unions_ast(ex["sql"]))
    print("Has JOIN:", has_join_ast(ex["sql"]))
    print("Question:", ex["question"])
    print("SQL:", ex["query"])
    print("Tables:", extract_tables_from_ast(ex["sql"], ex["db_id"]))

# -------------------------------------------------
# Export to CSV
# -------------------------------------------------
out_path = os.path.join(BASE_DIR, "spider_union_all_including_joins.csv")
with open(out_path, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow([
        "id", "split", "db_id",
        "union_count", "has_join",
        "tables_used",
        "question", "sql"
    ])

    for i, (split, ex) in enumerate(union_any, 1):
        w.writerow([
            i,
            split,
            ex["db_id"],
            count_unions_ast(ex["sql"]),
            int(has_join_ast(ex["sql"])),
            ", ".join(extract_tables_from_ast(ex["sql"], ex["db_id"])),
            ex["question"],
            ex["query"],
        ])

print("\nWrote CSV:", out_path)

# -------------------------------------------------
# OPTIONAL: If you want to execute SQL later, keep these helpers.
# Not used by default.
# -------------------------------------------------
def get_db_path(db_id: str) -> str:
    return os.path.join(DB_ROOT, db_id, f"{db_id}.sqlite")

def run_sql(db_id: str, sql: str, limit_rows: int = 5):
    db_path = get_db_path(db_id)
    if not os.path.exists(db_path):
        return f"[DB not found] {db_path}", None

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchmany(limit_rows)
        conn.close()
        return None, rows
    except Exception as e:
        return str(e), None
