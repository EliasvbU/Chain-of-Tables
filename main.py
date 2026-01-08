from datasets import load_dataset

dataset = load_dataset("xlangai/spider")

def is_union_only(sql):
    sql = sql.upper()
    return "UNION" in sql and " JOIN " not in sql

union_only = []

for split in ["train", "validation"]:
    for ex in dataset[split]:
        if is_union_only(ex["query"]):
            union_only.append(ex)

print("Gefundene UNION-only Fragen:", len(union_only))

for i, ex in enumerate(union_only, 1):
    print(f"#{i}")
    print("Frage:", ex["question"])
    print("SQL:", ex["query"])
    print("DB:", ex["db_id"])
    print("-" * 60)

tables = dataset["tables"]

db_schemas = {}

for db in tables:
    db_id = db["db_id"]
    table_names = db["table_names_original"]
    db_schemas[db_id] = table_names

import re

def extract_tables(sql, db_id):
    sql = sql.lower()
    used_tables = []

    for table in db_schemas[db_id]:
        if re.search(rf"\b{table.lower()}\b", sql):
            used_tables.append(table)

    return list(set(used_tables))

for i, ex in enumerate(union_only, 1):
    tables_used = extract_tables(ex["query"], ex["db_id"])

    print(f"#{i}")
    print("Frage:", ex["question"])
    print("SQL:", ex["query"])
    print("DB:", ex["db_id"])
    print("Tabellen:", tables_used)
    print("-" * 70)

import csv

with open("spider_union_only.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "question", "sql", "db_id", "tables"])

    for i, ex in enumerate(union_only, 1):
        tables_used = extract_tables(ex["query"], ex["db_id"])
        writer.writerow([
            i,
            ex["question"],
            ex["query"],
            ex["db_id"],
            ", ".join(tables_used)
        ])

