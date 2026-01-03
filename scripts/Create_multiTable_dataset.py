import os
import csv
import json
from pathlib import Path
from typing import List, Dict, Tuple


# =========================
# CONFIG (EDIT THESE)
# =========================
WTQ_TSV = Path("Data/raw/data/training.tsv")  # <-- your WTQ tsv
TABLES_ROOT = Path("Data/raw/CSV")            # <-- folder that contains 200-csv/, 201-csv/, ...
OUT_TABLES_DIR = Path("Data/processed/multitable/tables")
OUT_DATASET_JSON = Path("Data/processed/multitable/wtq_multitable.json")


# =========================
# SPLIT POLICY (your rules)
# =========================
def decide_num_splits(num_rows: int) -> int:
    """
    num_rows = number of data rows (excluding header)
    Your rule:
      8 rows -> 2 tables
      14 -> 3
      20 -> 4
      40 -> 5
      60 -> 6
      70 -> 7
      80 -> 8
      90 -> 9
      100 -> 10
    For < 8 rows we keep it as 1 table (otherwise you create ultra-tiny splits).
    """
    if num_rows >= 100:
        return 10
    if num_rows >= 90:
        return 9
    if num_rows >= 80:
        return 8
    if num_rows >= 70:
        return 7
    if num_rows >= 60:
        return 6
    if num_rows >= 40:
        return 5
    if num_rows >= 20:
        return 4
    if num_rows >= 14:
        return 3
    if num_rows >= 8:
        return 2
    return 1


# =========================
# IO HELPERS
# =========================
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_wtq_tsv(tsv_path: Path) -> List[Dict]:
    """
    WTQ training.tsv format (as you showed):
      id  utterance  context  targetValue
    """
    if not tsv_path.exists():
        raise FileNotFoundError(f"WTQ TSV not found: {tsv_path}")

    examples = []
    with tsv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        # expected keys: id, utterance, context, targetValue
        for row in reader:
            examples.append({
                "id": row["id"],
                "question": row["utterance"],
                "context": row["context"],       # e.g. csv/204-csv/590.csv
                "answer": row["targetValue"],
            })
    return examples


def robust_read_table(csv_path: Path) -> Tuple[List[str], List[List[str]]]:
    """
    Reads a CSV robustly even if some lines are messy.
    Strategy:
      - Try csv.Sniffer to detect delimiter
      - Fall back to comma
      - Normalize row lengths to header length (pad/truncate)
    Returns: (header, rows)
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Table file not found: {csv_path}")

    text = csv_path.read_text(encoding="utf-8", errors="replace")
    sample = text[:5000]

    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ","

    lines = text.splitlines()
    reader = csv.reader(lines, delimiter=delimiter)

    all_rows = list(reader)
    if not all_rows:
        return [], []

    header = all_rows[0]
    data = all_rows[1:]

    # normalize row lengths
    hlen = len(header)
    normalized = []
    for r in data:
        if len(r) < hlen:
            r = r + [""] * (hlen - len(r))
        elif len(r) > hlen:
            r = r[:hlen]
        normalized.append(r)

    return header, normalized


def write_csv(path: Path, header: List[str], rows: List[List[str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def split_rows_evenly(rows: List[List[str]], k: int) -> List[List[List[str]]]:
    """
    Split rows into k chunks as evenly as possible.
    """
    n = len(rows)
    if k <= 1 or n == 0:
        return [rows]

    base = n // k
    rem = n % k

    chunks = []
    start = 0
    for i in range(k):
        size = base + (1 if i < rem else 0)
        end = start + size
        chunks.append(rows[start:end])
        start = end

    # If some chunk accidentally becomes empty (can happen if k > n),
    # collapse to max n chunks of size 1.
    chunks = [c for c in chunks if len(c) > 0]
    return chunks


def resolve_context_to_file(context: str) -> Path:
    """
    WTQ context looks like: csv/204-csv/590.csv
    Your actual files are under: Data/raw/CSV/204-csv/590.csv
    So we drop the leading 'csv/' and join with TABLES_ROOT.
    """
    context = context.replace("\\", "/").strip()
    if context.startswith("csv/"):
        rel = context[len("csv/"):]
    else:
        rel = context
    return TABLES_ROOT / rel


# =========================
# MAIN PIPELINE
# =========================
def build_multitable_dataset() -> None:
    print("=== WTQ -> Multi-table construction (row-splitting) ===")

    ensure_dir(OUT_TABLES_DIR)
    ensure_dir(OUT_DATASET_JSON.parent)

    wtq = load_wtq_tsv(WTQ_TSV)
    print(f"Loaded {len(wtq)} WTQ examples from {WTQ_TSV}")

    out_entries = []
    missing_tables = 0
    processed = 0

    for ex in wtq:
        table_file = resolve_context_to_file(ex["context"])

        try:
            header, rows = robust_read_table(table_file)
        except FileNotFoundError:
            missing_tables += 1
            continue

        num_rows = len(rows)
        k = decide_num_splits(num_rows)

        # Split rows
        chunks = split_rows_evenly(rows, k)

        # Save split tables
        # Make stable unique folder per original table
        # Example: csv/204-csv/590.csv -> 204-csv_590
        safe_name = ex["context"].replace("\\", "/").replace("csv/", "")
        safe_name = safe_name.replace("/", "_").replace(".csv", "")

        split_paths = []
        for i, chunk in enumerate(chunks, start=1):
            out_csv = OUT_TABLES_DIR / safe_name / f"part_{i:02d}.csv"
            write_csv(out_csv, header, chunk)
            split_paths.append(str(out_csv).replace("\\", "/"))

        out_entries.append({
            "id": ex["id"],
            "question": ex["question"],
            "tables": split_paths,                 # <-- THIS is your multi-table input
            "source_table": ex["context"],         # original WTQ context
            "answer": ex["answer"],
            "num_splits": len(split_paths),
            "num_rows_original": num_rows
        })

        processed += 1
        if processed % 2000 == 0:
            print(f"Processed {processed}/{len(wtq)}")

    with OUT_DATASET_JSON.open("w", encoding="utf-8") as f:
        json.dump(out_entries, f, indent=2, ensure_ascii=False)

    print("Done.")
    print(f"Created {len(out_entries)} multi-table examples.")
    print(f"Missing original tables: {missing_tables}")
    if out_entries:
        print("Example entry:")
        print(json.dumps(out_entries[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    build_multitable_dataset()
