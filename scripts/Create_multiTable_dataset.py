import os
import csv
import json
import random
import string
from pathlib import Path
from typing import List, Dict, Tuple



# CONFIG (EDIT THESE)

WTQ_TSV = Path("Data/raw/data/training.tsv")  # Training wtq tsv path
TABLES_ROOT = Path("Data/raw/CSV")            # WTQ table files root path
OUT_TABLES_DIR = Path("Data/processed/multitable/tables")
OUT_DATASET_JSON = Path("Data/processed/multitable/wtq_multitable.json")



# SPLIT POLICY

def decide_num_splits(num_rows: int) -> int:
    """
    Determine number of table splits based on row count.
    
    Rule:
      - < 10 rows: 1 table
      - >= 10 rows: num_rows // 10, minimum 2, capped at 10 max
    
    Examples:
      - 10 rows: 2 tables
      - 30 rows: 3 tables
      - 50 rows: 5 tables
      - 100 rows: 10 tables
    """
    if num_rows < 10:
        return 1
    else:
        return min(max(2, num_rows // 10), 10)



# IO HELPERS

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


def generate_noise_headers(num_noise: int) -> List[Tuple[str, str]]:
    """Generate random noise column headers with their types.
    Returns list of (header_name, data_type) tuples.
    """
    noise_templates = [
        ("Random_Column_{}", "letters_numbers"),
        ("Noise_Field_{}", "letters"),
        ("Extra_Data_{}", "numbers"),
        ("Dummy_Col_{}", "letters"),
        ("Unused_{}", "empty"),
        ("Filler_{}", "letters"),
        ("Irrelevant_{}", "letters_numbers"),
    ]
    headers = []
    for i in range(num_noise):
        template, data_type = random.choice(noise_templates)
        headers.append((template.format(i + 1), data_type))
    return headers


def add_noise_columns(header: List[str], rows: List[List[str]], num_noise: int) -> Tuple[List[str], List[List[str]]]:
    """Add noise columns to a table."""
    noise_headers_with_types = generate_noise_headers(num_noise)
    noise_header_names = [h for h, _ in noise_headers_with_types]
    new_header = header + noise_header_names
    
    # Add random values for noise columns in each row based on data type
    new_rows = []
    for row in rows:
        noise_values = []
        for _, data_type in noise_headers_with_types:
            if data_type == "letters":
                # Just letters (5-8 characters)
                value = ''.join(random.choices(string.ascii_letters, k=random.randint(5, 8)))
            elif data_type == "numbers":
                # Just numbers
                value = str(random.randint(0, 999))
            elif data_type == "letters_numbers":
                # Mix of letters and numbers (5-8 characters)
                value = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 8)))
            elif data_type == "empty":
                # Empty string
                value = ""
            else:
                value = ""
            noise_values.append(value)
        new_rows.append(row + noise_values)
    
    return new_header, new_rows


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
    Actual files are under: Data/raw/CSV/204-csv/590.csv
    Drop leading 'csv/' and join with TABLES_ROOT.
    """
    context = context.replace("\\", "/").strip()
    if context.startswith("csv/"):
        rel = context[len("csv/"):]
    else:
        rel = context
    return TABLES_ROOT / rel



# MAIN PIPELINE

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

        # Decide which tables get noise headers
        # At least 1 table must have NO noise
        # Randomly select 1-5 tables to add noise (if k > 1)
        tables_with_noise = set()
        if k > 1:
            # Ensure at least one table has no noise
            num_tables_with_noise = min(random.randint(1, 5), k - 1)
            # Randomly select which tables get noise
            all_indices = list(range(k))
            tables_with_noise = set(random.sample(all_indices, num_tables_with_noise))

        # Save split tables
        # Make stable unique folder per original table
        # Example: csv/204-csv/590.csv -> 204-csv_590
        safe_name = ex["context"].replace("\\", "/").replace("csv/", "")
        safe_name = safe_name.replace("/", "_").replace(".csv", "")

        split_paths = []
        for i, chunk in enumerate(chunks, start=1):
            # Determine if this table should have noise
            table_header = header
            table_rows = chunk
            
            if (i - 1) in tables_with_noise:
                # Add 1-3 noise columns to this table
                num_noise_cols = random.randint(1, 3)
                table_header, table_rows = add_noise_columns(header, chunk, num_noise_cols)
            
            out_csv = OUT_TABLES_DIR / safe_name / f"part_{i:02d}.csv"
            write_csv(out_csv, table_header, table_rows)
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
