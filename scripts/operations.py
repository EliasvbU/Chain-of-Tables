import pandas as pd
from typing import Optional, List


def normalize_str(x):
    if x is None:
        return ""
    return str(x).strip().lower()


def find_columns_like(df: pd.DataFrame, keywords: List[str]) -> List[str]:
    cols = []
    for c in df.columns:
        name = normalize_str(c)
        if any(k in name for k in keywords):
            cols.append(c)
    return cols


def filter_rows_contains(df: pd.DataFrame, col: str, substrings: List[str]) -> pd.DataFrame:
    mask = False
    for s in substrings:
        mask = mask | df[col].astype(str).str.contains(s, case=False, na=False)
    return df[mask]


def to_numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    # convert strings like "12,467" -> 12467
    cleaned = df[col].astype(str).str.replace(",", "", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")
