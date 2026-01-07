import pandas as pd
from typing import Union, List

from scripts.operations import find_columns_like, filter_rows_contains, to_numeric_series


def baseline_predict(question: str, tables: Union[pd.DataFrame, List[pd.DataFrame]]) -> str:
    """
    Baseline v0:
    - If tables is a list -> union it (temporary behavior)
    - Heuristic for simple comparison questions like "which is deeper X or Y?"
    """
    if isinstance(tables, list):
        df = pd.concat(tables, ignore_index=True, sort=False)
    else:
        df = tables

    q = (question or "").lower().strip()
    if df is None or df.empty:
        return ""

    # Heuristic: comparison between two entities
    # Example: "which is deeper, lake tuz or lake palas tuzla?"
    if "which is" in q and " or " in q:
        # try to find numeric attribute columns
        num_cols = find_columns_like(df, ["depth", "height", "score", "points", "population", "area", "length"])
        if not num_cols:
            return ""

        value_col = num_cols[0]

        # guess an entity/name column
        name_cols = find_columns_like(df, ["name", "lake", "city", "team", "player", "country", "title"])
        if not name_cols:
            name_cols = [df.columns[0]]  # fallback

        name_col = name_cols[0]

        # naive split by " or "
        parts = q.split(" or ")
        if len(parts) >= 2:
            left = parts[0].split(",")[-1].strip()
            right = parts[1].replace("?", "").strip()

            sub = df.copy()
            sub = filter_rows_contains(sub, name_col, [left, right])

            if sub.empty:
                return ""

            sub["_num"] = to_numeric_series(sub, value_col)
            sub = sub.dropna(subset=["_num"])
            if sub.empty:
                return ""

            # pick max by default
            best = sub.sort_values("_num", ascending=False).iloc[0]
            return str(best.get(name_col, ""))

    return ""
