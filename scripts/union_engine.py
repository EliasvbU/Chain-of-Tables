import pandas as pd
from typing import List


def union_tables(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Union = row-wise concatenation.
    We keep all columns (schema-light); missing columns become NaN.
    """
    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True, sort=False)
