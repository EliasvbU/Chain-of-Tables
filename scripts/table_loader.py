import pandas as pd
from typing import List, Union


def load_tables(paths: List[str]) -> List[pd.DataFrame]:
    dfs = []
    for p in paths:
        # robust read: handles bad lines better
        df = pd.read_csv(p, dtype=str, encoding="utf-8", on_bad_lines="skip")
        dfs.append(df)
    return dfs
