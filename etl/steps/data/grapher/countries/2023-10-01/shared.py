from typing import List, Optional

import numpy as np
import pandas as pd
from owid.catalog import Table


def fill_gaps_with_zeroes(
    tb: Table, columns: List[str], cols_use_range: Optional[List[str]] = None, use_nan: bool = False
) -> Table:
    """Fill missing values with zeroes.

    Makes sure all combinations of `columns` are present. If not present in the original table, then it is added with zero value.
    """
    # Build grid with all possible values
    values_possible = []
    for col in columns:
        if cols_use_range and col in cols_use_range:
            value_range = np.arange(tb[col].min(), tb[col].max() + 1)
            values_possible.append(value_range)
        else:
            values_possible.append(set(tb[col]))

    # Reindex
    new_idx = pd.MultiIndex.from_product(values_possible, names=columns)
    tb = tb.set_index(columns).reindex(new_idx).reset_index()

    # Fill zeroes
    if not use_nan:
        columns_fill = [col for col in tb.columns if col not in columns]
        tb[columns_fill] = tb[columns_fill].fillna(0)
    return tb
