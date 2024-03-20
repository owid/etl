import numpy as np
import pandas as pd
from owid.catalog import Table


def expand_observations(tb: Table) -> Table:
    """Expand to have a row per (year, country)."""
    # Column for index
    column_idx = ["country", "year"]

    # List of countries
    regions = set(tb["country"])

    # List of possible years
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)

    # New index
    new_idx = pd.MultiIndex.from_product([regions, years], names=column_idx)

    # Add flag
    tb["vdem_obs"] = 1

    # Reset index
    tb = tb.set_index(column_idx, verify_integrity=True).reindex(new_idx).sort_index().reset_index()

    # Update flag
    tb["vdem_obs"] = tb["vdem_obs"].fillna(0)

    # Type of `year`
    tb["year"] = tb["year"].astype("int")
    return tb
