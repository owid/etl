"""Format HYDE table accordingly.

Data is provided from -10000 to 2023.

Frequency is:
    - -10000 to 1: Every 1000 years
    - 1 to 1000: Every 100 years
    - 1700 to 1950: Every 10 years
    - 1950 to 2023: Every year
"""

from owid.catalog import Table

# Year boundaries
YEAR_HYDE_START = -10000
YEAR_HYDE_END = 2023


def format_hyde(tb: Table) -> Table:
    """Format HYDE table."""
    # Sanity checks IN
    assert tb["year"].min() == YEAR_HYDE_START, f"Unexpected start year for HYDE. Should be {YEAR_HYDE_START}!"
    assert tb["year"].max() == YEAR_HYDE_END, f"Unexpected end year for HYDE. Should be {YEAR_HYDE_END}!"

    # Round population values
    tb["popc_c"] = tb["popc_c"].round()

    # Rename columns, dtypes, sort rows
    columns_rename = {
        "country": "country",
        "year": "year",
        "popc_c": "population",
    }
    tb = (
        tb.rename(columns=columns_rename, errors="raise")[columns_rename.values()]
        .assign(source="hyde")
        .astype(
            {
                "source": "str",
                "country": str,
                "population": "uint64",
                "year": "int64",
            }
        )
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )
    return tb
