from pathlib import Path
from typing import List

import pandas as pd
from owid import catalog

CURRENT_DIR = Path(__file__).parent
VERSION = CURRENT_DIR.name


def gather_sources_from_tables(
    tables: List[catalog.Table],
) -> List[catalog.meta.Source]:
    """Gather unique sources from the metadata.dataset of each table in a list of tables.

    Note: To check if a source is already listed, only the name of the source is considered (not the description or any
    other field in the source).

    Parameters
    ----------
    tables : list
        List of tables with metadata.

    Returns
    -------
    known_sources : list
        List of unique sources from all tables.

    """
    # Initialise list that will gather all unique metadata sources from the tables.
    known_sources: List[catalog.meta.Source] = []
    for table in tables:
        # Get list of sources of the dataset of current table.
        table_sources = table.metadata.dataset.sources
        # Go source by source of current table, and check if its name is not already in the list of known_sources.
        for source in table_sources:
            # Check if this source's name is different to all known_sources.
            if all([source.name != known_source.name for known_source in known_sources]):
                # Add the new source to the list.
                known_sources.append(source)

    return known_sources


def combine_two_overlapping_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, index_columns: List[str]) -> pd.DataFrame:
    """Combine two dataframes that may have identical columns, prioritizing the first one.

    Both dataframes must have a dummy index (if not, use reset_index() on both of them).
    The columns to be considered as index should be declared in index_columns.

    Suppose you have two dataframes, df1 and df2, both having columns "col_a" and "col_b", and we want to create a
    combined dataframe with the union of rows and columns, and, on the overlapping elements, prioritize df1 values.
    To do this, you could:
    * Merge the dataframes. But then the result would have columns "col_a_x", "col_a_y", "col_b_x", and "col_b_y".
    * Concatenate them and then drop duplicates (for example keeping the last repetition). This works, but, if df1 has
    nans then we would keep those nans.
    To solve these problems, this function will not create new columns, and will prioritize df1 **only if it has data**,
    and otherwise use values from df2.

    Parameters
    ----------
    df1 : pd.DataFrame
        First dataframe (the one that has priority).
    df2 : pd.DataFrame
        Second dataframe.
    index_columns : list
        Columns (that must be present in both dataframes) that should be treated as index (e.g. ["country", "year"]).

    Returns
    -------
    combined : pd.DataFrame
        Combination of the two dataframes.

    """
    # Find columns of data (those that are not index columns).
    df1_columns = df1.columns.tolist()
    df2_columns = df2.columns.tolist()
    common_columns = [column for column in df1_columns if column not in index_columns] + [
        column for column in df2_columns if column not in df1_columns
    ]

    # Go column by column, concatenate, remove nans, and then keep df1 version on duplicated rows.
    # Note: There may be a faster, simpler way to achieve this.
    combined = pd.DataFrame({column: [] for column in index_columns})
    for variable in common_columns:
        _df1 = pd.DataFrame()
        _df2 = pd.DataFrame()
        if variable in df1.columns:
            _df1 = df1[index_columns + [variable]].dropna(subset=variable)
        if variable in df2.columns:
            _df2 = df2[index_columns + [variable]].dropna(subset=variable)
        _combined = pd.concat([_df1, _df2], ignore_index=True)
        # On rows where both datasets overlap, give priority to df1.
        _combined = _combined.drop_duplicates(subset=index_columns, keep="first")
        # Add the current variable to the combined dataframe.
        combined = pd.merge(combined, _combined, on=index_columns, how="outer")

    return combined
