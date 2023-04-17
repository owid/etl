from typing import Dict, List, Optional

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

log = get_logger()

# Columns required for the main table
COLUMNS_COMMON = [
    "conflict_name",
    "conflict_participants",
    "start_year",
    "end_year",
    "continent",
    "total_deaths",
]
# Extra columns used if available
COLUMNS_EXTRA = [
    "type_of_conflict",
    "deaths_military_direct",
    "deaths_military_indirect",
    "deaths_military_unclear",
    "deaths_unclear_direct",
    "deaths_unclear_indirect",
    "deaths_unclear_unclear",
    "deaths_civilian_direct",
    "deaths_civilian_indirect",
    "deaths_civilian_unclear",
    "casualties_military_direct",
    "casualties_military_indirect",
    "casualties_military_unclear",
    "casualties_unclear_direct",
    "casualties_unclear_indirect",
    "casualties_unclear_unclear",
    "casualties_civilian_direct",
    "casualties_civilian_indirect",
    "casualties_civilian_unclear",
]
# Columns required for the notes table
COLUMNS_COMMON_NOTES = [
    "notes_inc_key_quote",
]
_INDEX_COLNAME_AUX = "index"


def make_tables(df: pd.DataFrame, short_name: str) -> List[Table]:
    """Make tables.

    There are three tables created:
        - Main table: Table with the essential data.
        - Notes table: Table linking entries in the main table with specific notes.
        - Bulk ID table: Mapping between IDs in the main table and bulk IDs. Bulk IDs are legacy IDs. This table might be removed in the future.
    """
    df = _add_range_index(df)
    # Main table
    tb_main = make_table_main(df, short_name)
    # Notes table
    tb_notes = make_table_notes(df)
    # Bulk ID table
    tb_bulk_id = make_table_bulk_id(df)
    # Return
    tables = [
        tb_main,
        tb_bulk_id,
    ]
    if tb_notes is not None:
        tables.append(tb_notes)
    else:
        log.warning("Notes table is empty!")
    return tables


def table_to_clean_df(tb: Table, entities_with_comma: Optional[List[str]] = None) -> pd.DataFrame:
    """Clean data"""
    # Table to DataFrame
    df = pd.DataFrame(tb)
    # Check for duplicate conflicts (currently in debug mode)
    check_duplicates(df, logging=True, filename=tb.metadata.dataset.short_name)
    # Clean participants
    df = clean_participants(df, entities_with_comma=entities_with_comma)
    # Remove expected duplicates and check that there are no more duplicates
    # check_duplicates(df, force_error=True)
    if df.duplicated().any():
        raise ValueError("Why was this not cought?!")
    return df


def make_table_main(df: pd.DataFrame, short_name: str) -> Table:
    """Create main table.

    The resulting table contains all the columns from `COLUMNS_COMMON`, and those available from `COLUMNS_EXTRA`."""
    # Get available extra columns
    df = df[COLUMNS_COMMON + _extra_columns(df) + [_INDEX_COLNAME_AUX]].set_index(_INDEX_COLNAME_AUX)
    # Check for duplicates
    msk = df.duplicated()
    if msk.any():
        raise ValueError(f"{msk.sum()} rows in table are duplicated! {df[msk]}")
    tb = Table(df, short_name=short_name)
    return tb


def make_table_notes(df: pd.DataFrame) -> Table:
    """Create notes table.

    For each row in the main table, there might be a note. This table links each row in the main table with a note. If there
    are no notes, the table will be empty (which should be hadnled later)
    """
    df = df[COLUMNS_COMMON_NOTES + [_INDEX_COLNAME_AUX]].dropna().set_index(_INDEX_COLNAME_AUX)
    tb = Table(df, short_name="notes")
    return tb


def make_table_bulk_id(df: pd.DataFrame) -> Table:
    df = df[["bulk_id", _INDEX_COLNAME_AUX]].set_index(_INDEX_COLNAME_AUX)
    tb = Table(df, short_name="bulk_id")
    return tb


def clean_participants(
    df: pd.DataFrame,
    mapping: Optional[Dict[str, str]] = None,
    entities_with_comma: Optional[List[str]] = None,
    default_separator: str = ", ",
    new_separator: str = "; ",
) -> pd.DataFrame:
    """Clean the names of the participants in each conflicts.

    Typical cleaning includes:
        - Remove whitespace (start or end).
        - Map specific country names to new ones.
        - Change the separator from `default_separator` to `new_separator`. By default, ', ' -> '; '.
    """
    _REPLACE_CHAR = "-/*/-"

    def _clean_participant_name(name: str) -> str:
        """Clean participant name"""
        name = name.strip()
        if entities_with_comma:
            for entity in entities_with_comma:
                name = name.replace(entity.replace(",", _REPLACE_CHAR), entity)
        if mapping:
            name = mapping.get(name, name)
        return name

    ds = df["conflict_participants"]
    # Replace raw values
    if entities_with_comma:
        for entity in entities_with_comma:
            ds = ds.str.replace(entity, entity.replace(",", _REPLACE_CHAR), regex=False)
    # Remove whitespace (start or end)
    ds = ds.str.strip()
    # String to list of elements
    ds = ds.str.split(default_separator)
    # List back to string, but with fixed names (remove spaces)
    ds = ds.apply(lambda x: new_separator.join([_clean_participant_name(xx) for xx in set(x)]))

    return df.assign(conflict_participants=ds)


def remove_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows from the dataframe."""
    return df.drop_duplicates()


def get_set_participants(df: pd.DataFrame):
    """Get list of all participant countries.

    Given a table, obtain the set of countries that participated in any of the included conflicts.
    The table should have a column named `conflict_participants` of type string, with the list of countries
    that participated in the conflict, separated by semi-colons and a whitespace ('; ')."""
    participants = df["conflict_participants"].tolist()
    participants = [p.split("; ") for p in participants]
    participants = set(pp for p in participants for pp in p)
    return participants


def check_duplicates(df: pd.DataFrame, filename: str, logging: bool = False, force_error: bool = False):
    def _check_duplicates(
        df, columns_subset: List[str], logging: bool = True, force_error: bool = False, to_csv: Optional[str] = None
    ):
        msk = df.duplicated(subset=columns_subset, keep=False)
        if (n_duplicates := msk.sum()) > 0:
            df_ = df.loc[
                msk, ["bulk_id", "conflict_name", "conflict_participants", "start_year", "end_year", "total_deaths"]
            ].sort_values(["conflict_name", "conflict_participants", "start_year", "end_year"])
            error_msg = (
                "There are some entries with the same"
                f" {', '.join([f'`{col}`' for col in columns_subset])} ({n_duplicates}). Please review!\n{df_}"
            )
            if force_error:
                raise ValueError(error_msg)
            elif logging:
                log.warning(error_msg)
            if to_csv:
                df_.to_csv(to_csv, index=False)
            return True
        return False

    # check duplicates with same conflict_name and conflict_participants
    _check_duplicates(
        df,
        ["conflict_name", "conflict_participants"],
        logging=logging,
        force_error=False,
        # to_csv=f"logging/{filename}-conflict-countries.csv",
    )
    # check duplicates with same conflict_name and years
    _check_duplicates(
        df,
        ["conflict_name", "start_year", "end_year"],
        logging=logging,
        force_error=False,
        # to_csv=f"logging/{filename}-conflict-years.csv",
    )
    # check duplicates with same conflict_name and fatalities
    _check_duplicates(
        df,
        ["conflict_name", "total_deaths"],
        logging=logging,
        force_error=False,
        # to_csv=f"logging/{filename}-numdeaths.csv",
    )
    # check duplicates with same conflict_name
    _check_duplicates(
        df,
        ["conflict_name"],
        logging=logging,
        force_error=False,
        # to_csv=f"logging/{filename}-conflict.csv",
    )


def _add_range_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(**{_INDEX_COLNAME_AUX: pd.RangeIndex(df.shape[0])})
    return df


def _extra_columns(df: pd.DataFrame):
    return list(set(df.columns).intersection(COLUMNS_EXTRA))
