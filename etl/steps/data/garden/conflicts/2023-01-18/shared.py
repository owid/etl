from typing import Dict, List, Optional

import pandas as pd
from owid.catalog import Table

COLUMNS_COMMON = [
    "conflict_name",
    "conflict_participants",
    "type_of_conflict",
    "start_year",
    "end_year",
    "continent",
    "total_deaths",
]

COLUMNS_COMMON_NOTES = [
    "notes_inc_key_quote",
]
_INDEX_COLNAME_AUX = "index"


def get_set_participants(df: pd.DataFrame):
    # Get set of participants
    participants = df["conflict_participants"].tolist()
    participants = [p.split(", ") for p in participants]
    participants = set(pp for p in participants for pp in p)
    return participants


def clean_participants(
    df: pd.DataFrame, mapping: Optional[Dict[str, str]] = None, entities_with_comma: Optional[List[str]] = None
) -> pd.DataFrame:
    REPLACE_CHAR = "-/*/-"

    def _clean_participant_name(name: str) -> str:
        """Clean participant name"""
        name = name.strip()
        if entities_with_comma:
            for entity in entities_with_comma:
                name = name.replace(entity.replace(",", REPLACE_CHAR), entity)
        if mapping:
            name = mapping.get(name, name)
        return name

    ds = df["conflict_participants"]
    # Replace raw values
    if entities_with_comma:
        for entity in entities_with_comma:
            ds = ds.str.replace(entity, entity.replace(",", REPLACE_CHAR), regex=False)
    # Remove whitespace (start or end)
    ds = ds.str.strip()
    # String to list of elements
    ds = ds.str.split(", ")
    # List back to string, but with fixed names (remove spaces)
    ds = ds.apply(lambda x: "; ".join([_clean_participant_name(xx) for xx in set(x)]))

    return df.assign(conflict_participants=ds)


def _add_range_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(**{_INDEX_COLNAME_AUX: pd.RangeIndex(df.shape[0])})
    return df


def clean_data(df: pd.DataFrame, entities_with_comma: Optional[List[str]] = None) -> pd.DataFrame:
    """Clean data"""
    df = clean_participants(df, entities_with_comma=entities_with_comma)
    # Remove expected duplicates and check that there are no more duplicates
    df = remove_duplicate_rows(df)
    return df


def remove_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows from the dataframe."""
    return df.drop_duplicates()


def make_tables(df: pd.DataFrame, short_name: str) -> Dict[str, Table]:
    """Make tables"""
    df = _add_range_index(df)
    # Main table
    tb_main = make_table_main(df, short_name)
    # Notes table
    tb_notes = make_table_notes(df)
    # Bulk ID table
    tb_bulk_id = make_table_bulk_id(df)
    # Return
    return {
        "main": tb_main,
        "notes": tb_notes,
        "bulk_id": tb_bulk_id,
    }


def make_table_main(df: pd.DataFrame, short_name: str) -> Table:
    df = df[COLUMNS_COMMON + [_INDEX_COLNAME_AUX]].set_index(_INDEX_COLNAME_AUX)
    # Check for duplicates
    msk = df.duplicated()
    if msk.any():
        raise ValueError(f"{msk.sum()} rows in table are duplicated! {df[msk]}")
    tb = Table(df, short_name=short_name)
    return tb


def make_table_notes(df: pd.DataFrame) -> Table:
    df = df[COLUMNS_COMMON_NOTES + [_INDEX_COLNAME_AUX]].dropna().set_index(_INDEX_COLNAME_AUX)
    tb = Table(df, short_name="notes")
    return tb


def make_table_bulk_id(df: pd.DataFrame) -> Table:
    df = df[["bulk_id", _INDEX_COLNAME_AUX]].set_index(_INDEX_COLNAME_AUX)
    tb = Table(df, short_name="bulk_id")
    return tb
