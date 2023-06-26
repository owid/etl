import os
import tempfile
import zipfile
from typing import Any, List, Optional, Tuple

import pandas as pd
from owid.catalog import Dataset, Table, utils
from pandas.api.types import CategoricalDtype

from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata


def extract_data(snap: Snapshot, output_dir: str) -> None:
    z = zipfile.ZipFile(snap.path)
    z.extractall(output_dir)


def load_data(tmp_dir: str) -> Tuple[pd.DataFrame, ...]:
    return (
        _load_population(tmp_dir),
        _load_fertility(tmp_dir),
        _load_demographics(tmp_dir),
        _load_dependency_ratio(tmp_dir),
        _load_deaths(tmp_dir),
    )


def _load_population(tmp_dir: str) -> pd.DataFrame:
    """Load population dataset (CSV)"""
    filenames = list(filter(lambda x: "PopulationBySingleAgeSex" in x, sorted(os.listdir(tmp_dir))))
    dtype = {
        "SortOrder": "category",
        "LocID": "category",
        "Notes": "category",
        "ISO3_code": "category",
        "ISO2_code": "category",
        "SDMX_code": "category",
        "LocTypeID": "category",
        "LocTypeName": "category",
        "ParentID": "category",
        "Location": "category",
        "VarID": CategoricalDtype(categories=["2", "3", "4"]),
        "Variant": CategoricalDtype(categories=["Medium", "High", "Low", "Constant fertility"]),
        "Time": "uint16",
        "MidPeriod": "uint16",
        "AgeGrp": "category",
        "AgeGrpStart": "uint8",
        "AgeGrpSpan": "int8",
        "PopMale": "float",
        "PopFemale": "float",
        "PopTotal": "float",
    }
    return pd.concat(
        [pd.read_csv(os.path.join(tmp_dir, filename), dtype=dtype) for filename in filenames],
        ignore_index=True,
    )


def _load_fertility(tmp_dir: str) -> pd.DataFrame:
    """Load fertility dataset (CSV)"""
    (filename,) = [f for f in filter(lambda x: "Fertility" in x, sorted(os.listdir(tmp_dir)) if "notes" not in f])
    dtype = {
        "SortOrder": "category",
        "LocID": "category",
        "Notes": "category",
        "ISO3_code": "category",
        "ISO2_code": "category",
        "SDMX_code": "category",
        "LocTypeID": "category",
        "LocTypeName": "category",
        "ParentID": "category",
        "Location": "category",
        "VarID": "category",
        "Variant": "category",
        "Time": "uint16",
        "MidPeriod": "float32",
        "AgeGrp": "category",
        "AgeGrpStart": "uint8",
        "AgeGrpSpan": "uint8",
        "ASFR": "float",
        "PASFR": "float",
        "Births": "float",
    }
    return pd.read_csv(os.path.join(tmp_dir, filename), dtype=dtype)


def _load_demographics(tmp_dir: str) -> pd.DataFrame:
    """Load demographics dataset (CSV)"""
    filenames = [f for f in filter(lambda x: "Demographic" in x, sorted(os.listdir(tmp_dir)) if "notes" not in f])
    dtype = {
        "SortOrder": "category",
        "LocID": "category",
        "Notes": "category",
        "ISO3_code": "category",
        "ISO2_code": "category",
        "SDMX_code": "category",
        "LocTypeID": "category",
        "LocTypeName": "category",
        "ParentID": "category",
        "Location": "category",
        "VarID": CategoricalDtype(categories=["2", "3", "4", "5", "6", "7", "8", "9", "10", "16"]),
        "Variant": CategoricalDtype(
            categories=[
                "Medium",
                "High",
                "Low",
                "Constant fertility",
                "Instant replacement",
                "Zero migration",
                "Constant mortality",
                "No change",
                "Momentum",
                "Instant replacement zero migration",
                "Median PI",
                "Upper 80 PI",
                "Lower 80 PI",
                "Upper 95 PI",
                "Lower 95 PI",
            ]
        ),
        "Time": "uint16",
    }
    return pd.concat(
        [pd.read_csv(os.path.join(tmp_dir, filename), dtype=dtype) for filename in filenames],
        ignore_index=True,
    )


def _load_deaths(tmp_dir: str) -> Any:
    """Load deaths dataset (XLSX)"""
    filenames = list(filter(lambda x: "DEATHS" in x, sorted(os.listdir(tmp_dir))))
    # Load
    dfs = [_read_xlsx_file(tmp_dir, filename) for filename in filenames]
    return pd.concat(dfs, ignore_index=True)


def _load_dependency_ratio(tmp_dir: str) -> Any:
    """Load dependency ratio dataset (XLSX)"""
    filenames = list(filter(lambda x: "DEPENDENCY_RATIOS" in x, sorted(os.listdir(tmp_dir))))
    # Load
    dfs = [_read_xlsx_file(tmp_dir, filename) for filename in filenames]
    return pd.concat(dfs, ignore_index=True)


def _read_xlsx_file(tmp_dir: str, filename: str) -> Any:
    dtype_base = {
        "Index": "category",
        "Variant": "category",
        "Region, subregion, country or area *": "category",
        "Notes": "category",
        "Location code": "category",
        "ISO3 Alpha-code": "category",
        "ISO2 Alpha-code": "category",
        "SDMX code**": "category",
        "Type": "category",
        "Parent code": "category",
        "Year": "uint16",
    }
    # Load excel
    df = pd.read_excel(
        os.path.join(tmp_dir, filename),
        skiprows=16,
        sheet_name=["Estimates", "Medium variant", "High variant", "Low variant"],
    )
    # Check
    assert len(set(str(set(d.columns)) for _, d in df.items())) == 1
    # Concatenate
    df = pd.concat(list(df.values()), ignore_index=True)
    # Filter
    df = df[df["Type"] != "Label/Separator"]
    # Dtypes
    dtype = {**dtype_base, **{m: "float" for m in df.columns if m not in dtype_base}}
    df = df.astype(dtype)
    if "BOTH_SEXES" in filename:
        df = df.assign(Sex="Both")
    elif "FEMALE" in filename:
        df = df.assign(Sex="Female")
    else:
        df = df.assign(Sex="Male")
    return df


def process(
    df_population: pd.DataFrame,
    df_fertility: pd.DataFrame,
    df_demographics: pd.DataFrame,
    df_depratio: pd.DataFrame,
    df_deaths: pd.DataFrame,
) -> Tuple[pd.DataFrame, ...]:
    # Sanity checks
    (
        df_population,
        df_fertility,
        df_demographics,
        df_depratio,
        df_deaths,
    ) = sanity_checks(df_population, df_fertility, df_demographics, df_depratio, df_deaths)
    # Harmonize column names across datasets (CSV, XLSX)
    df_depratio, df_deaths = std_columns(df_depratio, df_deaths)
    # Set index
    df_population, df_fertility, df_demographics, df_depratio, df_deaths = set_index(
        df_population, df_fertility, df_demographics, df_depratio, df_deaths
    )
    # Dataframe columns as strings
    (
        df_population,
        df_fertility,
        df_demographics,
        df_depratio,
        df_deaths,
    ) = df_cols_as_str(df_population, df_fertility, df_demographics, df_depratio, df_deaths)
    # Fix column types
    df_depratio, df_deaths = fix_types(df_depratio, df_deaths)
    return df_population, df_fertility, df_demographics, df_depratio, df_deaths


def sanity_checks(
    df_population: pd.DataFrame,
    df_fertility: pd.DataFrame,
    df_demographics: pd.DataFrame,
    df_depratio: pd.DataFrame,
    df_deaths: pd.DataFrame,
) -> Tuple[pd.DataFrame, ...]:
    df_population = _sanity_checks(
        df_population,
        "Location",
        "LocID",
        "LocTypeName",
        ["Geographic region", "Income group", "Country/Area", "World", "Development group"],  # , "SDG region"],
    )
    df_fertility = _sanity_checks(
        df_fertility,
        "Location",
        "LocID",
        "LocTypeName",
        ["Geographic region", "Income group", "Country/Area", "World", "Development group"],  # , "SDG region"],
    )
    df_demographics = _sanity_checks(
        df_demographics,
        "Location",
        "LocID",
        "LocTypeName",
        ["Geographic region", "Income group", "Country/Area", "World", "Development group"],  # , "SDG region"],
    )
    df_depratio = _sanity_checks(
        df_depratio,
        "Region, subregion, country or area *",
        "Location code",
        "Type",
        ["Region", "Income Group", "Country/Area", "World", "Development Group"],  # , "SDG region"],
    )
    df_deaths = _sanity_checks(
        df_deaths,
        "Region, subregion, country or area *",
        "Location code",
        "Type",
        ["Region", "Income Group", "Country/Area", "World", "Development Group"],  # , "SDG region"],
    )
    return df_population, df_fertility, df_demographics, df_depratio, df_deaths


def std_columns(df_depratio: pd.DataFrame, df_deaths: pd.DataFrame) -> Tuple[pd.DataFrame, ...]:
    columns_rename = {
        "Variant": "Variant",
        "Region, subregion, country or area *": "Location",
        "Notes": "Notes",
        "Location code": "LocID",
        "ISO3 Alpha-code": "ISO3_code",
        "ISO2 Alpha-code": "ISO2_code",
        "SDMX code**": "SDMX_code",
        "Type": "LocTypeName",
        "Parent code": "ParentID",
        "Year": "Time",
    }
    columns_drop = ["Index"]
    df_depratio = df_depratio.rename(columns=columns_rename).drop(columns=columns_drop)
    df_deaths = df_deaths.rename(columns=columns_rename).drop(columns=columns_drop)
    return df_depratio, df_deaths


def set_index(
    df_population: pd.DataFrame,
    df_fertility: pd.DataFrame,
    df_demographics: pd.DataFrame,
    df_depratio: pd.DataFrame,
    df_deaths: pd.DataFrame,
) -> Tuple[pd.DataFrame, ...]:
    df_population = df_population.set_index(
        ["Location", "Time", "Variant", "AgeGrp"],
        verify_integrity=True,
    )
    df_fertility = df_fertility.set_index(
        ["Location", "Time", "Variant", "AgeGrp"],
        verify_integrity=True,
    )
    df_demographics = df_demographics.set_index(
        ["Location", "Time", "Variant"],
        verify_integrity=True,
    )
    df_depratio = df_depratio.set_index(
        ["Location", "Time", "Variant", "Sex"],
        verify_integrity=True,
    )
    df_deaths = df_deaths.set_index(
        ["Location", "Time", "Variant", "Sex"],
        verify_integrity=True,
    )
    return df_population, df_fertility, df_demographics, df_depratio, df_deaths


def df_cols_as_str(
    df_population: pd.DataFrame,
    df_fertility: pd.DataFrame,
    df_demographics: pd.DataFrame,
    df_depratio: pd.DataFrame,
    df_deaths: pd.DataFrame,
) -> Tuple[pd.DataFrame, ...]:
    df_population = _df_cols_as_str(df_population)
    df_fertility = _df_cols_as_str(df_fertility)
    df_demographics = _df_cols_as_str(df_demographics)
    df_depratio = _df_cols_as_str(df_depratio)
    df_deaths = _df_cols_as_str(df_deaths)
    return df_population, df_fertility, df_demographics, df_depratio, df_deaths


def fix_types(df_depratio: pd.DataFrame, df_deaths: pd.DataFrame) -> Tuple[pd.DataFrame, ...]:
    _type = pd.StringDtype()
    df_depratio = df_depratio.assign(Notes=df_depratio.Notes.astype(_type))
    df_deaths = df_deaths.assign(Notes=df_deaths.Notes.astype(_type))
    return df_depratio, df_deaths


def _df_cols_as_str(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str)
    return df


def _sanity_checks(
    df: pd.DataFrame,
    column_location: str,
    column_location_id: str,
    column_location_type: Optional[str] = None,
    location_type_values: List[str] = [],
) -> pd.DataFrame:
    # Quick filter
    # df = df[-((df[column_location] == "Latin America and Caribbean") & (df[column_location_type] == "SDG region"))]
    # There are some duplicates. Some locations appear with two different location IDs, but same data.
    if column_location_type:
        df = df[df[column_location_type].isin(location_type_values)]
    cols = [col for col in df.columns if col != column_location_id]
    df = df.drop_duplicates(subset=cols)
    assert df.groupby(column_location_id)[column_location].nunique().max() == 1
    assert df.groupby(column_location)[column_location_id].nunique().max() == 1
    return df


def init_dataset(dest_dir: str, snap: Snapshot) -> Dataset:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.short_name = "un_wpp"
    ds.save()
    return ds


def add_tables_to_ds(
    ds: Dataset,
    df_population: pd.DataFrame,
    df_fertility: pd.DataFrame,
    df_demographics: pd.DataFrame,
    df_depratio: pd.DataFrame,
    df_deaths: pd.DataFrame,
) -> Dataset:
    data = [
        (df_population, "population"),
        (df_fertility, "fertility"),
        (df_demographics, "demographics"),
        (df_depratio, "dependency_ratio"),
        (df_deaths, "deaths"),
    ]
    for elem in data:
        print(elem[1])
        t = Table(elem[0])
        t.metadata.short_name = elem[1]
        ds.add(utils.underscore_table(t))
    return ds


def run(dest_dir: str) -> None:
    # Load
    snap = Snapshot("un/2022-07-11/un_wpp.zip")
    with tempfile.TemporaryDirectory() as tmp_dir:
        extract_data(snap, tmp_dir)
        (
            df_population,
            df_fertility,
            df_demographics,
            df_depratio,
            df_deaths,
        ) = load_data(tmp_dir)
    # Process
    df_population, df_fertility, df_demographics, df_depratio, df_deaths = process(
        df_population, df_fertility, df_demographics, df_depratio, df_deaths
    )
    # Initiate dataset
    ds = init_dataset(dest_dir, snap)
    # Add tables to dataset
    ds = add_tables_to_ds(ds, df_population, df_fertility, df_demographics, df_depratio, df_deaths)
