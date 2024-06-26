import os
import tempfile
import zipfile
from typing import List, Optional, Tuple

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Origin, Table, TableMeta
from owid.catalog.tables import concat
from pandas.api.types import CategoricalDtype

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__=__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    paths.log.info("reading snapshots...")

    # Population
    paths.log.info("reading population...")
    tb_population = paths.read_snap_table("un_wpp_population.csv")
    # Growth rate
    tb_growth_rate = read_estimates_and_projections_from_snap("un_wpp_growth_rate.xlsx")
    # Natural change rate
    tb_nat_change = read_estimates_and_projections_from_snap("un_wpp_nat_change_rate.xlsx")
    # Fertility rate
    tb_tot = read_estimates_and_projections_from_snap("un_wpp_fert_rate_tot.xlsx")
    tb_age = read_estimates_and_projections_from_snap("un_wpp_fert_rate_age.xlsx")
    tb_fertility = combine_fertility_tables(tb_tot, tb_age)
    # Migration
    tb_migration = read_estimates_and_projections_from_snap("un_wpp_migration.xlsx")
    tb_migration = to_long_format_migration(tb_migration)
    tb_migration_rate = read_estimates_and_projections_from_snap("un_wpp_migration_rate.xlsx")
    # Deaths
    tb_deaths_tot = read_estimates_and_projections_from_snap("un_wpp_deaths.xlsx")
    tb_deaths_age = read_estimates_and_projections_from_snap("un_wpp_deaths_age.xlsx")
    tb_deaths_age_fem = read_estimates_and_projections_from_snap("un_wpp_deaths_age_fem.xlsx")
    tb_deaths_age_male = read_estimates_and_projections_from_snap("un_wpp_deaths_age_male.xlsx")
    tb_deaths = combine_death(tb_deaths_tot, tb_deaths_age, tb_deaths_age_fem, tb_deaths_age_male)
    # Death rate
    tb_death_rate = read_estimates_and_projections_from_snap("un_wpp_death_rate.xlsx")

    #
    # Process data.
    #
    # Process tables
    tb_population = clean_table(tb_population, "population")
    tb_growth_rate = clean_table(tb_growth_rate, "growth_rate")
    tb_nat_change = clean_table(tb_nat_change, "natural_change_rate")
    tb_fertility = clean_table(tb_fertility, "fertility_rate")
    tb_migration = clean_table(tb_migration, "net_migration")
    tb_migration_rate = clean_table(tb_migration_rate, "net_migration_rate")
    tb_deaths = clean_table(tb_deaths, "deaths")
    tb_death_rate = clean_table(tb_death_rate, "death_rate")

    #
    # Save outputs.
    #
    tables = [
        tb_population,
        tb_growth_rate,
        tb_nat_change,
        tb_fertility,
        tb_migration,
        tb_migration_rate,
        tb_deaths,
        tb_death_rate,
    ]
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_estimates_and_projections_from_snap(short_name: str) -> Table:
    paths.log.info(f"reading {short_name}...")
    # Read snap
    snap = paths.load_snapshot(short_name)
    # Read tables
    tb_estimates = snap.read(sheet_name="Estimates")
    tb_projections = snap.read(sheet_name="Medium")
    # Merge tables
    tb = concat([tb_estimates, tb_projections], ignore_index=True)
    return tb


def combine_fertility_tables(tb_tot: Table, tb_age: Table) -> Table:
    columns = set(tb_tot.columns).intersection(set(tb_age.columns))
    tb_age = tb_age.melt(list(columns), var_name="Age", value_name="Value")
    tb_tot["Age"] = "all"
    tb_fertility = concat([tb_age, tb_tot], ignore_index=True)

    return tb_fertility


def combine_death(tb_tot: Table, tb_age: Table, tb_age_fem: Table, tb_age_male: Table) -> Table:
    # Drop column 'Sex'
    tb_age = tb_age.drop(columns=["Sex"])
    tb_age_fem = tb_age_fem.drop(columns=["Sex"])
    tb_age_male = tb_age_male.drop(columns=["Sex"])

    # Get common columns
    columns = set(tb_tot.columns).intersection(set(tb_age.columns))

    # Add missing dimension to general population
    tb_tot = tb_tot.melt(list(columns), var_name="Sex", value_name="Value")
    tb_tot["Age"] = "all"

    # Unpivot age table (all sex)
    tb_age = tb_age.melt(list(columns), var_name="Age", value_name="Value")
    tb_age["Sex"] = "Total"

    # Unpivot age table (all sex)
    tb_age_fem = tb_age_fem.melt(list(columns), var_name="Age", value_name="Value")
    tb_age_fem["Sex"] = "Female"

    # Unpivot age table (all sex)
    tb_age_male = tb_age_male.melt(list(columns), var_name="Age", value_name="Value")
    tb_age_male["Sex"] = "Male"

    # Combine
    tb_deaths = concat([tb_tot, tb_age, tb_age_fem, tb_age_male], ignore_index=True)

    return tb_deaths


def to_long_format_migration(tb: Table) -> Table:
    """Convert migration table to long format."""
    # Melt
    tb = tb.melt(
        id_vars=[col for col in tb.columns if col not in {"Male", "Female", "Total"}],
        var_name="Sex",
        value_name="Value",
    )
    return tb


def clean_table(tb: Table, indicator_name: str) -> Table:
    """Process growth rate data.

    From snapshot table to ETL-ready-cleaned table.
    """
    paths.log.info(f"processing {indicator_name} data...")

    COLUMNS = {
        "LocationName": "country",
        "Year": "year",
        "LocTypeName": "location_type",
        "VariantName": "variant",
        "Sex": "sex",
        "Age": "age",
        "Value": indicator_name,
    }
    COLUMNS = {k: v for k, v in COLUMNS.items() if k in tb.columns}
    COLUMNS_INDEX = [v for k, v in COLUMNS.items() if v not in {indicator_name}]

    # Column rename
    tb = tb.rename(columns=COLUMNS)
    # Keep relevant columns
    tb = tb.loc[:, COLUMNS.values()]

    # Keep relevant location types
    location_types = ["Country/Area", "Region", "Income Group", "Development Group", "World"]
    tb = tb.loc[tb["location_type"].isin(location_types)]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(COLUMNS_INDEX, short_name=indicator_name)

    return tb


#################################################################################
#################################################################################
# Old code below. Left in case it's needed for reference. Might delete later.
#################################################################################
#################################################################################
def extract_data(snap: Snapshot, output_dir: str) -> None:
    z = zipfile.ZipFile(snap.path)
    z.extractall(output_dir)


def load_data(tmp_dir: str, metadata: TableMeta, origin: Origin) -> Tuple[Table, ...]:
    return (
        _load_population(tmp_dir, metadata, origin),
        _load_fertility(tmp_dir, metadata, origin),
        _load_demographics(tmp_dir, metadata, origin),
        _load_dependency_ratio(tmp_dir, metadata, origin),
        _load_deaths(tmp_dir, metadata, origin),
    )


def _load_population(tmp_dir: str, metadata: TableMeta, origin: Origin) -> Table:
    """Load population dataset (CSV)"""
    paths.log.info("un_wpp._load_population")
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
    return pr.concat(
        [
            pr.read_csv(os.path.join(tmp_dir, filename), dtype=dtype, metadata=metadata, origin=origin)
            for filename in filenames
        ],
        ignore_index=True,
    )


def _load_fertility(tmp_dir: str, metadata: TableMeta, origin: Origin) -> Table:
    paths.log.info("un_wpp._load_fertility")
    """Load fertility dataset (CSV)"""
    (filename,) = [f for f in filter(lambda x: "Fertility" in x, sorted(os.listdir(tmp_dir))) if "notes" not in f]
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
    return pr.read_csv(os.path.join(tmp_dir, filename), dtype=dtype, metadata=metadata, origin=origin)


def _load_demographics(tmp_dir: str, metadata: TableMeta, origin: Origin) -> Table:
    """Load demographics dataset (CSV)"""
    paths.log.info("un_wpp._load_demographics")
    filenames = [f for f in filter(lambda x: "Demographic" in x, sorted(os.listdir(tmp_dir))) if "notes" not in f]
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
    return pr.concat(
        [
            pr.read_csv(os.path.join(tmp_dir, filename), dtype=dtype, metadata=metadata, origin=origin)
            for filename in filenames
        ],
        ignore_index=True,
    )


def _load_deaths(tmp_dir: str, metadata: TableMeta, origin: Origin) -> Table:
    """Load deaths dataset (XLSX)"""
    paths.log.info("un_wpp._load_deaths")
    filenames = list(filter(lambda x: "DEATHS" in x, sorted(os.listdir(tmp_dir))))
    # Load
    dfs = [_read_xlsx_file(tmp_dir, filename, metadata=metadata, origin=origin) for filename in filenames]
    return pr.concat(dfs, ignore_index=True)


def _load_dependency_ratio(tmp_dir: str, metadata: TableMeta, origin: Origin) -> Table:
    """Load dependency ratio dataset (XLSX)"""
    paths.log.info("un_wpp._load_dependency_ratio")
    filenames = list(filter(lambda x: "DEPENDENCY_RATIOS" in x, sorted(os.listdir(tmp_dir))))
    # Load
    dfs = [_read_xlsx_file(tmp_dir, filename, metadata=metadata, origin=origin) for filename in filenames]
    return pr.concat(dfs, ignore_index=True)


def _read_xlsx_file(tmp_dir: str, filename: str, metadata: TableMeta, origin: Origin) -> Table:
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
    dfs = []
    for sheet_name in ["Estimates", "Medium variant", "High variant", "Low variant"]:
        dfs.append(
            pr.read_excel(
                os.path.join(tmp_dir, filename),
                skiprows=16,
                sheet_name=sheet_name,
                metadata=metadata,
                origin=origin,
            )
        )

    # Check that all dataframes have same columns
    assert len(set(str(set(df.columns)) for df in dfs)) == 1
    # Concatenate
    df = pr.concat(dfs, ignore_index=True)
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
    df_population: Table,
    df_fertility: Table,
    df_demographics: Table,
    df_depratio: Table,
    df_deaths: Table,
) -> Tuple[Table, ...]:
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
    df_population: Table,
    df_fertility: Table,
    df_demographics: Table,
    df_depratio: Table,
    df_deaths: Table,
) -> Tuple[Table, ...]:
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


def std_columns(df_depratio: Table, df_deaths: Table) -> Tuple[Table, ...]:
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
    df_population: Table,
    df_fertility: Table,
    df_demographics: Table,
    df_depratio: Table,
    df_deaths: Table,
) -> Tuple[Table, ...]:
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
    df_population: Table,
    df_fertility: Table,
    df_demographics: Table,
    df_depratio: Table,
    df_deaths: Table,
) -> Tuple[Table, ...]:
    df_population = _df_cols_as_str(df_population)
    df_fertility = _df_cols_as_str(df_fertility)
    df_demographics = _df_cols_as_str(df_demographics)
    df_depratio = _df_cols_as_str(df_depratio)
    df_deaths = _df_cols_as_str(df_deaths)
    return df_population, df_fertility, df_demographics, df_depratio, df_deaths


def fix_types(df_depratio: Table, df_deaths: Table) -> Tuple[Table, ...]:
    _type = pd.StringDtype()
    df_depratio = df_depratio.assign(Notes=df_depratio.Notes.astype(_type))
    df_deaths = df_deaths.assign(Notes=df_deaths.Notes.astype(_type))
    return df_depratio, df_deaths


def _df_cols_as_str(df: Table) -> Table:
    df.columns = df.columns.astype(str)
    return df


def _sanity_checks(
    df: Table,
    column_location: str,
    column_location_id: str,
    column_location_type: Optional[str] = None,
    location_type_values: List[str] = [],
) -> Table:
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


def run_old(dest_dir: str) -> None:
    # Load
    snap = paths.load_snapshot("un_wpp.zip")
    with tempfile.TemporaryDirectory() as tmp_dir:
        paths.log.info("un_wpp.extract_data")
        extract_data(snap, tmp_dir)
        assert snap.metadata.origin
        paths.log.info("un_wpp.load_data")
        (
            df_population,
            df_fertility,
            df_demographics,
            df_depratio,
            df_deaths,
        ) = load_data(tmp_dir, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)
    # Process
    paths.log.info("un_wpp.process")
    df_population, df_fertility, df_demographics, df_depratio, df_deaths = process(
        df_population, df_fertility, df_demographics, df_depratio, df_deaths
    )

    # Create dataset
    ds = create_dataset(
        dest_dir,
        [
            df_population.underscore().update_metadata(short_name="population"),
            df_fertility.underscore().update_metadata(short_name="fertility"),
            df_demographics.underscore().update_metadata(short_name="demographics"),
            df_depratio.underscore().update_metadata(short_name="dependency_ratio"),
            df_deaths.underscore().update_metadata(short_name="deaths"),
        ],
        default_metadata=snap.metadata,
    )
    ds.save()
