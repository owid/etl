"""WB Gender Garden step."""

from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
from owid import catalog
from structlog import get_logger

from etl.data_helpers import geo
from etl.paths import BASE_DIR as base_path

log = get_logger()


DATASET_MEADOW = base_path / "data/meadow/wb/2022-10-29/wb_gender"
THIS_DIRECTORY = Path(__file__).parent


def init_garden_ds(dest_dir: str, ds_meadow: catalog.Dataset) -> catalog.Dataset:
    """Initiate garden dataset.

    Returns
    -------
    catalog.Dataset
        Garden dataset.
    """
    ds = catalog.Dataset.create_empty(dest_dir, ds_meadow.metadata)
    # ds.metadata = catalog.DatasetMeta(
    #     namespace="wb",
    #     short_name="wb_gender",
    #     title="Gender Statistics - World Bank (2022)",
    #     description="Gender statistics by the World Bank. More details at https://genderdata.worldbank.org/.",
    #     version="2022",
    # )
    return ds


def load_meadow_ds() -> catalog.Dataset:
    """Load dataset from Meadow.

    Returns
    -------
    catalog.Dataset
        Meadow dataset.
    """
    ds = catalog.Dataset(DATASET_MEADOW)
    return ds


def make_table(table: catalog.Table) -> catalog.Table:
    """Generate dataset table.

    Parameters
    ----------
    ds : catalog.Table
        Data table.

    Returns
    -------
    catalog.Table
        Table.
    """
    log.info("Loading meadow dataset...")
    table = table.reset_index()
    # Harmonize country names
    log.info("Harmonize country names...")
    table = geo.harmonize_countries(
        df=table,
        countries_file=Path(__file__).parent / "wb_gender.countries.json",
        country_col="country",
        make_missing_countries_nan=True,
        show_full_warning=False,
    )
    # Drop countries without hamonized name
    table = table.dropna(subset=["country"])
    # Set index
    log.info("Set index...")
    column_idx = ["country", "variable", "year"]
    table = table.set_index(column_idx).sort_index()
    return table


def make_table_metadata(table: catalog.Table, table_metadata: catalog.Table) -> catalog.Table:
    """Create metadata table.

    Parameters
    ----------
    table : catalog.Table
        Raw metadata table.

    Returns
    -------
    catalog.Table
        Metadata table.
    """
    columns_relevant = [
        "indicator_name",
        "topic",
        "series_code",
        "long_definition",
        "unit_of_measure",
        "source",
        "related_source_links",
        "license_type",
    ]
    # Get dataset generic license
    license_ = table.metadata.dataset.licenses[0]
    # Keep relevant columns
    table_metadata = table_metadata[columns_relevant]
    # Check that all variables have either no license or generic dataset license
    licenses = table_metadata.license_type.dropna().unique()
    assert len(licenses) == 1
    assert licenses[0] == "CC BY-4.0", f"License in dataset should be 'CC BY-4.0', but is {licenses[0].name} instead."
    assert license_.name == "CC BY 4.0", f"License in dataset should be 'CC BY 4.0', but is {license_.name} instead."
    # Assign generic dataset license to all variables
    # Replace NaNs in unit_of_measure for empty strings
    units = _build_unit(table_metadata)
    short_units = _build_short_unit(units)
    table_metadata = table_metadata.assign(
        license_name=license_.name,
        license_url=license_.url,
        unit=units,
        short_unit=short_units,
    ).drop(columns=["license_type", "unit_of_measure"])
    return table_metadata


def _build_unit(table_metadata: catalog.Table) -> pd.Series:
    """Build unit of measure.

    The unit of measure is created using the `indicator_name` field. We have come up with some rules, which
    you can explore in the source code of the function. Some examples are:

    - Any name containing string "(%...)" is assigned the unit "%..."
    - Any name containing string "(%)" is assigned the unit "%"
    - Any name containing string "(...$)" is assigned the unit "...$"
    - Any name containing string "(days)" is assigned the unit "days"

    The output is a pandas Series with the unit of measure for each variable.

    Parameters
    ----------
    table_metadata : catalog.Table
        Metadata table.

    Returns
    -------
    catalog.Variable
        Variable with units.
    """
    # extract units
    unit_1 = table_metadata["indicator_name"].str.extract(r"\((% [^\)]*)\)")  # (%...)
    unit_12 = table_metadata["indicator_name"].str.extract(r"\((%), tertiary\)")  # (%, tertiary)
    unit_2 = table_metadata["indicator_name"].str.extract(r"\(([^\)]*\$)\)")  # (...$)
    unit_3 = table_metadata["indicator_name"].str.extract(r"\(([^\)]*\%)\)")  # (...%)
    unit_4 = table_metadata["indicator_name"].str.extract(r"[Ee]xpected ([Yy]ears)")  # Expected years...
    unit_4[0] = unit_4[0].apply(lambda x: x.lower() if not pd.isnull(x) else np.nan)
    unit_5 = table_metadata["indicator_name"].str.extract(r"\((per[^%)]*)\)")  # (per...)
    unit_6 = table_metadata["indicator_name"].str.extract(r"\((?:calendar )?(days)\)")  # (days)
    unit_7 = table_metadata["indicator_name"].str.extract(r"\((liters)[^\)]*\)")  # (liters...)
    # Build units
    units = pd.concat([unit_1, unit_12, unit_2, unit_3, unit_4, unit_5, unit_6, unit_7], axis=1)
    # Sanity check
    assert all((-units.isna()).sum(axis=1) <= 1), "Multiple units found!"
    # Final formatting
    units.columns = range(units.shape[1])
    units_ = units[0]
    for i in range(1, units.shape[1]):
        units_ = units_.fillna(units[i])
    # Replace NaNs with empty strings
    units_ = units_.fillna("")
    # Check if units_ is as expected (perhaps new units? wrong units detected?)
    # units_.to_csv("wb_gender.units.check.csv", index=False)
    units_check = pd.read_csv(THIS_DIRECTORY / "wb_gender.units.check.csv")["0"].squeeze()
    assert units_.equals(units_check.fillna("")), (
        "Units are not as expected. Please review the output of _build_unit function. If the output looks fine, then"
        " update auxiliary file wb_gender.units.check.csv."
    )
    return units_


def _build_short_unit(units: Union[pd.Series, catalog.Variable]) -> pd.Series:
    short_units: pd.Series = units.str.contains(r"\%").replace({True: "%", False: ""})
    return short_units


def run(dest_dir: str) -> None:
    # Load meadow dataset
    ds_meadow = load_meadow_ds()
    # Create garden dataset
    ds_garden = init_garden_ds(dest_dir, ds_meadow)
    # Obtain data table
    table = make_table(ds_meadow["wb_gender"])
    # Add table to garden dataset
    ds_garden.add(table)
    # Obtain metadata table
    metadata = make_table_metadata(ds_meadow["wb_gender"], ds_meadow["metadata_variables"])
    # Add table to garden dataset
    ds_garden.add(metadata)
    # Save state
    ds_garden.save()
