"""Load a garden dataset and create an explorers dataset.

The output csv file will feed our Climate Change Impacts data explorer:
https://ourworldindata.org/explorers/climate-change
"""

from typing import Dict, Optional

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load GISS dataset surface temperature analysis, and read monthly data.
    ds_giss = paths.load_dataset("surface_temperature_analysis")
    tb_giss = ds_giss["surface_temperature_analysis"].reset_index()

    # Load NSIDC dataset of sea ice index.
    ds_nsidc = paths.load_dataset("sea_ice_index")
    tb_nsidc = ds_nsidc["sea_ice_index"].reset_index()

    #
    # Process data.
    #
    # Combine monthly data from different tables.
    # NOTE: For now, use only the existing data from GISS.
    tb_monthly = tb_giss.merge(
        tb_nsidc,
        how="outer",
        on=["location", "date"],
        validate="one_to_one",
        short_name="climate_change_impacts_monthly",
    )

    # Create table of annual data.
    # tb_annual = resample_monthly_to_yearly_data(tb_monthly)
    # TODO: For now (that no variables need to be resampled) copy the monthly table.
    tb_annual = tb_monthly.copy()
    tb_annual = tb_annual.rename(columns={"date": "year"}, errors="raise")
    tb_annual.metadata.short_name = "climate_change_impacts_annual"

    # Set an appropriate index to monthly and annual tables, and sort conveniently.
    tb_monthly = tb_monthly.set_index(["location", "date"], verify_integrity=True).sort_index()
    tb_annual = tb_annual.set_index(["location", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create explorer dataset with combined table in csv format.
    ds_explorer = create_dataset(dest_dir, tables=[tb_annual, tb_monthly], formats=["csv"])
    ds_explorer.save()


def resample_monthly_to_yearly_data(
    tb: Table, aggregations: Optional[Dict] = None, date_column: str = "date", year_column: str = "year"
) -> Table:
    """Resample a table of monthly data (with a column of dates) into another of yearly data.

    Parameters
    ----------
    tb : Table
        Table to be resampled.
    aggregations : dict
        Type of aggregation to apply to each column in the table. If not specified, 'mean' will be applied.
    date_column : str
        Name of date column, that should be present in the input table.
    year_column : str
        Name of year column, that will be added to the output table.

    Returns
    -------
    tb_resampled : Table
        Original table, after resampling.

    """
    tb_resampled = tb.copy().astype({"location": str})
    # Define dictionary of aggregations.
    if aggregations is None:
        # By default, if nothing specified, assume that all columns should be averaged.
        resampling = {col: "mean" for col in tb_resampled.columns if col not in [date_column]}
    else:
        # Ensure columns in aggregations dictionary exist in dataset.
        resampling = {
            col: aggregations[col] for col in aggregations if col in tb_resampled.columns if col != date_column
        }
    # Convert date column into a datetime column, and resample.
    tb_resampled[date_column] = pd.to_datetime(tb_resampled[date_column])
    tb_resampled = tb_resampled.resample("Y", on=date_column).agg(resampling).reset_index()
    # Create year column and delete original date column.
    tb_resampled[year_column] = tb_resampled[date_column].dt.year
    tb_resampled = tb_resampled.drop(columns="date").reset_index(drop=True)

    return tb_resampled
