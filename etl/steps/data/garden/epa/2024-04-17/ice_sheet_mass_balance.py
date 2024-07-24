"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def fix_multiple_rows_for_the_same_year(tb: Table) -> Table:
    # There are repeated years, but there are no ambiguities (i.e. for each column, either the first or the second
    # repeated year has data, not both).
    # To fix that, remove nans from each column and merge them together.
    tb_corrected = tb[["year"]].drop_duplicates().reset_index(drop=True)
    for column in tb.columns[1:]:
        tb_column = tb[["year", column]].dropna().reset_index(drop=True)
        assert tb_column[tb_column.duplicated(subset="year", keep=False)].empty
        tb_corrected = tb_corrected.merge(tb_column, how="outer", on="year")

    return tb_corrected


def decimal_date_to_date(year: int) -> str:
    return (pd.to_datetime(year, format="%Y") + pd.Timedelta(days=(year % 1) * 364.2425)).date()


def separate_antarctica_and_greenland_data(tb: Table) -> Table:
    columns_antarctica = {
        "date": "date",
        "nasa__antarctica_land_ice_mass": "land_ice_mass_nasa",
        "imbie__antarctica_cumulative_ice_mass_change": "cumulative_ice_mass_change_imbie",
    }
    tb_antarctica = (
        tb[list(columns_antarctica)]
        .rename(columns=columns_antarctica, errors="raise")
        .assign(**{"location": "Antarctica"})
        .copy()
    )
    columns_greenland = {
        "date": "date",
        "nasa__greenland_land_ice_mass": "land_ice_mass_nasa",
        "imbie__greenland_cumulative_ice_mass_change": "cumulative_ice_mass_change_imbie",
    }
    tb_greenland = (
        tb[list(columns_greenland)]
        .rename(columns=columns_greenland, errors="raise")
        .assign(**{"location": "Greenland"})
        .copy()
    )

    # Combine data for Antarctica and Greenland.
    tb_combined = pr.concat([tb_antarctica, tb_greenland], ignore_index=True)

    return tb_combined


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("ice_sheet_mass_balance")
    tb = ds_meadow["ice_sheet_mass_balance"].reset_index()

    #
    # Process data.
    #
    # Fix issue with the original data, where there are multiple rows for the same year.
    tb = fix_multiple_rows_for_the_same_year(tb=tb)

    # Remove empty rows.
    tb = tb.dropna(how="all")

    # Create a date column (given that "year" is given with decimals).
    tb["date"] = tb["year"].apply(decimal_date_to_date).astype(str)

    # Separate data for Antarctica and Greenland.
    tb = separate_antarctica_and_greenland_data(tb=tb)

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
