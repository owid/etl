"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccinations_global")

    # Read table from meadow dataset.
    tb = ds_meadow["vaccinations_global"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Dtypes
    tb = tb.astype(
        {
            "country": "string",
            "total_vaccinations": float,
            "people_vaccinated": float,
            "people_fully_vaccinated": float,
            "total_boosters": float,
        }
    )

    # Add daily vaccinations
    tb = add_daily_vaccinations(tb)

    # Add interpolation
    tb = add_interpolated_indicators(tb)

    # Add smoothed indicators
    tb = add_smoothed_indicators(tb)

    # Format
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_daily_vaccinations(tb: Table) -> Table:
    """Get daily vaccinations."""
    tb = tb.sort_values(by=["country", "date"])
    # Estimate daily difference
    tb["new_vaccinations"] = tb.groupby("country").total_vaccinations.diff()
    # Set to NaN if day-diff is not 1 day (i.e. make sure `new_vaccinations` only contains daily values)
    tb.loc[tb.date.diff().dt.days > 1, "new_vaccinations"] = np.nan
    return tb


def add_interpolated_indicators(tb: Table) -> Table:
    """Add linearly interpolated indicators.

    New columns:
    - new_vaccinations_interpolated
    - new_people_vaccinated_interpolated
    """

    # Interpolate
    tb_int = tb.loc[:, ["country", "date", "total_vaccinations", "people_vaccinated"]]
    tb_int = geo.interpolate_table(tb_int, "country", "date", all_dates_per_country=True)

    # Estimate difference
    cols = ["total_vaccinations", "people_vaccinated"]
    tb_int[cols] = tb_int.groupby("country")[cols].diff()

    # Rename
    tb_int = tb_int.rename(
        columns={
            "total_vaccinations": "new_vaccinations_interpolated",
            "people_vaccinated": "new_people_vaccinated_interpolated",
        }
    )

    # Add to main table
    tb = tb.merge(tb_int, on=["country", "date"], how="outer")

    return tb


def add_smoothed_indicators(tb: Table) -> Table:
    """Smooth values."""
    columns = [
        "new_vaccinations_interpolated",
        "new_people_vaccinated_interpolated",
    ]

    tb[["new_vaccinations_smoothed", "new_people_vaccinated_smoothed"]] = (
        tb.groupby("country")[columns].rolling(window=7, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    return tb
