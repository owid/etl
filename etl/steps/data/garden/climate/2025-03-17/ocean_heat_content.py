"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("ocean_heat_content")
    tb_monthly = ds_meadow["ocean_heat_content_monthly"].reset_index()
    tb_annual = ds_meadow["ocean_heat_content_annual"].reset_index()

    #
    # Process data.
    #
    # Improve the format of the date column in monthly date (assume the middle of the month for each data point).
    tb_monthly["date"] = (
        tb_monthly["date"].str.split("-").str[0] + "-" + tb_monthly["date"].str.split("-").str[1].str.zfill(2) + "-15"
    )

    # Replace date column (where all years are given as, e.g. 1955.5, 2000.5) by year column in annual data.
    tb_annual["year"] = tb_annual["date"].astype(int)
    tb_annual = tb_annual.drop(columns=["date"], errors="raise")

    # Instead of having a column for depth, create columns of heat content for each depth.
    tb_monthly["depth"] = tb_monthly["depth"].astype(str) + "m"
    tb_monthly = tb_monthly.pivot(index=["location", "date"], columns="depth", join_column_levels_with="_")
    tb_annual["depth"] = tb_annual["depth"].astype(str) + "m"
    tb_annual = tb_annual.pivot(index=["location", "year"], columns="depth", join_column_levels_with="_")

    # Set an appropriate index to each table and sort conveniently.
    tb_monthly = tb_monthly.format(["location", "date"], sort_columns=True)
    tb_annual = tb_annual.format(["location", "year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_annual, tb_monthly])
    ds_garden.save()
