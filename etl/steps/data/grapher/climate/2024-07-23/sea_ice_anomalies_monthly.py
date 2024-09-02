"""Load a garden dataset and create a grapher dataset."""

import re

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def create_yearly_table(tb: Table) -> Table:
    # Create a table with a column for each year.
    tb_yearly = tb.pivot(
        index=["location", "month"], columns=["year"], values="sea_ice_extent", join_column_levels_with=""
    )

    # To adjust to grapher, rename location column.
    tb_yearly = tb_yearly.rename(columns={"location": "country", "month": "year"}, errors="raise")

    # Ensure all column names are strings.
    tb_yearly = tb_yearly.rename(
        columns={
            column: f"sea_ice_extent_in_{column}" for column in tb_yearly.drop(columns=["country", "year"]).columns
        },
        errors="raise",
    )

    return tb_yearly


def create_decadal_table(tb: Table) -> Table:
    # Calculate decadal averages.
    tb_grouped = tb.groupby(["location", "month", "decade"], observed=True, as_index=False).agg(
        {"sea_ice_extent": "mean"}
    )

    # Pivot the table to get decades as columns.
    tb_decadal = tb_grouped.pivot(
        index=["location", "month"], columns="decade", values="sea_ice_extent", join_column_levels_with=""
    )

    # To adjust to grapher, rename location column.
    tb_decadal = tb_decadal.rename(columns={"location": "country", "month": "year"}, errors="raise")

    # Rename columns conveniently.
    tb_decadal = tb_decadal.rename(
        columns={
            column: f"sea_ice_extent_in_the_{int(column)}s"
            for column in tb_decadal.drop(columns=["country", "year"]).columns
        }
    )

    return tb_decadal


def improve_metadata(tb: Table) -> Table:
    tb = tb.copy()

    tb.metadata.title = "Monthly sea ice extent"
    for column in tb.drop(columns=["country", "year"]).columns:
        year = int(re.findall(r"\d{4}", column)[0])
        title = column.replace("_", " ").capitalize()
        if 1970 <= year < 1980:
            color = "#E9F2FF"  # 1970s: Very light blue
        elif 1980 <= year < 1990:
            color = "#CCE0FF"  # 1980s: Light blue
        elif 1990 <= year < 2000:
            color = "#99C2FF"  # 1990s: Medium blue
        elif 2000 <= year < 2010:
            color = "#66A3FF"  # 2000s: Darker blue
        elif 2010 <= year < 2020:
            color = "#3385FF"  # 2010s: Even darker blue
        else:  # From 2020 onwards, use dark red.
            color = "#8E0F0F"
        tb[column].metadata.title = title
        tb[column].metadata.display = {"name": str(year), "color": color}
        tb[column].metadata.presentation.title_public = title

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("sea_ice_index")
    tb = ds_garden.read_table("sea_ice_index")

    #
    # Process data.
    #
    # Rename locations conveniently.
    tb = tb.astype({"location": "string"})
    tb.loc[tb["location"] == "Northern Hemisphere", "location"] = "Arctic Ocean"
    tb.loc[tb["location"] == "Southern Hemisphere", "location"] = "Antarctica"
    assert set(tb["location"]) == {"Antarctica", "Arctic Ocean"}, "Unexpected locations."

    # For visualization purposes, create one indicator for each year.
    # Each indicator will have "location" as the "entity" column, and one time stamp for each month in the "time" column.
    # TODO: Alternatively, consider creating only one indicator for arctic ocean, and another for antarctica, where the "entity" column is the year, and the "time" column is the month.
    # TODO: Add descriptions (short description, and processing description to explain averages).

    # Create a month and a year columns.
    tb["year"] = tb["date"].dt.year
    tb["month"] = tb["date"].dt.month
    assert (tb.groupby(["location", "year"]).count()["sea_ice_extent"] <= 12).all(), "More than 12 months in a year."
    assert (tb.groupby(["location", "year", "month"]).count()["sea_ice_extent"] == 1).all(), "Repeated months."

    # Add a column for decade.
    tb["decade"] = (tb["year"] // 10) * 10

    # Create yearly table.
    tb_yearly = create_yearly_table(tb=tb)

    # Create decadal table.
    tb_decadal = create_decadal_table(tb=tb)

    # Improve metadata.
    tb_yearly = improve_metadata(tb=tb_yearly)
    tb_decadal = improve_metadata(tb=tb_decadal)

    # Combine both tables.
    tb_combined = tb_yearly.merge(tb_decadal, on=["country", "year"], how="outer")

    # Improve format.
    tb_combined = tb_combined.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_combined], check_variables_metadata=True)
    ds_grapher.save()
