"""Load a garden dataset and create a grapher dataset."""

import re

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum year to consider.
# This is chosen because the minimum year informed is 1978 (with only 2 months informed).
# NOTE: We could include 1979. But, for consistency between yearly and decadal data, we ignore this year.
YEAR_MIN = 1980


def create_yearly_table(tb: Table) -> Table:
    tb_yearly = tb.copy()

    tb_yearly = tb_yearly[tb_yearly["year"] == tb_yearly["year"].max()].reset_index(drop=True)
    tb_yearly = tb_yearly.drop(columns=["decade"], errors="raise").rename(
        columns={"year": "country", "month": "year"}, errors="raise"
    )

    return tb_yearly


def create_decadal_table(tb: Table) -> Table:
    tb_decadal = tb.copy()

    # Calculate the sea ice extent of each month, averaged over the same 10 months of each decade.
    # For example, January 1990 will be the average sea ice extent of the 10 months of January between 1990 and 1999.
    tb_decadal["decade"] = tb_decadal["decade"].astype("string") + "s"
    tb_decadal = tb_decadal.groupby(["month", "decade"], observed=True, as_index=False).agg(
        {"sea_ice_extent_arctic": "mean", "sea_ice_extent_antarctic": "mean"}
    )
    tb_decadal = tb_decadal.rename(columns={"decade": "country", "month": "year"}, errors="raise")

    return tb_decadal


def improve_metadata(tb: Table) -> Table:
    tb = tb.astype({"country": "string"}).copy()

    # Gather years in the data, and assign colors to them.
    colors = {}
    columns = [str(year) for year in set(tb["country"])]
    years = [int(re.findall(r"\d{4}", column)[0]) for column in columns]
    for year, column in zip(years, columns):
        if 1980 <= year < 1990:
            # Light blue.
            color = "#CCE5FF"
        elif 1990 <= year < 2000:
            # Medium light blue.
            color = "#99CCFF"
        elif 2000 <= year < 2010:
            # Medium blue.
            color = "#6699FF"
        elif 2010 <= year < 2020:
            # Darker blue.
            color = "#3366FF"
        elif year == max(years):
            # Black.
            color = "#000000"
        else:
            # Red.
            color = "#F89B9B"
        colors[column] = color

    # Rename table.
    tb.metadata.title = "Sea ice extent in the northern and southern hemispheres by decade"

    for column in tb.drop(columns=["country", "year"]).columns:
        location = column.split("sea_ice_extent_")[-1].title()
        title = f"Monthly sea ice extent in the {location}, decadal average"
        description_short = (
            "Each point represents the monthly average sea ice extent, averaged across all years within the decade."
        )
        subtitle = (
            description_short
            + " The current decade is highlighted in red, with the current year shown in black for comparison."
        )
        footnote = "The horizontal axis shows months from January (1) to December (12). All years have data for all 12 months, except 1987 and 1988 (each missing one month) and the current year."

        tb[column].metadata.title = title
        tb[column].metadata.description_short = description_short
        tb[column].metadata.presentation.title_public = title
        tb[column].metadata.presentation.grapher_config = {
            "subtitle": subtitle,
            "note": footnote,
            "selectedEntityNames": columns,
            "selectedEntityColors": colors,
            "originUrl": "https://ourworldindata.org/climate-change",
            "hideAnnotationFieldsInTitle": {"time": True},
            "entityType": "year",
            "entityTypePlural": "years",
        }

    return tb


def sanity_check_inputs(tb: Table) -> None:
    error = "Expected 1978 to be the first year in the data. Data may have changed. Consider editing YEAR_MIN"
    assert tb["year"].min() == 1978, error

    # All years should have 12 months except:
    # * The very first year in the data (1978).
    # * Years 1987 and 1988, that have 11 months (because 1987-12 and 1988-01 are missing).
    # * The very last year in the data (since it's the ongoing year).
    error = "Expected 12 months per year."
    assert (
        tb[~tb["year"].isin([tb["year"].min(), 1987, 1988, tb["year"].max()])]
        .groupby(["location", "year"])
        .count()["sea_ice_extent"]
        == 12
    ).all(), error
    # Each month-year should appear only once in the data.
    error = "Repeated months."
    assert (tb.groupby(["location", "year", "month"]).count()["sea_ice_extent"] == 1).all(), error
    # Each month-decade should appear 10 times (one per year in the decade), except:
    # * The very first decade (1970s), since it starts in 1978. This decade will be ignored in the decadal data.
    # * January and December 1980s, that appear 9 times (because 1987-12 and 1988-01 are missing).
    # * The very last decade (since it's the ongoing decade).
    error = "Expected 10 instances of each month per decade (except in specific cases)."
    exceptions = tb[
        (tb["decade"] == tb["decade"].min())
        | (tb["decade"] == tb["decade"].max())
        | ((tb["decade"] == 1980) & (tb["month"].isin([1, 12])))
    ].index
    assert (tb.drop(exceptions).groupby(["location", "decade", "month"]).count()["sea_ice_extent"] == 10).all(), error
    assert (
        tb[(tb["decade"] == 1980) & (tb["month"].isin([1, 12]))]
        .groupby(["location", "decade", "month"])
        .count()["sea_ice_extent"]
        == 9
    ).all(), error
    assert (
        tb[(tb["decade"] == tb["decade"].max())].groupby(["location", "decade", "month"]).count()["sea_ice_extent"]
        <= 10
    ).all(), error


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("sea_ice_index")
    tb = ds_garden.read("sea_ice_index", safe_types=False)

    #
    # Process data.
    #
    # Rename locations conveniently.
    tb = tb.astype({"location": "string"})
    tb.loc[tb["location"] == "Northern Hemisphere", "location"] = "Arctic"
    tb.loc[tb["location"] == "Southern Hemisphere", "location"] = "Antarctic"
    assert set(tb["location"]) == {"Arctic", "Antarctic"}, "Unexpected locations."

    # Create columns for month, year, and decade.
    tb["year"] = tb["date"].dt.year
    tb["month"] = tb["date"].dt.month
    tb["decade"] = (tb["year"] // 10) * 10

    # Sanity checks.
    sanity_check_inputs(tb=tb)

    # Select years after a certain minimum (see explanation above, where YEAR_MIN is defined) and a certain location.
    tb = (
        tb[(tb["year"] >= YEAR_MIN)]
        .sort_values(["year", "month"], ascending=(False, True))
        .drop(columns=["date"], errors="raise")
        .reset_index(drop=True)
    )

    # Create one column for each hemisphere.
    tb = tb.pivot(
        index=["year", "decade", "month"], columns=["location"], values=["sea_ice_extent"], join_column_levels_with="_"
    ).underscore()

    # Create yearly table, adapted to grapher.
    tb_yearly = create_yearly_table(tb=tb)

    # Create decadal table, adapted to grapher.
    tb_decadal = create_decadal_table(tb=tb)

    # Combine both tables (take decadal data prior to 2020, and individual years from 2020 on).
    tb_combined = pr.concat([tb_decadal, tb_yearly], ignore_index=True)

    # Improve metadata.
    tb_combined = improve_metadata(tb=tb_combined)

    # Improve format.
    tb_combined = tb_combined.format(sort_rows=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_combined])
    ds_grapher.save()
