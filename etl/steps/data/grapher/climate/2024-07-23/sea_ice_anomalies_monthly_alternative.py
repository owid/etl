"""Load a garden dataset and create a grapher dataset."""

import re

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum year to consider.
# This is chosen because the minimum year informed is 1978 (with only 2 months informed).
# NOTE: We could include 1979. But, for consistency between yearly and decadal data, we ignore this year.
YEAR_MIN = 1980


def create_yearly_table(tb: Table) -> Table:
    tb_yearly = tb.copy()

    tb_yearly = tb_yearly[tb_yearly["year"] == tb_yearly["year"].max()].reset_index(drop=True)

    # NOTE: Ideally, we would rename these entities that appear in the legend. However, there seems to be a bug (somewhere between ETL and grapher), that causes "selectedEntityColors" to be lower case, removing spaces, making them different to "selectedEntityNames".
    # Therefore, for now, I will use names that are not affected by the bug.
    # tb_yearly["country"] = tb_yearly["location"] + " " + tb_yearly["year"].astype("string")
    tb_yearly["country"] = tb_yearly["location"].str.lower() + ":" + tb_yearly["year"].astype("string")
    tb_yearly = tb_yearly.drop(columns=["decade", "year", "location"], errors="raise").rename(
        columns={"month": "year"}, errors="raise"
    )

    return tb_yearly


def create_decadal_table(tb: Table) -> Table:
    tb_decadal = tb.copy()

    # Calculate the sea ice extent of each month, averaged over the same 10 months of each decade.
    # For example, January 1990 will be the average sea ice extent of the 10 months of January between 1990 and 1999.
    tb_decadal = tb_decadal.groupby(["location", "month", "decade"], observed=True, as_index=False).agg(
        {"sea_ice_extent": "mean"}
    )

    # tb_decadal["country"] = tb_decadal["location"] + " " + tb_decadal["decade"].astype("string") + "s"
    tb_decadal["country"] = tb_decadal["location"].str.lower() + ":" + tb_decadal["decade"].astype("string") + "s"
    tb_decadal = tb_decadal.drop(columns=["decade", "location"], errors="raise").rename(
        columns={"month": "year"}, errors="raise"
    )

    return tb_decadal


def improve_metadata(tb: Table) -> Table:
    tb = tb.copy()

    # Main title (which will be used for the indicator, the table, and the dataset).
    title = "Monthly sea ice extent in the northern and southern hemispheres"
    description_short_yearly = "Each point represents the sea ice extent, averaged over all days in the month, then averaged across all years in the decade. The current decade is highlighted in red, with the current year shown in black for comparison. Sea ice peaks around February in the north and September in the south."
    footnote = "The horizontal axis shows months from January (1) to December (12). All years have data for all 12 months, except 1987 and 1988 (each missing one month) and the current decade."

    colors = {}
    columns = sorted(set(tb["country"]))
    years = [int(re.findall(r"\d{4}", column)[0]) for column in columns]
    for year, column in zip(years, columns):
        year = int(re.findall(r"\d{4}", column)[0])
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
    tb.metadata.title = title

    # Name of data column (there is only one).
    column = "sea_ice_extent"
    tb[column].metadata.title = title
    tb[column].metadata.description_short = description_short_yearly
    tb[column].metadata.presentation.title_public = title
    # Set color for each entity.
    tb[column].metadata.presentation.grapher_config = {
        "selectedEntityNames": columns,
        "selectedEntityColors": colors,
        "originUrl": "https://ourworldindata.org/climate-change",
        "note": footnote,
        "hideAnnotationFieldsInTitle": {"time": True},
        "entityType": "location and year",
        "entityTypePlural": "locations and years",
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
    tb.loc[tb["location"] == "Northern Hemisphere", "location"] = "North"
    tb.loc[tb["location"] == "Southern Hemisphere", "location"] = "South"
    assert set(tb["location"]) == {"North", "South"}, "Unexpected locations."

    # Create columns for month, year, and decade.
    tb["year"] = tb["date"].dt.year
    tb["month"] = tb["date"].dt.month
    tb["decade"] = (tb["year"] // 10) * 10

    # Sanity checks.
    sanity_check_inputs(tb=tb)

    # Select years after a certain minimum (see explanation above, where YEAR_MIN is defined).
    tb = (
        tb[(tb["year"] >= YEAR_MIN)]
        .sort_values(["year", "month"], ascending=(False, True))
        .drop(columns=["date"], errors="raise")
        .reset_index(drop=True)
    )

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
    ds_grapher = create_dataset(dest_dir, tables=[tb_combined], check_variables_metadata=True)
    ds_grapher.save()
