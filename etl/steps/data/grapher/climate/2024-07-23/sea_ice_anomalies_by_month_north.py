"""Load a garden dataset and create a grapher dataset."""


from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum year to consider.
# This is chosen because the minimum year informed is 1978 (with only 2 months informed).
# NOTE: We could include 1979. But, for consistency between yearly and decadal data, we ignore this year.
YEAR_MIN = 1980

# Location (either "Arctic" or "Antarctic").
LOCATION = "Arctic"

# Reference year. The monthly value of sea ice extent on this year will be subtracted from each data point.
REFERENCE_YEAR = 2000


def improve_metadata(tb: Table) -> Table:
    tb = tb.copy()

    # Main title (which will be used for the indicator, the table, and the dataset).
    title = f"Monthly sea ice extent in the {LOCATION}"
    description_short_yearly = f"Each point represents the sea ice extent, averaged over all days in the month, with respect to the same month in {REFERENCE_YEAR}."
    footnote = (
        "All years have data for all 12 months, except 1987 and 1988 (each missing one month) and the current year."
    )

    # Rename table.
    tb.metadata.title = title

    # Name of data column (there is only one).
    column = "sea_ice_extent"
    tb[column].metadata.title = title
    tb[column].metadata.description_short = description_short_yearly
    tb[column].metadata.presentation.title_public = title
    # Set color for each entity.
    tb[column].metadata.presentation.grapher_config = {
        # "selectedEntityNames": columns,
        # "selectedEntityColors": colors,
        "originUrl": "https://ourworldindata.org/climate-change",
        "note": footnote,
        # "hideAnnotationFieldsInTitle": {"time": True},
        "entityType": "month",
        "entityTypePlural": "months",
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
    tb.loc[tb["location"] == "Northern Hemisphere", "location"] = "Arctic"
    tb.loc[tb["location"] == "Southern Hemisphere", "location"] = "Antarctic"
    assert set(tb["location"]) == {"Arctic", "Antarctic"}, "Unexpected locations."

    # Create columns for month, year, and decade.
    tb["year"] = tb["date"].dt.year
    tb["month"] = tb["date"].dt.month
    tb["month_name"] = tb["date"].dt.strftime("%B")
    tb["decade"] = (tb["year"] // 10) * 10

    # Sanity checks.
    sanity_check_inputs(tb=tb)

    # Select years after a certain minimum (see explanation above, where YEAR_MIN is defined) and a certain location.
    tb = (
        tb[(tb["year"] >= YEAR_MIN) & (tb["location"] == LOCATION)]
        .sort_values(["year", "month"], ascending=(False, True))
        .drop(columns=["date", "location", "month", "decade"], errors="raise")
        .reset_index(drop=True)
    )

    # Subtract the value of sea ice extent on a certain reference year.
    tb_reference = tb[tb["year"] == REFERENCE_YEAR][["month_name", "sea_ice_extent"]].rename(
        columns={"sea_ice_extent": "sea_ice_extent_reference"}, errors="raise"
    )
    tb = tb.merge(tb_reference, on=["month_name"], how="left")
    tb["sea_ice_extent"] -= tb["sea_ice_extent_reference"]
    tb = tb.drop(columns=["sea_ice_extent_reference"], errors="raise")

    # Adapt column names to grapher.
    tb = tb.rename(columns={"month_name": "country"}, errors="raise")

    # Improve metadata.
    tb = improve_metadata(tb=tb)

    # Improve format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
