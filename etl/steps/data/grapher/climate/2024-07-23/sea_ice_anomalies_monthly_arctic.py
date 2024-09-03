"""Load a garden dataset and create a grapher dataset."""

import re

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum year to consider.
# This is chosen because the minimum year informed is 1978 (with only 2 months informed).
# NOTE: We could include 1979. But, for consistency between yearly and decadal data, we ignore this year.
YEAR_MIN = 1980


def create_yearly_table(tb: Table) -> Table:
    # Ignore the very first years (1978 and 1979), as explained above (where YEAR_MIN is defined).
    _tb = tb[tb["year"] >= YEAR_MIN].reset_index(drop=True)
    # Create a table with a column for each year.
    tb_yearly = _tb.pivot(
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
    # Ignore the very first decade (1970s), since it contains only 2 years.
    _tb = tb[tb["year"] >= YEAR_MIN].reset_index(drop=True)

    # Calculate the sea ice extent of each month, averaged over the same 10 months of each decade.
    # For example, January 1990 will be the average sea ice extent of the 10 months of January between 1990 and 1999.
    tb_grouped = _tb.groupby(["location", "month", "decade"], observed=True, as_index=False).agg(
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

    # Table title (which will also become the dataset title).
    tb.metadata.title = "Monthly sea ice extent"

    description_short_yearly = "Mean sea ice extent for each month. For example, June 2010 represents the sea ice extent averaged over all days in June 2010."
    description_short_decadal = "Average of monthly means over the decade. For example, June 2010 represents the mean sea ice extent for June — calculated as the average over all days in the month — averaged across all Junes from 2010 to 2019."
    for column in tb.drop(columns=["country", "year"]).columns:
        year = int(re.findall(r"\d{4}", column)[0])
        title = column.replace("_", " ").capitalize()
        if 1980 <= year < 1990:
            # Very light blue.
            color = "#E9F2FF"
        elif 1990 <= year < 2000:
            # Light blue.
            color = "#CCE0FF"
        elif 2000 <= year < 2010:
            # Medium blue.
            color = "#99C2FF"
        elif 2010 <= year < 2020:
            # Darker blue.
            color = "#66A3FF"
        else:
            # Dark red.
            color = "#8E0F0F"
        tb[column].metadata.title = title
        if f"{year}s" in column:
            tb[column].metadata.description_short = description_short_decadal
        else:
            tb[column].metadata.description_short = description_short_yearly
        tb[column].metadata.display = {"name": str(year), "color": color}
        tb[column].metadata.presentation.title_public = title

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
    tb.loc[tb["location"] == "Northern Hemisphere", "location"] = "Arctic Ocean"
    tb.loc[tb["location"] == "Southern Hemisphere", "location"] = "Antarctica"
    assert set(tb["location"]) == {"Antarctica", "Arctic Ocean"}, "Unexpected locations."

    # Create columns for month, year, and decade.
    tb["year"] = tb["date"].dt.year
    tb["month"] = tb["date"].dt.month
    tb["decade"] = (tb["year"] // 10) * 10

    # Sanity checks.
    sanity_check_inputs(tb=tb)

    # Select only Arctic Ocean and years after a certain minimum (see explanation above, where YEAR_MIN is defined).
    tb = (
        tb[(tb["location"] == "Arctic Ocean") & (tb["year"] >= YEAR_MIN)]
        .sort_values(["year", "month"], ascending=(False, True))
        .reset_index(drop=True)
        .drop(columns=["location", "date"], errors="raise")
    )

    # Create yearly table, adapted to grapher.
    tb_yearly = tb.drop(columns=["decade"], errors="raise").rename(
        columns={"year": "country", "month": "year"}, errors="raise"
    )

    # Create decadal table.
    # tb_decadal = tb.drop(columns=["year"], errors="raise").rename(columns={"decade": "country"}, errors="raise")

    # Combine both tables.
    # tb_combined = tb_yearly.merge(tb_decadal, on=["country", "year"], how="outer")

    # Create a dictionary of colors.
    years = sorted(set(tb_yearly["country"]), reverse=True)
    colors = {}
    for year in years:
        if 1980 <= year < 1990:
            # Very light blue.
            color = "#E9F2FF"
        elif 1990 <= year < 2000:
            # Light blue.
            color = "#CCE0FF"
        elif 2000 <= year < 2010:
            # Medium blue.
            color = "#99C2FF"
        elif 2010 <= year < 2020:
            # Darker blue.
            color = "#66A3FF"
        else:
            # Dark red.
            color = "#8E0F0F"
        colors[str(year)] = color

    # Improve metadata.
    tb_yearly.metadata.title = "Monthly sea ice extent in the Arctic Ocean"
    tb_yearly["sea_ice_extent"].metadata.presentation.grapher_config = {
        "selectedEntityNames": [str(year) for year in years],
        "selectedEntityColors": colors,
    }

    # Improve format.
    tb_yearly = tb_yearly.format(sort_rows=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_yearly], check_variables_metadata=True)
    ds_grapher.save()
