"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers.misc import expand_time_column
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets.
    ds_meadow = paths.load_dataset("countries_reporting")
    ds_vax = paths.load_dataset("vaccinations_global")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccinations", safe_types=False)
    tb_latest = ds_vax.read("vaccinations_global", safe_types=False)

    # 1/ LATEST DATA
    ## 1.1/ Process main table
    ## Drop NaNs and zeroes
    tb_latest = tb_latest.dropna(subset="total_vaccinations")
    tb_latest = tb_latest.loc[tb_latest["total_vaccinations"] != 0]
    ## Keep first entry
    tb_latest = tb_latest.sort_values("date").drop_duplicates(subset="country", keep="first")

    # Get table with flags whenever a country reports
    tb_latest = make_table_with_country_flags(tb_latest)

    ## 1.2/ New table: counts
    tb_counts = make_table_counts(tb_latest)

    ## 1.3/ Renamings
    tb_latest = tb_latest.rename(
        columns={
            "reporting": "reporting_latest",
        }
    )
    tb_counts = tb_counts.rename(
        columns={
            "num_countries_reporting": "num_countries_reporting_latest",
            "share_countries_reporting": "share_countries_reporting_latest",
        }
    )
    # 2/ GIT HISTORY
    ## 2.0/ Only keep countries (as per the UN list) and avoid double-countings
    COUNTRIES_SKIP = [
        "England",
        "Scotland",
        "Wales",
        "Northern Ireland",
    ]
    tb = tb.loc[~tb["country"].isin(COUNTRIES_SKIP)]

    ## 2.1/ Process main table
    tb_history = make_table_with_country_flags(tb)
    ## 2.2/ New table: counts
    tb_hist_counts = make_table_counts(tb_history)
    ## 2.3/ Aux
    tb["num_days_delay_in_reporting"] = (tb["date_first_reported"] - tb["date_first_value"]).dt.days
    tb["num_days_delay_in_reporting"] = tb["num_days_delay_in_reporting"].copy_metadata(tb["date_first_reported"])
    tb["year"] = 2023
    tb = tb[["country", "year", "num_days_delay_in_reporting"]]

    # Format
    tables = [
        tb_latest.format(["country", "date", "type"], short_name="country_flags"),
        tb_counts.format(["country", "date", "type"], short_name="country_counts"),
        tb_history.format(["country", "date", "type"], short_name="country_flags_historical"),
        tb_hist_counts.format(["country", "date", "type"], short_name="country_counts_historical"),
        tb.format(["country", "year"], short_name="country_reporting_delay"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_table_with_country_flags(tb: Table):
    ## Add reporting column
    tb["reporting"] = 1
    ## Copy metadata
    tb["reporting"] = tb["reporting"].copy_metadata(tb["date"])
    ## Keep relevant columns
    tb = tb.loc[:, ["country", "date", "reporting"]]
    ## Extend
    tb = expand_time_column(tb, time_col="date", dimension_col="country", method="full_range", fillna_method=["ffill"])
    ## Add data type
    tb["type"] = "vaccinations"

    return tb


def make_table_counts(tb: Table, col_name: str = "reporting"):
    ## Count number of countries reporting
    tb_counts = tb.groupby("date", as_index=False)[col_name].sum()
    ## Rename columns
    tb_counts = tb_counts.rename(
        columns={
            col_name: "num_countries_reporting",
        }
    )
    ## Estimate ratio
    tb_counts["share_countries_reporting"] = (
        tb_counts["num_countries_reporting"] / tb_counts["num_countries_reporting"].max()
    )
    ## Add world
    tb_counts["country"] = "World"
    ## Add data type
    tb_counts["type"] = "vaccinations"
    ## Sort columns
    tb_counts = tb_counts[["country", "date", "type", "num_countries_reporting", "share_countries_reporting"]]

    return tb_counts
