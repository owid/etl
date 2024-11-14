"""Load a meadow dataset and create a garden dataset."""


from etl.data_helpers.misc import expand_time_column
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

    # Process main table
    ## Drop NaNs and zeroes
    tb = tb.dropna(subset="total_vaccinations")
    tb = tb[tb["total_vaccinations"] != 0]
    ## Keep first entry
    tb = tb.sort_values("date").drop_duplicates(subset="country", keep="first")
    ## Add reporting column
    tb["reporting"] = 1
    ## Copy metadata
    tb["reporting"] = tb["reporting"].copy_metadata(tb["total_vaccinations"])
    ## Keep relevant columns
    tb = tb[["country", "date", "reporting"]]
    ## Extend
    tb = expand_time_column(tb, time_col="date", dimension_col="country", method="full_range", fillna_method=["ffill"])
    ## Add data type
    tb["type"] = "vaccinations"

    # New table: counts
    ## Count number of countries reporting
    tb_counts = tb.groupby("date", as_index=False)["reporting"].sum()
    ## Rename columns
    tb_counts = tb_counts.rename(
        columns={
            "reporting": "num_countries_reporting",
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

    # Format
    tables = [
        tb.format(["country", "date", "type"], short_name="country_flags"),
        tb_counts.format(["country", "date", "type"], short_name="country_counts"),
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
