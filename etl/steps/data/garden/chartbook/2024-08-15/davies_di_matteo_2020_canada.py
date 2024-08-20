"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("davies_di_matteo_2020_canada")

    # Read table from meadow dataset.
    tb = ds_meadow["davies_di_matteo_2020_canada"].reset_index()

    #
    # Process data.
    #
    # Keep only the share of the top 1% and separate the series according to their original source.
    tb = keep_top_1_percent_and_separate_series(tb)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def keep_top_1_percent_and_separate_series(tb: Table) -> Table:
    """
    Keep only the share of the top 1% and separate the series according to their original source.
    """
    tb = tb[["country", "year", "top_1"]].copy()

    # Separate series according to their original source.
    tb.loc[tb["year"] <= 1902, "source"] = "ontario_families"
    tb.loc[(tb["year"] >= 1945) & (tb["year"] <= 1968), "source"] = "canada_adults"
    tb.loc[tb["year"] >= 1970, "source"] = "canada_families"

    # Make table wide
    tb = tb.pivot(index=["country", "year"], columns="source", values="top_1").reset_index()

    return tb
