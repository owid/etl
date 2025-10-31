"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

ANY_RELIGION = "any_religion"


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("religious_composition")

    # Read table from meadow dataset.
    tb = ds_meadow.read("religious_composition")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Lower case religion names
    tb["religion"] = tb["religion"].str.lower()

    # Process types
    tb["count_unrounded"] = tb["count_unrounded"].str.replace(",", "", regex=False).astype(int)
    tb["count"] = tb["count"].str.replace(",", "", regex=False)

    # Create new table with columns country, year, most_popular_religion
    tb_most_popular = make_tb_popular_religion(tb)

    # Add "any religion"
    tb = add_any_religion(tb)

    # Improve table format.
    tables = [
        tb.format(["country", "year", "religion"]),
        tb_most_popular.format(["country", "year"], short_name="most_popular_religion"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def add_any_religion(tb):
    # Get rows with any religion and sum
    tb_religion = tb.loc[tb["religion"] != "religiously_unaffiliated"]
    tb_religion = tb_religion.groupby(["country", "year"], as_index=False)[["share", "count_unrounded"]].sum()

    # Estimate `count` column
    tb_religion["count"] = ((tb_religion["count_unrounded"] / 10000).round() * 10000).astype(int).astype("string")
    mask = tb_religion["count_unrounded"] <= 10000
    tb_religion.loc[mask, "count"] = "<10,000"

    tb_religion["religion"] = ANY_RELIGION
    tb = pr.concat([tb, tb_religion])
    return tb


def make_tb_popular_religion(tb):
    # Drop "any religion"
    tb = tb.loc[tb["religion"] != ANY_RELIGION]

    # Group by country and year, find religion with highest share
    tb_popular = tb.sort_values("share", ascending=False).drop_duplicates(["country", "year"], keep="first")

    # Select and rename columns
    tb_popular = tb_popular[["country", "year", "religion"]].rename(columns={"religion": "most_popular_religion"})

    tb_popular["most_popular_religion"] = tb_popular["most_popular_religion"].str.replace("_", " ").str.title()

    # Reset index
    tb_popular = tb_popular.reset_index(drop=True)

    return tb_popular
