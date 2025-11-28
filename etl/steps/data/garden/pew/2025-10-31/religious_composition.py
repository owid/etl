"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

ANY_RELIGION = "any_religion"
UNDER_10K = "< 10,000"


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

    # Custom regions
    tb = add_owid_regions(tb)

    # Create new table with columns country, year, most_popular_religion
    tb_most_popular = make_tb_popular_religion(tb)

    # Add "any religion"
    tb = add_any_religion(tb)

    # Add percentage change
    tb_pct = make_tb_pct_change(tb)

    # Improve table format.
    tables = [
        tb.format(["country", "year", "religion"]),
        tb_most_popular.format(["country", "year"], short_name="most_popular_religion"),
        tb_pct.format(["country", "year", "religion"], short_name="share_change"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def add_owid_regions(tb):
    """Add OWID regions as countries with aggregated data."""
    regions = [
        "North America",
        "South America",
        "Africa",
        "Europe",
        "Asia",
        "Oceania",
    ]
    # Create aggregator object
    agg = paths.regions.aggregator(
        regions=regions,
        aggregations={"count_unrounded": "sum"},
        index_columns=["country", "year", "religion"],
    )
    # Add regional aggregates (sum)
    tb = agg.add_aggregates(tb)

    # Add rounded counts
    ## Sanity check NaN counts only in regions
    mask = tb["count"].isna()
    assert set(tb.loc[mask, "country"].unique()) == set(regions), "Unexpected missing shares after region aggregation!"
    ## Estimate `count` column based on unrounded counts
    x = (tb.loc[mask, "count_unrounded"] / 10000).round().astype(int).astype("string")
    mask_2 = tb.loc[mask, "count_unrounded"] < 10000
    x[mask_2] = UNDER_10K
    tb.loc[mask, "count"] = x

    # Add shares
    column_tmp = "share_tmp"
    tb = agg.add_per_capita(
        tb,
        columns=["count_unrounded"],
        column_new_name=column_tmp,
    )
    assert set(tb.loc[tb["share"].isna(), "country"].unique()) == set(
        regions
    ), "Unexpected missing shares after region aggregation!"
    tb["share"] = tb["share"].fillna(100 * tb[column_tmp])

    # Drop temporary columns
    tb = tb.drop(columns=[column_tmp])

    assert tb.isna().sum().sum() == 0, "Missing values found after adding OWID regions!"

    return tb


def add_any_religion(tb):
    # Get rows with any religion and sum
    tb_religion = tb.loc[tb["religion"] != "religiously_unaffiliated"]
    tb_religion = tb_religion.groupby(["country", "year"], as_index=False)[["share", "count_unrounded"]].sum()

    # Estimate `count` column
    tb_religion["count"] = ((tb_religion["count_unrounded"] / 10000).round() * 10000).astype(int).astype("string")
    mask = tb_religion["count_unrounded"] <= 10000
    tb_religion.loc[mask, "count"] = UNDER_10K

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


def make_tb_pct_change(tb):
    # Sanity check
    assert set(tb["year"].unique()) == {2010, 2020}, "Unexpected years detected!"
    # Separate 2010 and 2020 years
    cols = ["country", "religion", "share"]
    tb_10 = tb.loc[tb["year"] == 2010, cols]
    tb_20 = tb.loc[tb["year"] == 2020, cols]
    # Merge and calculate change
    tb_pct = tb_10.merge(tb_20, on=["country", "religion"], suffixes=("_2010", "_2020"))
    tb_pct["share_change_2010_2020"] = tb_pct["share_2020"] - tb_pct["share_2010"]
    # Keep relevant columns
    tb_pct["year"] = 2020
    tb_pct = tb_pct[["country", "year", "religion", "share_change_2010_2020"]]
    return tb_pct
