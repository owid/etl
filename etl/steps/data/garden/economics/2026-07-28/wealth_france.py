"""Load a meadow dataset and create a garden dataset.

The source reports wealth shares as fractions (0-1); we convert them to percentages.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Wealth-share columns. bottom 50% + middle 40% + top 10% partition the population and
# sum to 100%; top 1% is a subset of the top 10%.
PARTITION_COLUMNS = ["share_bottom_50", "share_middle_40", "share_top_10"]
SHARE_COLUMNS = PARTITION_COLUMNS + ["share_top_1"]


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["country"].unique()) == {"France"}, "Expected France as the only entity."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert tb["year"].between(1800, 2020).all(), "Year outside the plausible range."
    # Source values are fractions of total wealth, so they must lie in (0, 1].
    for col in SHARE_COLUMNS:
        vals = tb[col].dropna()
        assert vals.between(0, 1).all(), f"{col} has values outside 0-1 before conversion to %."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    for col in SHARE_COLUMNS:
        vals = tb[col].dropna()
        assert vals.between(0, 100).all(), f"{col} has values outside 0-100%."
    # Guard the fraction->% conversion: the top 10% share should be tens of percent, not a fraction.
    assert 50 < tb["share_top_10"].max() < 100, "Top 10% share not on a percentage scale — conversion lost?"
    # The three partition shares must add up to 100% (they cover the whole population).
    partition_sum = tb[PARTITION_COLUMNS].sum(axis=1)
    assert partition_sum.between(99.5, 100.5).all(), "Bottom 50% + middle 40% + top 10% does not sum to ~100%."
    # The top 1% is nested inside the top 10%.
    pair = tb[["share_top_10", "share_top_1"]].dropna()
    assert (pair["share_top_10"] >= pair["share_top_1"] - 0.05).all(), "Top 10% share below top 1% for some year."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("wealth_france")
    tb = ds_meadow["wealth_france"].reset_index()

    #
    # Process data.
    #
    sanity_check_inputs(tb)

    # Convert wealth shares from fractions (0-1) to percentages.
    for col in SHARE_COLUMNS:
        tb[col] = tb[col] * 100

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
