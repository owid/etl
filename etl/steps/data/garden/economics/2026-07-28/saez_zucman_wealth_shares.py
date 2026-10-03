"""Load a meadow dataset and create a garden dataset.

The source reports wealth shares as fractions (0-1); we convert them to percentages.
Each share exists for two population concepts: "tax units" and "equal-split adults".
"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Cumulative top-share columns per population concept, ordered broadest -> narrowest.
CONCEPTS = {
    "tax_units": ["share_top_10_tax_units", "share_top_1_tax_units", "share_top_0p1_tax_units"],
    "equal_split": ["share_top_10_equal_split", "share_top_1_equal_split", "share_top_0p1_equal_split"],
}
SHARE_COLUMNS = [c for cols in CONCEPTS.values() for c in cols]


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["country"].unique()) == {"United States"}, "Expected United States as the only entity."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert tb["year"].between(1910, 2025).all(), "Year outside the plausible range."
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
    assert 50 < tb["share_top_10_tax_units"].max() < 100, "Top 10% share not on a percentage scale — conversion lost?"
    # Cumulative shares must be non-increasing as the group narrows (top 10% >= top 1% >= top 0.1%).
    for cols in CONCEPTS.values():
        for broader, narrower in zip(cols, cols[1:]):
            pair = tb[[broader, narrower]].dropna()
            assert (pair[broader] >= pair[narrower] - 0.05).all(), f"{broader} < {narrower} for some year."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("saez_zucman_wealth_shares")
    tb = ds_meadow["saez_zucman_wealth_shares"].reset_index()

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
