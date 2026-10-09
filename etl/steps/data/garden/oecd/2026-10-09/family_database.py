"""Garden step that combines OECD family database sources into a single dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Indicators expected in the marriage and divorce rates meadow table.
EXPECTED_MARRIAGE_DIVORCE_INDICATORS = {"marriage_rate", "divorce_rate", "mean_age_first_marriage"}
# Categories expected in the children in families meadow table.
EXPECTED_CHILDREN_INDICATORS = {
    "Living with a single parent",
    "Living with two parents",
    "Other",
    "Two cohabiting parents",
    "Two married parents",
}
# Minimum number of countries with data on births outside marriage.
MIN_COUNTRIES_BIRTHS = 43
# Value bounds per indicator, checked on the output tables.
BOUNDS = {
    "marriage_rate": (0, 20),
    "divorce_rate": (0, 10),
    "mean_age_first_marriage": (15, 45),
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets
    ds_marriage_divorce = paths.load_dataset("marriage_divorce_rates")
    ds_births_outside_marriage = paths.load_dataset("births_outside_marriage")
    ds_children_in_families = paths.load_dataset("children_in_families")
    ds_garden_oecd_hist = paths.load_dataset("family_database")

    # Get tables from each dataset
    tb_marriage_divorce = ds_marriage_divorce.read("marriage_divorce_rates")
    tb_births_outside_marriage = ds_births_outside_marriage.read("births_outside_marriage")
    tb_children_in_families = ds_children_in_families.read("children_in_families")
    tb_garden_oecd_hist = ds_garden_oecd_hist.read("family_database")

    sanity_check_inputs(tb_marriage_divorce, tb_births_outside_marriage, tb_children_in_families)

    # Extract historical data columns
    tb_garden_oecd_hist = tb_garden_oecd_hist[["country", "year", "marriage_rate", "divorce_rate"]]

    #
    # Process data.
    #

    # Harmonize country names for all tables
    tb_marriage_divorce = paths.regions.harmonize_names(tb_marriage_divorce, warn_on_unused_countries=False)
    tb_births_outside_marriage = paths.regions.harmonize_names(
        tb_births_outside_marriage, warn_on_unused_countries=False
    )
    tb_children_in_families = paths.regions.harmonize_names(tb_children_in_families, warn_on_unused_countries=False)

    # Process marriage/divorce rates - merge historical data with new data
    # Filter for marriage and divorce rates only from new data
    tb_marriage_new = tb_marriage_divorce[
        (tb_marriage_divorce["indicator"].isin(["marriage_rate", "divorce_rate"]))
        & (tb_marriage_divorce["gender"] == "Both")
    ][["country", "year", "indicator", "value"]].copy()

    # Pivot to get marriage_rate and divorce_rate as columns
    tb_marriage_new = tb_marriage_new.pivot(
        index=["country", "year"], columns="indicator", values="value"
    ).reset_index()
    tb_marriage_new.columns.name = None

    # Validate historical data has expected columns
    assert "marriage_rate" in tb_garden_oecd_hist.columns, "Historical data missing marriage_rate column"
    assert "divorce_rate" in tb_garden_oecd_hist.columns, "Historical data missing divorce_rate column"

    # Merge historical data with new data - new data takes precedence where it exists
    tb_marriage_combined = pr.merge(
        tb_garden_oecd_hist[["country", "year", "marriage_rate", "divorce_rate"]],
        tb_marriage_new,
        on=["country", "year"],
        how="outer",
        suffixes=("_hist", "_new"),
    )

    # Validate merge created expected columns
    required_cols = ["marriage_rate_hist", "marriage_rate_new", "divorce_rate_hist", "divorce_rate_new"]
    missing_cols = [col for col in required_cols if col not in tb_marriage_combined.columns]
    if missing_cols:
        raise ValueError(f"Merge failed - missing columns: {missing_cols}")

    # Use new data where available, otherwise use historical data
    tb_marriage_combined["marriage_rate"] = tb_marriage_combined["marriage_rate_new"].fillna(
        tb_marriage_combined["marriage_rate_hist"]
    )
    tb_marriage_combined["divorce_rate"] = tb_marriage_combined["divorce_rate_new"].fillna(
        tb_marriage_combined["divorce_rate_hist"]
    )
    tb_marriage_combined = tb_marriage_combined[["country", "year", "marriage_rate", "divorce_rate"]]

    # Convert back to long format for consistency with the rest of the data
    tb_marriage_combined_long = tb_marriage_combined.melt(
        id_vars=["country", "year"],
        value_vars=["marriage_rate", "divorce_rate"],
        var_name="indicator",
        value_name="value",
    )
    # Add gender column
    tb_marriage_combined_long["gender"] = "Both"

    # Process births outside marriage. The latest file covers all years and countries of the older release,
    # so no merge with historical data is needed.
    tb_births_combined = tb_births_outside_marriage[["country", "year", "births_outside_marriage"]].copy()
    tb_births_combined = paths.apply_corrections(tb_births_combined)

    # Keep mean age data from new dataset only (no historical equivalent)
    tb_mean_age = tb_marriage_divorce[tb_marriage_divorce["indicator"] == "mean_age_first_marriage"][
        ["country", "year", "gender", "indicator", "value"]
    ].copy()

    sanity_check_outputs(tb_mean_age, tb_marriage_combined_long, tb_births_combined, tb_children_in_families)

    #
    # Save outputs.
    #
    # Create a new garden dataset with multiple tables
    tables = [
        tb_mean_age.format(["country", "year", "gender", "indicator"], short_name="mean_age_first_marriage"),
        tb_marriage_combined_long.format(
            ["country", "year", "gender", "indicator"], short_name="marriage_divorce_rates"
        ),
        tb_births_combined.format(["country", "year"], short_name="births_outside_marriage"),
        tb_children_in_families.format(["country", "year", "indicator"]),
    ]
    ds_garden = paths.create_dataset(tables=tables, check_variables_metadata=True)

    # Save the dataset
    ds_garden.save()


def sanity_check_inputs(tb_marriage_divorce, tb_births_outside_marriage, tb_children_in_families) -> None:
    indicators = set(tb_marriage_divorce["indicator"].unique())
    assert indicators == EXPECTED_MARRIAGE_DIVORCE_INDICATORS, f"Unexpected marriage/divorce indicators: {indicators}"
    assert "births_outside_marriage" in tb_births_outside_marriage.columns, "Births table missing its value column"
    indicators = set(tb_children_in_families["indicator"].unique())
    assert indicators == EXPECTED_CHILDREN_INDICATORS, f"Unexpected children in families indicators: {indicators}"


def sanity_check_outputs(tb_mean_age, tb_marriage_combined_long, tb_births_combined, tb_children_in_families) -> None:
    # Rates and ages lie within plausible bounds.
    for tb in [tb_mean_age, tb_marriage_combined_long]:
        for indicator, (low, high) in BOUNDS.items():
            values = tb.loc[tb["indicator"] == indicator, "value"].dropna()
            out = values[(values < low) | (values > high)]
            assert out.empty, f"{indicator} outside [{low}, {high}]: {sorted(out.unique())}"

    # Shares are percentages.
    births = tb_births_combined["births_outside_marriage"].dropna()
    assert births.between(0, 100).all(), "Share of births outside marriage outside [0, 100]"
    children = tb_children_in_families["value"].dropna()
    assert children.between(0, 100).all(), "Children in families shares outside [0, 100]"

    # Children living with a single parent, two parents, or in other arrangements add up to 100%,
    # and children living with two parents split into married and cohabiting parents.
    tb = tb_children_in_families.pivot(index=["country", "year"], columns="indicator", values="value").astype(float)
    total = tb[["Living with a single parent", "Living with two parents", "Other"]].sum(axis=1, min_count=3).dropna()
    assert ((total - 100).abs() < 1).all(), (
        f"Children in families shares don't add up to 100%: {total[(total - 100).abs() >= 1]}"
    )
    split = (tb["Two cohabiting parents"] + tb["Two married parents"] - tb["Living with two parents"]).dropna()
    assert (split.abs() < 0.1).all(), (
        f"Married and cohabiting parents don't add up to two parents: {split[split.abs() >= 0.1]}"
    )

    # Coverage doesn't shrink.
    n_countries = tb_births_combined.dropna(subset=["births_outside_marriage"])["country"].nunique()
    assert n_countries >= MIN_COUNTRIES_BIRTHS, f"Only {n_countries} countries with births outside marriage data"
