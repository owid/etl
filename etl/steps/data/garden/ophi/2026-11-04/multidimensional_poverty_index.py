"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MEASURE_NAMES = {
    "A": "intensity",
    "H": "headcount_ratio",
    "M0": "mpi",
    "hd": "uncensored_headcount_ratio",
    "hdk": "censored_headcount_ratio",
    "sev": "severe",
    "vuln": "vulnerable",
}

# Define categories to keep in each column
CATEGORIES_TO_KEEP = {
    "loa": ["area", "nat"],
    "measure": list(MEASURE_NAMES.keys()),
}

# Define indicator categories
INDICATOR_NAMES = [
    "Assets",
    "Child mortality",
    "Cooking fuel",
    "Drinking water",
    "Electricity",
    "Housing",
    "Nutrition",
    "Sanitation",
    "School attendance",
    "Years of schooling",
]

# Define index column for the final table
INDEX_COLS = ["country", "year", "indicator", "area", "flavor"]

# Expected areas in the output.
EXPECTED_AREAS = {"National", "Urban", "Rural", "Camp"}

# Minimum number of countries per flavor.
# NOTE: OPHI drops countries whose latest survey is too old (Namibia in 2026), so a lower count can be legitimate.
# Check the "Countries excluded" section of the methodological note before lowering these.
MIN_COUNTRIES = {"Current margin estimate": 108, "Harmonized over time": 89}

# Weights of each indicator in the MPI (health and education indicators weigh 1/6, living standards 1/18).
INDICATOR_WEIGHTS = {
    "Nutrition": 1 / 6,
    "Child mortality": 1 / 6,
    "Years of schooling": 1 / 6,
    "School attendance": 1 / 6,
    "Cooking fuel": 1 / 18,
    "Sanitation": 1 / 18,
    "Drinking water": 1 / 18,
    "Electricity": 1 / 18,
    "Housing": 1 / 18,
    "Assets": 1 / 18,
}

# Relative tolerance for float comparisons (tables are saved as float32).
TOLERANCE = 1e-4


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("multidimensional_poverty_index")

    # Read table from meadow dataset.
    tb_hot = ds_meadow.read("hot", safe_types=False)
    tb_cme = ds_meadow.read("cme", safe_types=False)

    #
    # Process data.
    #
    # Harmonize country names.
    tb_hot = paths.regions.harmonize_names(tb=tb_hot, warn_on_unused_countries=False)
    tb_cme = paths.regions.harmonize_names(tb=tb_cme, warn_on_unused_countries=False)

    # Make tables wide (one column per measure) and combine both flavors.
    tb = make_tables_wide_and_merge(tb_cme=tb_cme, tb_hot=tb_hot)

    # Check the output before formatting.
    sanity_check_outputs(tb)

    tb = tb.format(keys=INDEX_COLS, short_name="multidimensional_poverty_index")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_tables_wide_and_merge(tb_cme: Table, tb_hot: Table) -> Table:
    """
    Make tables wide to separate indicators, rename categories and merge hot and cme tables
    """

    tb_cme = select_categories_and_rename(tb_cme)
    tb_hot = select_categories_and_rename(tb_hot)

    # Make tables wide
    tb_hot = tb_hot.pivot(
        index=["country", "year", "indicator", "area"],
        columns=["measure"],
        values="b",
        join_column_levels_with="_",
    ).reset_index(drop=True)

    tb_cme = tb_cme.pivot(
        index=["country", "year", "indicator", "area"],
        columns=["measure"],
        values="b",
        join_column_levels_with="_",
    ).reset_index(drop=True)

    # Add a flavor column to each table
    tb_cme["flavor"] = "Current margin estimate"
    tb_hot["flavor"] = "Harmonized over time"

    # Concatenate the two tables
    tb = pr.concat([tb_cme, tb_hot], ignore_index=True)

    return tb


def select_categories_and_rename(tb: Table) -> Table:
    """
    Select categories to keep and rename them
    """

    for col, categories in CATEGORIES_TO_KEEP.items():
        # Assert that all categories are in the column
        assert set(categories).issubset(set(tb[col].unique())), (
            f"Categories {set(categories) - set(tb[col].unique())} not in column {col}"
        )

        # Filter categories
        tb = tb[tb[col].isin(categories)].reset_index(drop=True)

    # Rename measure categories
    tb["measure"] = tb["measure"].cat.rename_categories(MEASURE_NAMES)

    # Check that the column ind_lab contains all INDICATOR_NAMES
    indicators_excluding_nan = tb[tb["ind_lab"].notna()]["ind_lab"].unique()
    assert set(indicators_excluding_nan) == set(INDICATOR_NAMES), (
        f"Column ind_lab is not identical to the expected list. These are the differences: {set(INDICATOR_NAMES) - set(indicators_excluding_nan)}"
    )

    # Remove indicator and area columns
    tb = tb.drop(columns=["indicator"])

    # Rename ind_lab as indicator and area_lab as area
    tb = tb.rename(columns={"ind_lab": "indicator", "area_lab": "area"})

    return tb


def sanity_check_outputs(tb: Table) -> None:
    """Check coverage, value ranges and the identities that link the MPI measures."""
    # Coverage.
    assert set(tb["area"].unique()) == EXPECTED_AREAS, f"Unexpected areas: {set(tb['area'].unique()) ^ EXPECTED_AREAS}"
    for flavor, n_min in MIN_COUNTRIES.items():
        n = tb.loc[tb["flavor"] == flavor, "country"].nunique()
        assert n >= n_min, f"{flavor}: only {n} countries, expected at least {n_min}."

    # Shares are percentages, and the MPI is between 0 and 1.
    share_cols = ["headcount_ratio", "severe", "vulnerable", "censored_headcount_ratio", "uncensored_headcount_ratio"]
    for col in share_cols + ["intensity"]:
        assert tb[col].dropna().between(0, 100).all(), f"{col} has values outside [0, 100]."
    assert tb["mpi"].dropna().between(0, 1).all(), "mpi has values outside [0, 1]."

    # The poor are deprived in at least a third of the weighted indicators.
    assert (tb["intensity"].dropna() >= 100 / 3 - TOLERANCE).all(), "intensity below 33.3%."

    # Aggregate rows (no indicator) carry the headline measures, and indicator rows the headcount ratios by indicator.
    agg = tb[tb["indicator"].isna()]
    ind = tb[tb["indicator"].notna()]
    assert set(ind["indicator"].unique()) == set(INDICATOR_WEIGHTS), "Unexpected indicators."

    # MPI = H x A.
    diff = (agg["mpi"] - agg["headcount_ratio"] * agg["intensity"] / 10000).abs().dropna()
    assert (diff < TOLERANCE).all(), f"MPI differs from H x A by up to {diff.max()}."

    # Severe poverty is a subset of poverty, and censored headcounts are a subset of uncensored ones.
    assert (agg["severe"] <= agg["headcount_ratio"] + TOLERANCE).all(), "Severe poverty above the headcount ratio."
    assert (ind["censored_headcount_ratio"] <= ind["uncensored_headcount_ratio"] + TOLERANCE).all(), (
        "Censored headcount ratio above the uncensored one."
    )

    # The weighted sum of censored headcount ratios equals the MPI, where all ten indicators are available.
    weighted = ind["censored_headcount_ratio"] * ind["indicator"].map(INDICATOR_WEIGHTS) / 100
    keys = ["country", "year", "area", "flavor"]
    tb_sum = (
        weighted.groupby([ind[k] for k in keys], observed=True).sum(min_count=len(INDICATOR_WEIGHTS)).rename("mpi_sum")
    )
    tb_check = pr.merge(tb_sum.reset_index(), agg[keys + ["mpi"]], on=keys, how="inner").dropna()
    diff = (tb_check["mpi_sum"] - tb_check["mpi"]).abs()
    assert (diff < TOLERANCE).all(), f"Weighted censored headcount ratios differ from the MPI by up to {diff.max()}."
