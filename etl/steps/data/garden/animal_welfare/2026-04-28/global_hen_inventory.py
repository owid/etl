"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

HOUSING_SHARE_COLUMNS = [
    "share_of_hens_free_range_not_organic",
    "share_of_hens_free_range_organic",
    "share_of_hens_in_barns",
    "share_of_hens_in_cages",
]

# Columns to select from data, and how to rename them.
COLUMNS = {
    "country": "country",
    "year": "year",
    "barn": "share_of_hens_in_barns",
    "brown__pct": "share_of_brown_hens",
    "cage": "share_of_hens_in_cages",
    "cage_free": "number_of_hens_cage_free",
    "cages": "number_of_hens_in_cages",
    "commercial_egg_farms": "number_of_commercial_egg_farms",
    "free_range": "share_of_hens_free_range_not_organic",
    "organic": "share_of_hens_free_range_organic",
    "total_layers": "number_of_laying_hens",
    "unknown": "share_of_hens_in_unknown_housing",
}


def clean_values(tb: Table) -> Table:
    tb = tb.copy()
    # Remove the spurious "%" symbols from some of the values in some columns.
    for column in tb.columns:
        if tb[column].astype(str).str.contains("%").any():
            tb[column] = tb[column].str.replace("%", "").astype(float)

    return tb


def fix_unknown_housing_systems(tb: Table) -> Table:
    tb = tb.copy()
    # The WFI compilation marks countries without a real housing breakdown as 100% "unknown
    # housing". Keep their total laying-hen counts, but do not assign them to any housing system.
    unknown_housing = tb["share_of_hens_in_unknown_housing"] > 0
    nonzero_unknown = tb[unknown_housing]
    assert (nonzero_unknown["share_of_hens_in_unknown_housing"] == 100).all(), (
        "Expected any non-zero share_of_hens_in_unknown_housing to be exactly 100%."
    )
    housing_columns = [
        "share_of_hens_in_barns",
        "share_of_hens_in_cages",
        "number_of_hens_cage_free",
        "number_of_hens_in_cages",
        "share_of_hens_free_range_not_organic",
        "share_of_hens_free_range_organic",
    ]
    tb.loc[unknown_housing, housing_columns] = None
    tb = tb.drop(columns=["share_of_hens_in_unknown_housing"])

    return tb


def run_sanity_checks_on_outputs(tb: Table) -> None:
    assert all([tb[column].min() >= 0 for column in tb.columns]), "All numbers should be >0"
    assert all([tb[column].max() <= 100 for column in tb.columns if "share" in column]), "Percentages should be <100"
    # Check that the percentages of the different laying-hen housing systems add up to 100%.
    # Note: The share of brown hens is not related to all other shares about housing systems.
    housing_share_totals = tb[HOUSING_SHARE_COLUMNS].sum(axis=1, min_count=1).dropna()
    assert housing_share_totals.between(99.5, 100.5).all(), (
        "Housing-system shares should add up to 100%, allowing for rounding."
    )


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("global_hen_inventory")
    tb = ds_meadow.read("global_hen_inventory")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Clean data (remove spurious "%" in the data).
    tb = clean_values(tb=tb)

    # Leave housing-system fields blank where the source marks the housing system as unknown.
    tb = fix_unknown_housing_systems(tb=tb)

    # Improve table format.
    tb = tb.format()

    # Run sanity checks on outputs.
    run_sanity_checks_on_outputs(tb=tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save the new garden dataset.
    ds_garden.save()
