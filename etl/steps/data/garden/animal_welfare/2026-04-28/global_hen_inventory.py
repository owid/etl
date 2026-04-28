"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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


def run_sanity_checks_on_outputs(tb: Table) -> None:
    assert all([tb[column].min() >= 0 for column in tb.columns]), "All numbers should be >0"
    assert all([tb[column].max() <= 100 for column in tb.columns if "share" in column]), "Percentages should be <100"
    # Check that the percentages of the different laying hen housings add up to 100%.
    # Note: The share of brown hens is not related to all other shares about housing systems.
    assert (
        tb[
            [
                "share_of_hens_free_range_not_organic",
                "share_of_hens_free_range_organic",
                "share_of_hens_in_barns",
                "share_of_hens_in_cages",
            ]
        ].sum(axis=1)
        < 101
    ).all()


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

    # The WFI compilation marks countries without a real housing breakdown as 100% "unknown
    # housing", with all the granular shares set to 0. The unknown column is therefore an
    # all-or-nothing flag rather than a real category. Verify that pattern, then drop the column.
    # The rows are kept because their absolute counts (e.g. number_of_hens_in_cages) are still
    # meaningful — WFI counts these under cages by default when the housing system is unknown.
    nonzero_unknown = tb[tb["share_of_hens_in_unknown_housing"] > 0]
    assert (nonzero_unknown["share_of_hens_in_unknown_housing"] == 100).all(), (
        "Expected any non-zero share_of_hens_in_unknown_housing to be exactly 100%."
    )
    tb = tb.drop(columns=["share_of_hens_in_unknown_housing"])

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
