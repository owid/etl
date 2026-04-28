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
    # The following columns were initially used to extract sources and urls, and to then add them to the source
    # metadata. However, in the end, there were too many fixes, and we manually corrected them in the snapshot metadata.
    # Afterwards, they will be removed.
    # "available_at": "available_at",
    # "source": "source",
    # "click_placements" : "click_placements",
    # "number_of_records": "number_of_records",
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
    # Check that the percentages of the different laying hens housings add up to 100%.
    # Note: The share of brown hens is not related to all other shares about housing systems.
    assert (
        tb[
            [
                "share_of_hens_free_range_not_organic",
                "share_of_hens_free_range_organic",
                "share_of_hens_in_barns",
                "share_of_hens_in_cages",
                "share_of_hens_in_unknown_housing",
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
