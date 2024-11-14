"""Load a snapshot and create a meadow dataset."""

from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns and their new names
COLUMNS = {"REF_DATE": "year", "GEO": "country", "Income concept": "income_concept", "VALUE": "gini"}

# Define income concepts and their new names
INCOME_CONCEPTS = {
    "Adjusted market income": "market_income",
    "Adjusted total income": "total_income",
    "Adjusted after-tax income": "after_tax_income",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gini_coefficients.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Keep only the columns of interest and rename them.
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS)

    # Map income concepts to new names.
    tb["income_concept"] = map_series(
        series=tb["income_concept"],
        mapping=INCOME_CONCEPTS,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    tb["income_concept"] = tb["income_concept"].copy_metadata(tb["country"])
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "income_concept"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
