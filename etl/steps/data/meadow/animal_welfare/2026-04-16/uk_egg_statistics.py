"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from data, and how to rename them.
COLUMNS = {
    "Year": "year",
    "Enriched": "enriched",
    "Barn": "barn",
    "Free range": "free_range",
    "Organic": "organic",
    "Total shell eggs": "total",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("uk_egg_statistics.ods")

    # Load data from snapshot.
    tb = snap.read_excel(sheet_name="Intake_Annual", skiprows=2)

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Replace markers for unavailable ("[x]") and suppressed ("[c]") data with NaN.
    tb = tb.replace({"[x]": None, "[c]": None})

    # Improve table format.
    tb = tb.format(keys=["year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
