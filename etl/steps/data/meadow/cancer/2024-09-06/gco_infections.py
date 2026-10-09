"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns from the source that are redundant with the ones we keep (counts and crude rates for the site totals).
COLUMNS_TO_DROP = ["id", "code", "ncases_sites", "ncases_all", "ir_att", "ir", "asr"]
SEX_MAPPING = {"0": "both", "1": "males", "2": "females"}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gco_infections.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    tb = tb.drop(columns=COLUMNS_TO_DROP)
    tb["sex"] = tb["sex"].astype(str).replace(SEX_MAPPING)

    # Improve table format.
    tb = tb.format(["country", "year", "sex", "agent", "cancer"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
