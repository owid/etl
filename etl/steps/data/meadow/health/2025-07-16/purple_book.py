"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REL_COLUMNS = [
    "Applicant",
    "BLA Number",
    "Proprietary Name",
    "Proper Name",
    "BLA Type",
    "Marketing Status",
    "Licensure",
    "Approval Date",
    "Ref. Product Proper Name",
    "Ref. Product Proprietary Name",
    "Submission Type",
    "License Number",
    "Product Number",
    "Center",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("purple_book.csv")

    # Load data from snapshot. Ignore changes (first 34 rows) and go directly to the database.
    tb = snap.read(header=34)

    # Remove products with higher product number but same BLA number as a product with lower product number.
    # Each new drug formulation (e.g., tablet/ capsule, different strength, etc.) is a unique product (which it's own product number), but we care only about the active ingredient (which will be given by the BLA number).
    tb = tb.sort_values("Product Number")
    tb = tb[~tb.duplicated(subset=["BLA Number"], keep="first")]
    tb = tb.sort_values("BLA Number")

    # Use only relevant columns.
    tb = tb[REL_COLUMNS]

    # format date column as datetime
    tb["Approval Date"] = tb["Approval Date"].astype("datetime64[ns]")

    # Improve tables format.
    tables = [tb.format(["BLA Number"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
