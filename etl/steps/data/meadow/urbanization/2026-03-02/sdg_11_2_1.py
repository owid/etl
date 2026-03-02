"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("sdg_11_2_1.xlsx")

    # Load data from snapshot.
    tb = snap.read_excel()

    #
    # Process data.
    #
    # Standardize column names
    tb.columns = [
        "sdg_goal",
        "sdg_target",
        "sdg_indicator",
        "country_code",
        "country",
        "region",
        "subregion",
        "city_code",
        "city",
        "public_transport_access",
        "units",
        "year",
        "source",
        "footnote",
    ]

    # Set appropriate data types
    tb["year"] = tb["year"].astype(int)
    tb["country"] = tb["country"].astype(str)
    tb["city"] = tb["city"].astype(str)

    # Ensure all columns in the list are snake-case, lower-case and with no spaces.
    tb = tb.format(["country", "city", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
