"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("igme.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive("UN IGME 2024.csv", low_memory=False, safe_types=False)
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.rename(columns={"Geographic area": "country", "Reference Date": "year"}, errors="raise")
    # There are some duplicated values in the UNICEF regions - so let's remove those. I wrote to UN IGME to report this 2024-09-12.
    tb = tb[tb["Regional group"] != "UNICEF"]
    # Only grab the UN IGME estimates (not the input raw data)
    tb = tb[tb["Observation Status"] == "Normal value"].reset_index(drop=True)
    tb = tb.format(
        [
            "country",
            "year",
            "indicator",
            "sex",
            "wealth_quintile",
            "series_name",
            "regional_group",
            "unit_of_measure",
        ]
    )
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata, repack=False
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
