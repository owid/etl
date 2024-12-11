"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("igme.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive("UN IGME 2023.csv", low_memory=False, safe_types=False)
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.rename(columns={"Geographic area": "country", "REF_DATE": "year"}, errors="raise")
    # There are some duplicated values in the UNICEF regions - so let's remove those. I wrote to UN IGME to report this 2024-09-12.
    tb = tb[tb["Regional group"] != "UNICEF"]
    tb = tb.format(
        ["country", "year", "indicator", "sex", "wealth_quintile", "series_name", "regional_group", "unit_of_measure"]
    )
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
