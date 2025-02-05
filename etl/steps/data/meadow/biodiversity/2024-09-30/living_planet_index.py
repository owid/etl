"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("living_planet_index.xlsx")
    sheet_names = [
        "Global",
        "Freshwater",
        "Africa",
        "AsiaPacific",
        "NorthAmerica",
        "LatinAmerica&Carib",
        "EuropeCentralAsia",
    ]
    all_tbs = Table()
    # Load data from snapshot.
    for sheet_name in sheet_names:
        tb = snap.read(safe_types=False, sheet_name=sheet_name)
        tb = tb[
            [
                "Unnamed: 0",
                "LPI_final",
                "CI_low",
                "CI_high",
            ]
        ]
        tb["country"] = sheet_name
        tb = tb.rename(columns={"Unnamed: 0": "year"})
        all_tbs = pr.concat([all_tbs, tb])
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    all_tbs = all_tbs.format(["country", "year"], short_name="living_planet_index")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[all_tbs], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
