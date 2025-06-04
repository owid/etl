"""Load snapshot and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data from snapshot.
    #
    snap = paths.load_snapshot()
    tb = snap.read(safe_types=False).set_index(["country", "year"])
    # Harmonize country names.
    tb = geo.harmonize_countries(
        tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
