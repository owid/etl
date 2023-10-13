"""Create a dummy dataset with indicators that have very different metadata situations.

NOTE: To play around with this dataset, you don't need to edit this file. Instead, simply edit the adjacent yaml file.

"""

import yaml
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Create a dummy data table.
    tb = Table({"country": ["France", "Portugal", "Portugal"], "year": [2022, 2022, 2023]}, short_name=paths.short_name)

    # Read the list of indicators defined in the adjacent yaml file.
    with open(paths.metadata_path) as istream:
        metadata = yaml.safe_load(istream)
    indicators = list(metadata["tables"]["dummy_monster"]["variables"])

    # Add some dummy data to each indicator.
    for indicator in indicators:
        tb[indicator] = [1, 2, 3]

    # Set an appropriate index.
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
