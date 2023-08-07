"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("afrobarometer_trust"))

    # Read table from meadow dataset.
    tb = ds_meadow["afrobarometer_trust"].reset_index()

    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    #
    # Save outputs.
    # Set index, verify integrity and sort.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
