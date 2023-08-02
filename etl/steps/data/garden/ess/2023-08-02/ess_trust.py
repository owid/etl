"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_countries_regions() -> Table:
    """Load countries-regions table from reference dataset (e.g. to map from iso codes to country names)."""
    ds_reference = cast(Dataset, paths.load_dependency("regions"))
    tb_countries_regions = ds_reference["regions"]

    return tb_countries_regions


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("ess_trust"))

    # Read table from meadow dataset.
    tb = ds_meadow["ess_trust"]

    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
