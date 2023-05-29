"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("mueller_et_al_2012.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("mueller_et_al_2012"))

    # Read table from meadow dataset.
    tb = ds_meadow["mueller_et_al_2012"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, warn_on_missing_countries=True, warn_on_unused_countries=True
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Add variable metadata.
    for column in tb.columns:
        item = column.split("_")[0]
        tb[column].metadata.title = f"Attainable yield for {item}"
        tb[column].metadata.units = "tonnes per hectare"
        tb[column].metadata.units = "t/ha"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("mueller_et_al_2012.end")
