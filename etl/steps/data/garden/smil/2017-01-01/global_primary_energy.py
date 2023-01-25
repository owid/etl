from owid.catalog import Dataset
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("global_primary_energy.start")

    # Load dataset from meadow.
    ds_meadow = N.meadow_dataset
    tb_meadow = ds_meadow["global_primary_energy"]

    # Create new garden dataset.
    ds_garden = Dataset.create_empty(dest_dir, ds_meadow.metadata)

    # Copy all metadata from meadow, including variable metadata.
    tb_garden = underscore_table(tb_meadow)
    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata
    # Sort data conveniently.
    tb_garden = tb_garden[sorted(tb_garden.columns)].sort_index()

    # Update metadata using yaml file.
    ####################################################################################################################
    # Temporary solution: At the moment, 'published_by' cannot be added to walden metadata.
    # I could add a new source to the yaml file in this step (with the appropriate 'published_by') and use
    # > ds_garden.metadata.update_from_yaml(N.metadata_path)
    # but this would keep the original source (without 'published_by') and add a new one (with 'published_by').
    # Therefore, for the moment the only solution I see is to manually input the 'published_by' field here.
    # Alternatively, I could ignore the metadata from walden and add all the relevant metadata in this step's yaml file.
    ds_garden.metadata.sources[
        0
    ].published_by = "Vaclav Smil (2017), Energy Transitions: Global and National Perspectives, 2nd edition, Appendix A"
    ####################################################################################################################
    tb_garden.update_metadata_from_yaml(N.metadata_path, "global_primary_energy")

    # Add table to dataset.
    ds_garden.add(tb_garden)

    # Save dataset.
    ds_garden.save()

    log.info("global_primary_energy.end")
