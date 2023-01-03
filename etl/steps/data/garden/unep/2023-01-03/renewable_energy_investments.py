"""Create a garden dataset on renewable energy investments based on UNEP data.

"""

from owid import catalog

from etl.helpers import PathFinder

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow.
    ds_meadow: catalog.Dataset = paths.load_dependency("renewable_energy_investments")
    tb_meadow = ds_meadow["renewable_energy_investments"]

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = catalog.Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Create new table with metadata and add it to the new dataset.
    tb_garden = tb_meadow
    ds_garden.add(tb_garden)

    # Update metadata and save dataset.
    ds_garden.update_metadata(paths.metadata_path)
    ds_garden.save()
