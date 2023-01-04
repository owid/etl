"""Load garden dataset for Farmer & Lafond (2016) data and create a grapher dataset.

"""
from owid import catalog

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    ds_garden: catalog.Dataset = paths.load_dependency("farmer_lafond_2016")
    tb_garden = ds_garden["farmer_lafond_2016"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)

    # Add table to dataset and save dataset.
    dataset.add(tb_garden)
    dataset.save()
