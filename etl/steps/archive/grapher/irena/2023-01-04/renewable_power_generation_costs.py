from owid import catalog

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load Garden dataset.
    ds_garden: catalog.Dataset = paths.load_dependency("renewable_power_generation_costs")
    # Get main table from Garden dataset.
    table = ds_garden["renewable_power_generation_costs"]

    # Create new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)

    # Add table to new Grapher dataset and save dataset.
    dataset.add(table)
    dataset.save()
