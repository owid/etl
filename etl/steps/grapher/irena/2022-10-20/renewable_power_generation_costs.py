from owid import catalog

from etl.helpers import Names

# Get naming conventions.
N = Names(__file__)


def run(dest_dir: str) -> None:
    # Create new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    # Get main table from Garden dataset.
    table = N.garden_dataset["renewable_power_generation_costs"]
    # Add table to new Grapher dataset and save dataset.
    dataset.add(table)
    dataset.save()
