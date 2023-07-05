"""Load renewable energy investments data from garden and create a grapher dataset.

"""

from owid import catalog

from etl.helpers import PathFinder

# Convert billion dollars to dollars.
BILLION_DOLLARS_TO_DOLLARS = 1e9

# Load paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from garden.
    table = paths.garden_dataset["renewable_energy_investments"]

    #
    # Prepare data.
    #
    # Convert billion dollars to dollars, and adapt metadata units accordingly.
    for column in table.columns:
        old_title = table[column].metadata.title
        old_description = table[column].metadata.description
        table[column] *= BILLION_DOLLARS_TO_DOLLARS
        table[column].metadata.title = old_title
        table[column].metadata.description = old_description
        table[column].metadata.unit = "US dollars"
        table[column].metadata.short_unit = "$"

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the metadata from garden.
    dataset = catalog.Dataset.create_empty(dest_dir, paths.garden_dataset.metadata)

    # Add new table to dataset and save dataset.
    dataset.add(table)
    dataset.save()
