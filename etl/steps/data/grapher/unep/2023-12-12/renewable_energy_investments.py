"""Load renewable energy investments data from garden and create a grapher dataset.

"""
from etl.helpers import PathFinder, create_dataset

# Load paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from garden and read its main table.
    ds_garden = paths.load_dataset("renewable_energy_investments")
    tb = ds_garden["renewable_energy_investments"]

    #
    # Prepare data.
    #
    # Convert billion dollars to dollars, and adapt metadata units accordingly.
    for column in tb.columns:
        tb[column] *= 1e9
        tb[column].metadata.unit = "US dollars"
        tb[column].metadata.short_unit = "$"

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
