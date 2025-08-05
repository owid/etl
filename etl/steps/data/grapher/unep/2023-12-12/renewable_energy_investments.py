"""Load renewable energy investments data from garden and create a grapher dataset."""

from etl.helpers import PathFinder

# Load paths and naming conventions.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load dataset from garden and read its main table.
    ds_garden = paths.load_dataset("renewable_energy_investments")
    tb = ds_garden.read("renewable_energy_investments", reset_index=False)

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
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()
