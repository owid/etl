from etl.helpers import PathFinder

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load table from garden dataset.
    ds_garden = paths.load_dataset("historical_energy_costs")
    tb = ds_garden.read("historical_energy_costs")

    #
    # Process data.
    #
    # Adapt data to grapher format.
    tb["country"] = "World"

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    dataset = paths.create_dataset(tables=[tb])
    dataset.save()
