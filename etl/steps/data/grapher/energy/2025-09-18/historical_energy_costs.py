from etl.helpers import PathFinder

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load table from garden dataset.
    ds_garden = paths.load_dataset("historical_energy_costs")
    tb_garden = ds_garden.read("historical_energy_costs", reset_index=False)

    #
    # Process data.
    #
    # Adapt data to grapher format.
    tb_garden["country"] = "World"

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    dataset = paths.create_dataset(tables=[tb_garden])
    dataset.save()
