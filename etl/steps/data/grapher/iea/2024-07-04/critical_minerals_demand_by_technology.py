"""Load a garden dataset and create a grapher dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("critical_minerals")
    tb_demand_by_technology_flat = ds_garden.read("demand_by_technology")

    #
    # Process data.
    #
    # Adapt table to grapher.
    tb_demand_by_technology_flat = tb_demand_by_technology_flat.rename(columns={"technology": "country"})

    # Improve format.
    tb_demand_by_technology_flat = tb_demand_by_technology_flat.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_demand_by_technology_flat], check_variables_metadata=True)
    ds_grapher.save()
