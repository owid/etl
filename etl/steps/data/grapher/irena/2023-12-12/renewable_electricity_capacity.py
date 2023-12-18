from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load dataset from Garden and read its main table.
    ds_garden = paths.load_dataset("renewable_electricity_capacity")
    tb = ds_garden["renewable_electricity_capacity"]

    #
    # Save outputs.
    #
    # Create new dataset.
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
