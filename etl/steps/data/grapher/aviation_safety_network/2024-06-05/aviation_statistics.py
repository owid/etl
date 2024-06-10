from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("aviation_statistics")
    tb = ds_garden["aviation_statistics"]

    #
    # Save outputs.
    #
    # Create new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
