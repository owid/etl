from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    ds_garden = paths.load_dataset("homicide_long_run_omm")

    tb = ds_garden["homicide_long_run_omm"]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
