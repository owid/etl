from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    ds_garden = paths.load_dataset("population_density")

    tb = ds_garden["population_density"]

    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.save()
