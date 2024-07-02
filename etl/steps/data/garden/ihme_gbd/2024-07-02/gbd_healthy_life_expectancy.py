from etl.helpers import PathFinder, create_dataset

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load in the cause of death data and hierarchy of causes data
    ds_hale = paths.load_dataset("gbd_healthy_life_expectancy")
    ds_le = paths.load_dataset("gbd_life_expectancy")

    tb_hale = ds_hale["gbd_hale"].reset_index()
    tb_le = ds_le["gbd_life_expectancy"].reset_index()

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_hale, tb_le],
        check_variables_metadata=True,
        default_metadata=ds_hale.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
