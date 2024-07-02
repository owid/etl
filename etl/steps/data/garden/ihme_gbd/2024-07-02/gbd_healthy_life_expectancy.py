from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load in the cause of death data and hierarchy of causes data
    ds_hale = paths.load_dataset("gbd_healthy_life_expectancy")
    ds_le = paths.load_dataset("gbd_life_expectancy")

    tb_hale = ds_hale["gbd_healthy_life_expectancy"].reset_index()
    tb_le = ds_le["gbd_life_expectancy"].reset_index()

    tb_hale = format_data(tb_hale, "healthy_life_expectancy_newborn")
    tb_le = format_data(tb_le, "life_expectancy_newborn")

    tb = Table()
    tb = pr.merge(tb_hale, tb_le, on=["country", "year"], how="outer")
    tb.metadata = tb_hale.metadata

    # calculate years lived with disability/disease
    tb["years_lived_with_disability"] = tb["life_expectancy_newborn"] - tb["healthy_life_expectancy_newborn"]

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.format(["country", "year"])
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_hale.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def format_data(tb: Table, var_name: str) -> Table:
    """
    Formatting the tables so that they can be combined
    """
    assert len(tb["measure_name"].unique()) == 1
    assert len(tb["age_name"].unique()) == 1
    tb = tb[["location_name", "year", "val"]].rename(columns={"val": var_name, "location_name": "country"})

    return tb
