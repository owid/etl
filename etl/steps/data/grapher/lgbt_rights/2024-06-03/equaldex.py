"""Load a garden dataset and create a grapher dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("equaldex")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from garden dataset.
    tb_garden = ds_garden["equaldex"].reset_index()

    countries_europe = geo.list_members_of_region("Europe", ds_regions)

    tb_garden["country"] = tb_garden["country"].astype("string")
    tb_garden = geo.add_population_to_table(tb_garden, ds_population)

    tb_garden = tb_garden.loc[tb_garden.year == 2024, ["country", "year", "ei_legal", "population"]]
    tb_garden = tb_garden[tb_garden.country.isin(countries_europe)]
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_garden], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
