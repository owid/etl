"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["Asia", "Europe", "Africa", "Oceania", "North America", "South America"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("tree_cover_loss_by_driver")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow["tree_cover_loss_by_driver"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Some regions are broken down into smaller regions in the dataset, so we need to aggregate them here e.g. Alaska and Hawaii are recorded separately in the dataset, but the geo.harmonize_countries function renames them as United States
    tb = tb.groupby(["country", "year", "category"]).sum().reset_index()
    tb["year"] = tb["year"].astype(int) + 2000
    # Convert m2 to ha
    tb["area"] = tb["area"].astype(float).div(10000)
    tb = convert_codes_to_drivers(tb)

    tb = tb.pivot(index=["country", "year"], columns="category", values="area").reset_index()
    tb = geo.add_regions_to_table(tb, ds_regions=ds_regions, regions=REGIONS, min_num_values_per_year=1)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def convert_codes_to_drivers(tb: Table) -> Table:
    """ """
    code_dict = {
        1: "commodity_driven_deforestation",
        2: "shifting_agriculture",
        3: "forestry",
        4: "wildfire",
        5: "urbanization",
    }
    tb["category"] = tb["category"].replace(code_dict)
    return tb