"""Load a meadow dataset and create a garden dataset."""
from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.data_helpers.geo import add_population_to_table, list_members_of_region
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["Africa", "North America", "South America", "Asia", "Europe", "Oceania"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gram_level")
    # Add population dataset
    ds_population = paths.load_dataset("population")
    # Add regions dataset
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow["gram_level"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Add population to the table
    tb = add_population_to_table(tb, ds_population)
    # Calculate total DDDs
    tb = add_regional_totals(tb, ds_regions)
    tb = tb.format(["country", "year", "atc_level_3_class"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regional_totals(tb: Table, ds_regions: Dataset) -> Table:
    """Add regional totals to the table."""
    # First back-calculate the total DDDs
    tb["antibiotics_ddd"] = (tb["antibiotic_consumption__ddd_1_000_day"] / 1000) * tb["population"]
    # Then calculate the regional totals
    for region in REGIONS:
        countries = list_members_of_region(region=region, ds_regions=ds_regions)
        tb_region = tb.loc[tb["country"].isin(countries)]
        tb_region = (
            tb_region.groupby(["year", "atc_level_3_class"])[["population", "antibiotics_ddd"]].sum().reset_index()
        )
        tb_region["antibiotic_consumption__ddd_1_000_day"] = (
            tb_region["antibiotics_ddd"] / tb_region["population"] * 1000
        )
        tb_region["country"] = region

        tb = pr.concat([tb, tb_region])
    tb = tb.drop(
        columns=[
            "population",
            "antibiotics_ddd",
        ]
    )
    return tb
