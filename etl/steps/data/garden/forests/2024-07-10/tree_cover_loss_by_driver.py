"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# List of regions to be used in the dataset - continents, world and income groups
REGIONS = [
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("tree_cover_loss_by_driver")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    ds_income_groups = paths.load_dataset("income_groups")
    tb = ds_meadow["tree_cover_loss_by_driver"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Some regions are broken down into smaller regions in the dataset, so we need to aggregate them here e.g. Alaska and Hawaii are recorded separately in the dataset, but the geo.harmonize_countries function renames them as United States
    tb = tb.groupby(["country", "year", "category"], observed=True).sum().reset_index()
    tb = tb.pivot(index=["country", "year"], columns="category", values="area").reset_index()
    tb = geo.add_regions_to_table(
        tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups, regions=REGIONS, min_num_values_per_year=1
    )
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
