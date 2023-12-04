"""Load a meadow dataset and create a garden dataset."""
from owid.catalog import Dataset, Table
from shared import add_variable_description_from_producer

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
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
    ds_meadow = paths.load_dataset("drug_resistance_surveillance")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Load data dictionary from snapshot.
    dd = snap.read()
    # Load regions dataset.
    ds_regions = paths.load_dependency("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dependency("income_groups")
    # Read table from meadow dataset.
    tb = ds_meadow["drug_resistance_surveillance"].reset_index()
    #
    # Process data.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = add_region_sum_aggregates(tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    tb = add_variable_description_from_producer(tb, dd)

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_region_sum_aggregates(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb = tb.copy()
    for region in REGIONS_TO_ADD:
        # List of countries in region.
        countries_in_region = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
        )

        # Add region aggregates.
        tb = geo.add_region_aggregates(
            df=tb,
            region=region,
            countries_in_region=countries_in_region,
            frac_allowed_nans_per_year=0.5,
            num_allowed_nans_per_year=None,
        )

    return tb
