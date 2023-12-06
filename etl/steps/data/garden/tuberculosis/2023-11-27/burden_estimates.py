"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr
from shared import add_variable_description_from_producer

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions to create aggregates for.
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
    ds_meadow = paths.load_dataset("burden_estimates")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Load regions dataset.
    ds_regions = paths.load_dependency("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dependency("income_groups")
    # Load population dataset.
    ds_population = paths.load_dependency("population")

    # Load data dictionary from snapshot.
    dd = snap.read()
    # Read table from meadow dataset.
    tb = ds_meadow["burden_estimates"].reset_index()
    tb = tb.drop(columns=["iso2", "iso3", "iso_numeric", "g_whoregion"])
    #
    # Process data.
    #
    tb = add_variable_description_from_producer(tb, dd)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.drop(columns="e_pop_num")

    # Add region aggregates.
    cols_to_aggregate = tb.columns[tb.columns.str.contains("num")].tolist()
    cols_to_aggregate = ["country", "year"] + cols_to_aggregate
    tb_agg = tb[cols_to_aggregate]
    tb_agg = add_region_sum_aggregates(tb_agg, ds_regions=ds_regions, ds_income_groups=ds_income_groups).copy_metadata(
        tb
    )
    tb_agg = calculate_region_rates(tb_agg, ds_population=ds_population)

    # Combine aggregated and original country-level tables.
    tb = pr.concat([tb, tb_agg], axis=0, ignore_index=True, copy=False)
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
            countries_that_must_have_data=[],
            frac_allowed_nans_per_year=0.5,
            num_allowed_nans_per_year=None,
        )

    tb = tb[tb["country"].isin(REGIONS_TO_ADD)]

    return tb


def calculate_region_rates(tb: Table, ds_population: Dataset) -> Table:
    # Add population to table to calculate rate variables
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population)

    cols = tb.columns.difference(["country", "year", "population"])
    rate_cols = cols.str.replace("num", "100k", regex=True)

    for col, rate_col in zip(cols, rate_cols):
        tb[rate_col] = tb[col] / tb["population"] * 100000  # per 100k

    tb = tb.drop(columns=["population"])

    return tb
