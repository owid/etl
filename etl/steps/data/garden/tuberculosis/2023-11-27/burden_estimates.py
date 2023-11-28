"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

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
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    # Add region aggregates.
    cols_to_aggregate = tb.columns[tb.columns.str.contains("num")].tolist()
    tb_agg = tb[cols_to_aggregate].reset_index()
    tb_no_agg = tb.drop(columns=cols_to_aggregate).reset_index()
    tb_agg = add_region_sum_aggregates(tb_agg, ds_regions=ds_regions, ds_income_groups=ds_income_groups).copy_metadata(
        tb
    )
    tb_agg = calculate_region_rates(tb_agg, ds_population=ds_population)

    # Combine aggregated and non-aggregated tables.
    tb = pr.merge(tb_agg, tb_no_agg, on=["country", "year"], how="outer", validate="one_to_one", copy=False)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_variable_description_from_producer(tb, dd):
    """Add variable description from the data dictionary to each variable."""
    columns = tb.columns.difference(["country", "year"])
    for col in columns:
        tb[col].metadata.description_from_producer = dd.loc[dd.variable_name == col, "definition"].values[0]
    return tb


def add_region_sum_aggregates(
    tb: Table, ds_regions: Dataset, ds_income_groups: Dataset, ds_population: Dataset
) -> Table:
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


def calculate_region_rates(tb: Table, ds_population: Dataset) -> Table:
    # Add population to table to calculate rate variables
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population)
    tb = tb.drop(columns=["e_pop_num"])

    return tb
