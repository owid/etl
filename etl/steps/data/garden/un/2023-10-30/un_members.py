"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define latest year for the dataset (the year of the last 31 December).
LATEST_YEAR = 2022


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_members")

    # Read table from meadow dataset.
    tb = ds_meadow["un_members"].reset_index()

    #
    # Process data.
    tb = data_processing(tb)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add missing countries
    tb = add_missing_countries(tb)

    # Add regional aggregations
    tb = regional_aggregations(tb)

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


def data_processing(tb: Table) -> Table:
    """
    Create membership_status column from admission year.
    """

    # Create a year column by creating a range between the year of admission and LATEST_YEAR.
    tb["year"] = tb["admission"].astype(int).apply(lambda x: range(tb["admission"].min(), LATEST_YEAR + 1))

    # Explode the year column.
    tb = tb.explode("year")

    # Create membership_status column, which is "Member" when year is greater or equal to admission year, and "Not a member" otherwise.
    # I copy the admission column first to keep metadata

    tb["membership_status"] = tb["admission"].copy()
    tb.loc[tb["year"] < tb["admission"], "membership_status"] = "Not a member"
    tb.loc[tb["year"] >= tb["admission"], "membership_status"] = "Member"

    # Drop admission column.
    tb = tb.drop(columns=["admission"])

    return tb


def add_missing_countries(tb: Table) -> Table:
    """
    Add countries not available in the list of UN members
    """

    # Load region dataset
    tb_regions = paths.load_dependency("regions", channel="grapher")
    tb_regions = tb_regions["regions"].reset_index()  # type: ignore
    tb_regions = tb_regions[
        (tb_regions["region_type"] == "country") & ~(tb_regions["is_historical"]) & (tb_regions["is_mappable"])
    ]

    countries_available = tb["country"].unique().tolist()
    countries_regions_dataset = tb_regions["name"].unique().tolist()

    # Obtain the list of countries in countries_regions_dataset that are not in countries_available
    countries_to_add = list(set(countries_regions_dataset) - set(countries_available))

    paths.log.info(
        f"""Adding these countries to the dataset (non-members):
                   {countries_to_add}"""
    )

    # Create a dataframe with the countries to add and years between tb["year"].min() and LATEST_YEAR
    tb_countries = Table({"country": countries_to_add})
    tb_years = Table({"year": range(tb["year"].min(), LATEST_YEAR + 1)})

    # Create a dataframe with the cartesian product of tb_countries and tb_years
    tb_countries_to_add = pr.merge(tb_countries, tb_years, how="cross")

    # All these countries are not members
    tb_countries_to_add["membership_status"] = "Not a member"

    # Concatenate tb and tb_countries_to_add
    tb = pr.concat([tb, tb_countries_to_add], ignore_index=True)

    return tb


def regional_aggregations(tb: Table) -> Table:
    """
    Add regional aggregations refered to the number of members and non-member, and also the population living in those regions.
    """

    tb_regions = tb.copy()

    # Load population data.
    tb_pop = paths.load_dataset("population")
    tb_pop = tb_pop["population"].reset_index()

    # Merge population data.
    tb_regions = tb_regions.merge(tb_pop[["country", "year", "population"]], how="left", on=["country", "year"])

    # Replace "Member" by 1 and "Not a member" by 0.
    tb_regions["membership_number"] = tb_regions["membership_status"].replace({"Member": 1, "Not a member": 0})

    # Create non_membership_number column, which is 1 when membership_status is "Not a member", and 0 otherwise.
    tb_regions["non_membership_number"] = 1 - tb_regions["membership_number"]

    tb_regions["membership_pop"] = tb_regions["membership_number"] * tb_regions["population"]
    tb_regions["non_membership_pop"] = tb_regions["non_membership_number"] * tb_regions["population"]

    # Define regions to aggregate
    regions = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "European Union (27)",
        "World",
    ]

    # Define group of variables to generate
    var_list = [
        "membership_number",
        "membership_pop",
        "non_membership_number",
        "non_membership_pop",
    ]

    # Define the variables and aggregation method to be used in the following function loop
    aggregations = dict.fromkeys(
        var_list + ["population"],
        "sum",
    )

    # Add regional aggregates, by summing up the variables in `aggregations`
    for region in regions:
        tb_regions = geo.add_region_aggregates(
            tb_regions,
            region=region,
            aggregations=aggregations,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.2,
        )

    # Filter table to keep only regions
    tb_regions = tb_regions[tb_regions["country"].isin(regions)].reset_index(drop=True)

    # Call the population data again to get regional total population
    tb_regions = tb_regions.merge(
        tb_pop[["country", "year", "population"]], how="left", on=["country", "year"], suffixes=("", "_region")
    )

    # Add missing_pop column, population minus membership_pop and non_membership_pop.
    tb_regions["missing_pop"] = (
        tb_regions["population_region"] - tb_regions["membership_pop"] - tb_regions["non_membership_pop"]
    )

    # Assert if missing_pop has negative values
    if tb_regions["missing_pop"].min() < 0:
        paths.log.warning(
            f"""`missing_pop` has negative values and will be replaced by 0.:
            {print(tb_regions[tb_regions["missing_pop"] < 0])}"""
        )
        # Replace negative values by 0
        tb_regions.loc[tb_regions["missing_pop"] < 0, "missing_pop"] = 0

    # Include missing_pop in non_membership_pop
    tb_regions["non_membership_pop"] = tb_regions["non_membership_pop"] + tb_regions["missing_pop"]

    # Drop columns
    tb_regions = tb_regions.drop(columns=["population", "population_region", "missing_pop"])

    # Concatenate tb and tb_regions
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    # Make variables in var_list integer
    for var in var_list:
        tb[var] = tb[var].astype("Int64")

    return tb
