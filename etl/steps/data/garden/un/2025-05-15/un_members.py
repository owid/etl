"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [
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

# The UN members dataset includes countries that are member states, but not all other countries that are not members.
# If we include all possible countries in our regions dataset, the number of countries increases dramatically (including disputed and overseas territories).
# Instead, we include here only "mappable countries" (those that would appear in a grapher map chart) that are not members.
MISSING_COUNTRIES = [
    "Kosovo",
    "Western Sahara",
    "Palestine",
    "Greenland",
    "New Caledonia",
    "French Southern Territories",
    "French Guiana",
    "Puerto Rico",
    "Taiwan",
]


def prepare_inputs(tb: Table) -> Table:
    """
    Create membership_status column from admission year.
    """
    # Find the latest complete year (since the publication of this dataset).
    # NOTE: By definition, unless this dataset is published in the last day of the year, the latest complete year is the previous one.
    latest_year = int(tb["admission"].metadata.origins[0].date_published[0:4]) - 1

    # Create a year column by creating a range between the year of admission and latest_year.
    tb["year"] = tb["admission"].astype(int).apply(lambda x: range(tb["admission"].min(), latest_year + 1))

    # Explode the year column.
    tb = tb.explode("year")

    # Create membership_status column, which is "Member" when year is greater or equal to admission year, and "Not a member" otherwise.
    # I copy the admission column first to keep metadata

    tb["membership_status"] = tb["admission"].copy().astype(object)
    tb.loc[tb["year"] < tb["admission"], "membership_status"] = "Not a member"
    tb.loc[tb["year"] >= tb["admission"], "membership_status"] = "Member"

    # Drop admission column.
    tb = tb.drop(columns=["admission"])

    return tb


def add_missing_countries(tb: Table) -> Table:
    """
    Add countries not available in the list of UN members
    """
    # Sanity check.
    error = "Some of the countries in MISSING_COUNTRIES is now a member sate. Change that list in the code."
    assert set(MISSING_COUNTRIES) & set(tb["country"]) == set(), error

    # Create a dataframe with the countries to add and years.
    tb_countries_to_add = pr.merge(
        Table({"country": MISSING_COUNTRIES}),
        Table({"year": range(tb["year"].min(), tb["year"].max() + 1)}),
        how="cross",
    ).assign(**{"membership_status": "Not a member"})

    # Concatenate tb and tb_countries_to_add
    tb = pr.concat([tb, tb_countries_to_add], ignore_index=True)

    return tb


def create_region_members_table(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Table:
    """
    Add regional aggregations refered to the number of members and non-member, and also the population living in those regions.
    """
    tb_with_regions = tb.copy()

    # Add population to table (before adding regions).
    tb_with_regions = geo.add_population_to_table(
        tb=tb_with_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    # Add regions (continents and income groups) with the number of members and non-members, as well as the population of members and non-members.
    # To do that, create a column "membership_number" that is 1 for a member, and 0 otherwise.
    # Then non_membership_number is the inverse of membership_number.
    tb_with_regions["membership_number"] = tb_with_regions["membership_status"].replace(
        {"Member": 1, "Not a member": 0}
    )
    tb_with_regions["non_membership_number"] = 1 - tb_with_regions["membership_number"]
    tb_with_regions["membership_pop"] = tb_with_regions["membership_number"] * tb_with_regions["population"]
    tb_with_regions["non_membership_pop"] = tb_with_regions["non_membership_number"] * tb_with_regions["population"]
    tb_with_regions = geo.add_regions_to_table(
        tb=tb_with_regions,
        regions=REGIONS,
        ds_regions=ds_regions,
        aggregations={
            column: "sum"
            for column in [
                "membership_number",
                "membership_pop",
                "non_membership_number",
                "non_membership_pop",
                "population",
            ]
        },
        frac_allowed_nans_per_year=None,
        num_allowed_nans_per_year=None,
        countries_that_must_have_data=[],
    )

    # Keep only regions.
    tb_with_regions = tb_with_regions[tb_with_regions["country"].isin(REGIONS)].reset_index(drop=True)

    # Since we are not considering all countries (we are missing some contested and overseas territories), add now the true total population of the regions.
    tb_with_regions = geo.add_population_to_table(
        tb=tb_with_regions,
        ds_population=ds_population,
        warn_on_missing_countries=False,
        population_col="population_region",
    )

    # Add missing_pop column.
    tb_with_regions["missing_pop"] = (
        tb_with_regions["population_region"] - tb_with_regions["membership_pop"] - tb_with_regions["non_membership_pop"]
    )

    error = "True population of regions is smaller than the sum of the population of member and non-member countries. Check this part of the code."
    assert tb_with_regions["missing_pop"].min() == 0, error

    # Include missing population in as part of the non members population.
    # NOTE: We fillna because some of the population data is missing (e.g. between 1945 and 1950).
    tb_with_regions["non_membership_pop"] += tb_with_regions["missing_pop"].fillna(0)

    # Drop temporary columns.
    tb_with_regions = tb_with_regions.drop(
        columns=["membership_status", "population", "population_region", "missing_pop"]
    )

    # Adjust column types.
    tb_with_regions = tb_with_regions.astype(
        {column: "Int64" for column in tb_with_regions.columns if column != "country"}
    )

    return tb_with_regions


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("un_members")
    tb = ds_meadow.read("un_members")

    # Load region dataset.
    ds_regions = paths.load_dataset("regions")

    # Load population data.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Prepare inputs.
    tb = prepare_inputs(tb=tb)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add missing countries that are not member states.
    tb = add_missing_countries(tb=tb)

    # Add regional aggregations
    tb_regions = create_region_members_table(tb=tb, ds_regions=ds_regions, ds_population=ds_population)

    # Improve table formats.
    tb = tb.format(["country", "year"])
    tb_regions = tb_regions.format(["country", "year"], short_name="un_members_in_regions")

    ####################################################################################################################
    # When combining tables with population, there is a known issue, and population's attribution gets propagated and overrides the true origins of the indicators in grapher.
    # For now, remove that presentation attribution.
    # TODO: Remove this temporary solution once the issues is fixed.
    for column in ["membership_pop", "non_membership_pop"]:
        tb_regions[column].metadata.presentation.attribution = None
    ####################################################################################################################

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_regions], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
