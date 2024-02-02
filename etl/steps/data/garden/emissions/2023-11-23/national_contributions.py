"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table, Variable
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum year to consider.
# There is data from 1830 for some variables and from 1850 for others.
# However, when inspecting data between 1830 and 1850 (e.g. total CO2 emissions) it seems there is an abrupt jump
# between 1849 and 1850, which happens for many countries.
# This jump seems to be spurious, and therefore we start all time series from 1850.
YEAR_MIN = 1850

# Conversion factor to change from teragrams to tonnes.
TERAGRAMS_TO_TONNES = 1e6
# Conversion factor to change from petagrams to tonnes.
PETAGRAMS_TO_TONNES = 1e9

# Conversion factors to change from tonnes of gases emitted to tonnes of CO2 equivalents (taken from IPCC AR6).
CH4_FOSSIL_EMISSIONS_TO_CO2_EQUIVALENTS = 29.8
CH4_LAND_EMISSIONS_TO_CO2_EQUIVALENTS = 27.2
N2O_EMISSIONS_TO_CO2_EQUIVALENTS = 273

# Gases and components expected to be in the data, and how to rename them.
GASES_RENAMING = {
    "3-GHG": "ghg",
    "CH[4]": "ch4",
    "CO[2]": "co2",
    "N[2]*O": "n2o",
}
COMPONENTS_RENAMING = {
    "Fossil": "fossil",
    "LULUCF": "land",
    "Total": "total",
}

# Columns for which we will create "share" variables, e.g. the percentage of methane emissions that a country produces
# in a year with respect to the world's methane emissions on the same year.
# NOTE: For this calculation, it doesn't matter if we use the total or the CO2-equivalent emissions.
SHARE_VARIABLES = [
    "annual_emissions_ch4_total",
    "annual_emissions_co2_total",
    "annual_emissions_n2o_total",
    "annual_emissions_ghg_total_co2eq",
    "temperature_response_ghg_total",
]

# Columns for which a per-capita variable will be created.
PER_CAPITA_VARIABLES = [
    "annual_emissions_ch4_total_co2eq",
    "annual_emissions_co2_total",
    "annual_emissions_n2o_total_co2eq",
    "annual_emissions_ghg_total_co2eq",
]

# Regions to be added by aggregating data from their member countries.
REGIONS = {
    # Default continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
    # Additional composite regions.
    "Asia (excl. China and India)": {
        "additional_regions": ["Asia"],
        "excluded_members": ["China", "India"],
    },
    "Europe (excl. EU-27)": {"additional_regions": ["Europe"], "excluded_regions": ["European Union (27)"]},
    "Europe (excl. EU-28)": {
        "additional_regions": ["Europe"],
        "excluded_regions": ["European Union (27)"],
        "excluded_members": ["United Kingdom"],
    },
    "European Union (28)": {
        "additional_regions": ["European Union (27)"],
        "additional_members": ["United Kingdom"],
    },
    "North America (excl. USA)": {
        "additional_regions": ["North America"],
        "excluded_members": ["United States"],
    },
    # EU27 is already included in the original data.
    # "European Union (27)": {},
}


def run_sanity_checks_on_inputs(tb):
    # Sanity checks.
    error = "Names of gases have changed."
    assert set(tb["gas"]) == set(GASES_RENAMING), error
    error = "Names of components have changed."
    assert set(tb["component"]) == set(COMPONENTS_RENAMING), error
    error = "Units have changed."
    assert set(tb["unit"]) == set(
        ["Tg~CH[4]~year^-1", "Pg~CO[2]~year^-1", "Tg~N[2]*O~year^-1", "Pg~CO[2]*-e[100]", "°C"]
    ), error


def add_kuwaiti_oil_fires_to_kuwait(tb: Table) -> Table:
    tb = tb.copy()

    # NOTE: Use this function before harmonizing country names. Otherwise adapt the following definitions.
    kuwait = "Kuwait"
    oil_fires = "Kuwaiti Oil Fires"

    # Sanity check.
    error = f"'{kuwait}' or '{oil_fires}' not found in the data."
    assert kuwait in set(tb["country"]), error
    assert oil_fires in set(tb["country"]), error

    # Add the emissions from the Kuwaiti oil fires (in 1991) to Kuwait.
    tb_kuwait = tb[tb["country"] == kuwait].drop(columns="country").set_index("year")
    tb_oil_fires = tb[tb["country"] == oil_fires].drop(columns="country").fillna(0).set_index(["year"])
    tb_combined = (tb_kuwait + tb_oil_fires).reset_index().assign(**{"country": kuwait})

    # Replace the origina data for Kuwait by the combined data.
    tb_updated = pr.concat([tb[tb["country"] != kuwait].reset_index(drop=True), tb_combined], ignore_index=True)

    # Sort conveniently.
    tb_updated = tb_updated.sort_values(["country", "year"]).reset_index(drop=True)

    return tb_updated


def add_emissions_in_co2_equivalents(tb: Table) -> Table:
    # Add columns for fossil/land/total emissions of CH4 in terms of CO2 equivalents.
    # NOTE: For methane, we apply different conversion factors for fossil and land-use emissions.
    tb["annual_emissions_ch4_fossil_co2eq"] = (
        tb["annual_emissions_ch4_fossil"] * CH4_FOSSIL_EMISSIONS_TO_CO2_EQUIVALENTS
    )
    tb["annual_emissions_ch4_land_co2eq"] = tb["annual_emissions_ch4_land"] * CH4_LAND_EMISSIONS_TO_CO2_EQUIVALENTS
    tb["annual_emissions_ch4_total_co2eq"] = (
        tb["annual_emissions_ch4_fossil_co2eq"] + tb["annual_emissions_ch4_land_co2eq"]
    )

    # Add columns for fossil/land/total emissions of N2O in terms of CO2 equivalents.
    # NOTE: For nitrous oxide, we apply the same conversion factors for fossil and land-use emissions.
    for component in ["fossil", "land", "total"]:
        tb[f"annual_emissions_n2o_{component}_co2eq"] = (
            tb[f"annual_emissions_n2o_{component}"] * N2O_EMISSIONS_TO_CO2_EQUIVALENTS
        )

    # Add columns for fossil/land/total emissions of all GHG in terms of CO2 equivalents.
    # NOTE: The file of annual emissions does not include GHG emissions, which is why we need to add them now.
    #  However, the files of temperature response and cumulative emissions do include GHG emissions.
    for component in ["fossil", "land", "total"]:
        tb[f"annual_emissions_ghg_{component}_co2eq"] = (
            tb[f"annual_emissions_co2_{component}"]
            + tb[f"annual_emissions_ch4_{component}_co2eq"]
            + tb[f"annual_emissions_n2o_{component}_co2eq"]
        )

    return tb


def add_region_aggregates(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    for region in REGIONS:
        # List members in this region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            additional_regions=REGIONS[region].get("additional_regions", None),
            excluded_regions=REGIONS[region].get("excluded_regions", None),
            additional_members=REGIONS[region].get("additional_members", None),
            excluded_members=REGIONS[region].get("excluded_members", None),
        )
        tb = geo.add_region_aggregates(
            df=tb,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            # Here we allow aggregating even when there are few countries informed.
            # However, if absolutely all countries have nan, we want the aggregate to be nan, not zero.
            frac_allowed_nans_per_year=0.999,
        )

    return tb


def add_share_variables(tb: Table) -> Table:
    tb = tb.copy()

    # Create "share" variables (percentages with respect to global).
    # To do that, first create a separate table for global data, and add it to the main table.
    tb_global = tb[tb["country"] == "World"][["year"] + SHARE_VARIABLES].reset_index(drop=True)

    tb = pr.merge(tb, tb_global, on=["year"], how="left", suffixes=("", "_global"))
    # For a list of variables, add the percentage with respect to global.
    for variable in SHARE_VARIABLES:
        new_variable = f"share_of_{variable.replace('_co2eq', '')}"
        tb[new_variable] = 100 * tb[variable] / tb[f"{variable}_global"]

    # Drop unnecessary columns for global data.
    tb = tb.drop(columns=[column for column in tb.columns if column.endswith("_global")])

    return tb


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    tb = tb.copy()

    # Add population to data.
    tb = geo.add_population_to_table(
        tb=tb,
        ds_population=ds_population,
        warn_on_missing_countries=False,
    )

    # Add per-capita variables.
    for variable in PER_CAPITA_VARIABLES:
        tb[f"{variable}_per_capita"] = tb[variable] / tb["population"]

    # Drop population column.
    tb = tb.drop(columns="population")

    return tb


def run_sanity_checks_on_outputs(tb: Table) -> None:
    error = "Share of global emissions cannot be larger than 101%"
    assert (tb[[column for column in tb.columns if "share" in column]].max() < 101).all(), error
    error = "Share of global emissions was not expected to be smaller than -1%"
    # Some countries did contribute negatively to CO2 emissions, however overall the negative contribution is always
    # smaller than 1% in absolute value.
    assert (tb[[column for column in tb.columns if "share" in column]].min() > -1).all(), error

    # Ensure that no country contributes to emissions more than the entire world.
    columns_that_should_be_smaller_than_global = [
        column for column in tb.drop(columns=["country", "year"]).columns if "capita" not in column
    ]
    tb_global = tb[tb["country"] == "World"].drop(columns="country")
    check = pr.merge(
        tb[tb["country"] != "World"].reset_index(drop=True), tb_global, on="year", how="left", suffixes=("", "_global")
    )
    for column in columns_that_should_be_smaller_than_global:
        # It is in principle possible that some region would emit more than the world, if the rest of regions
        # were contributing with negative CO2 emissions (e.g. High-income countries in 1854).
        # However, the difference should be very small.
        error = f"Region contributed to {column} more than the entire world."
        assert check[(check[column] - check[f"{column}_global"]) / check[f"{column}_global"] > 0.00001].empty, error


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("national_contributions")
    tb = ds_meadow["national_contributions"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Sanity checks.
    run_sanity_checks_on_inputs(tb=tb)

    # Rename gases and components.
    tb["gas"] = Variable(
        map_series(
            series=tb["gas"], mapping=GASES_RENAMING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
        )
    ).copy_metadata(tb["gas"])
    tb["component"] = Variable(
        map_series(
            series=tb["component"],
            mapping=COMPONENTS_RENAMING,
            warn_on_missing_mappings=True,
            warn_on_unused_mappings=True,
        )
    ).copy_metadata(tb["component"])

    # Convert units from teragrams and petagrams to tonnes.
    tb.loc[tb["unit"].str.startswith("Tg"), "data"] *= TERAGRAMS_TO_TONNES
    tb.loc[tb["unit"].str.startswith("Pg"), "data"] *= PETAGRAMS_TO_TONNES

    # Transpose data.
    tb = tb.pivot(
        index=["country", "year"], columns=["file", "gas", "component"], values="data", join_column_levels_with="_"
    )

    # We add the emissions from the Kuwaiti oil fires in 1991 (which are also included as a separate country) as part
    # of the emissions of Kuwait.
    # This ensures that these emissions will be included in aggregates of regions that include Kuwait.
    tb = add_kuwaiti_oil_fires_to_kuwait(tb=tb)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    )

    # Add region aggregates.
    tb = add_region_aggregates(tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Add columns for emissions in terms of CO2 equivalents.
    tb = add_emissions_in_co2_equivalents(tb=tb)

    # Add "share" variables (percentages with respect to global emissions).
    tb = add_share_variables(tb=tb)

    # Add per-capita variables.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Ensure data starts from a certain fixed year (see notes above).
    tb = tb[tb["year"] >= YEAR_MIN].reset_index(drop=True)

    # Sanity checks.
    run_sanity_checks_on_outputs(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
