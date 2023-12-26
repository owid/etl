"""Garden step that combines EI's statistical review with Ember's combined electricity data (combination of the European
Electricity Review and the Yearly Electricity Data) to create the Electricity Mix (EI & Ember) dataset.

"""

from typing import Dict, List

from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.data_helpers.geo import add_population_to_table
from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Megatonnes to grams.
MT_TO_G = 1e12


def process_statistical_review_data(tb_review: Table) -> Table:
    """Load necessary columns from EI's Statistical Review dataset, and create some new variables (e.g. electricity
    generation from fossil fuels).

    Parameters
    ----------
    table_ei : Table
        EI's Statistical Review (already processed, with harmonized countries and region aggregates).

    Returns
    -------
    tb_review : Table
        Processed EI data.

    """
    # Columns to load from EI dataset.
    columns = {
        "electricity_generation_twh": "total_generation__twh",
        "primary_energy_consumption_equivalent_twh": "primary_energy_consumption__twh",
        "hydro_electricity_generation_twh": "hydro_generation__twh",
        "nuclear_electricity_generation_twh": "nuclear_generation__twh",
        "solar_electricity_generation_twh": "solar_generation__twh",
        "wind_electricity_generation_twh": "wind_generation__twh",
        "other_renewables_electricity_generation_twh": "other_renewables_including_bioenergy_generation__twh",
        "oil_electricity_generation_twh": "oil_generation__twh",
        "coal_electricity_generation_twh": "coal_generation__twh",
        "gas_electricity_generation_twh": "gas_generation__twh",
    }
    tb_review = tb_review[list(columns)].rename(columns=columns, errors="raise")
    # New columns to be created by summing other columns.
    aggregates: Dict[str, List[str]] = {
        "fossil_generation__twh": [
            "oil_generation__twh",
            "coal_generation__twh",
            "gas_generation__twh",
        ],
        "renewable_generation__twh": [
            "hydro_generation__twh",
            "solar_generation__twh",
            "wind_generation__twh",
            "other_renewables_including_bioenergy_generation__twh",
        ],
        "low_carbon_generation__twh": [
            "renewable_generation__twh",
            "nuclear_generation__twh",
        ],
        "solar_and_wind_generation__twh": [
            "solar_generation__twh",
            "wind_generation__twh",
        ],
    }

    # Create a table with a dummy index.
    tb_review = tb_review.reset_index()

    # Create new columns, by adding up other columns (and allowing for zero nans in each sum).
    for new_column in aggregates:
        tb_review[new_column] = tb_review[aggregates[new_column]].sum(axis=1, min_count=len(aggregates[new_column]))

    return tb_review


def process_ember_data(tb_ember: Table) -> Table:
    """Load necessary columns from the Combined Electricity dataset and prepare a table with the required variables.

    Parameters
    ----------
    table_ember : Table
        Combined Electricity (combination of Ember's Yearly Electricity Data and European Electricity Review).

    Returns
    -------
    df_ember : Table
        Processed Combined Electricity data.

    """
    # Columns to load from Ember dataset.
    columns = {
        "generation__bioenergy__twh": "bioenergy_generation__twh",
        "generation__gas__twh": "gas_generation__twh",
        "generation__coal__twh": "coal_generation__twh",
        "generation__other_fossil__twh": "oil_generation__twh",
        "generation__renewables__twh": "renewable_generation__twh",
        "generation__other_renewables__twh": "other_renewables_excluding_bioenergy_generation__twh",
        "generation__clean__twh": "low_carbon_generation__twh",
        "generation__hydro__twh": "hydro_generation__twh",
        "generation__nuclear__twh": "nuclear_generation__twh",
        "generation__solar__twh": "solar_generation__twh",
        "generation__wind__twh": "wind_generation__twh",
        "generation__fossil__twh": "fossil_generation__twh",
        "generation__total_generation__twh": "total_generation__twh",
        "demand__total_demand__twh": "total_demand__twh",
        "emissions__total_emissions__mtco2": "total_emissions__mtco2",
        "emissions__co2_intensity__gco2_kwh": "co2_intensity__gco2_kwh",
        "imports__total_net_imports__twh": "total_net_imports__twh",
    }
    tb_ember = tb_ember[list(columns)].rename(columns=columns, errors="raise")

    # Create a table with a dummy index.
    tb_ember = tb_ember.reset_index()

    # In EI data, there is a variable "Geo Biomass Other", which combines all other renewables.
    # In Ember data, "other renewables" excludes bioenergy.
    # To be able to combine both datasets, create a new variable for generation of other renewables including bioenergy.
    tb_ember["other_renewables_including_bioenergy_generation__twh"] = (
        tb_ember["other_renewables_excluding_bioenergy_generation__twh"] + tb_ember["bioenergy_generation__twh"]
    )

    # Create a new variable for solar and wind generation.
    tb_ember["solar_and_wind_generation__twh"] = tb_ember["solar_generation__twh"] + tb_ember["wind_generation__twh"]

    return tb_ember


def add_per_capita_variables(combined: Table, ds_population: Dataset) -> Table:
    """Add per capita variables (in kWh per person) to the combined EI and Ember table.

    The list of variables to make per capita are given in this function. The new variable names will be 'per_capita_'
    followed by the original variable's name.

    Parameters
    ----------
    combined : Table
        Combination of EI's Statistical Review and Ember's Combined Electricity.
    ds_population: Dataset
        Population dataset.

    Returns
    -------
    combined : Table
        Input table after adding per capita variables.

    """
    combined = combined.copy()

    # Variables to make per capita.
    per_capita_variables = [
        "bioenergy_generation__twh",
        "coal_generation__twh",
        "fossil_generation__twh",
        "gas_generation__twh",
        "hydro_generation__twh",
        "low_carbon_generation__twh",
        "nuclear_generation__twh",
        "oil_generation__twh",
        "other_renewables_excluding_bioenergy_generation__twh",
        "other_renewables_including_bioenergy_generation__twh",
        "renewable_generation__twh",
        "solar_generation__twh",
        "total_generation__twh",
        "wind_generation__twh",
        "solar_and_wind_generation__twh",
    ]
    # Add a column for population (only for harmonized countries).
    combined = add_population_to_table(tb=combined, ds_population=ds_population, warn_on_missing_countries=False)

    for variable in per_capita_variables:
        assert "twh" in variable, f"Variables are assumed to be in TWh, but {variable} is not."
        new_column = "per_capita_" + variable.replace("__twh", "__kwh")
        combined[new_column] = combined[variable] * TWH_TO_KWH / combined["population"]

    return combined


def add_share_variables(combined: Table) -> Table:
    """Add variables for the electricity generation as a share of the total electricity generation (as a percentage).

    The following new variables will be created:
    * For each source (e.g. coal_generation__twh) in a list given in this function, a new variable will be created
      (named, e.g. coal_share_of_electricity__pct).
    * Total electricity generation as a share of primary energy consumption.
    * Total net electricity imports as a share of total electricity demand.

    Parameters
    ----------
    combined : Table
        Combination of EI's Statistical Review and Ember's Combined Electricity.

    Returns
    -------
    combined : Table
        Input table after adding share variables.

    """
    # Variables to make as share of electricity (new variable names will be the name of the original variable followed
    # by '_share_of_electricity__pct').
    share_variables = [
        "bioenergy_generation__twh",
        "coal_generation__twh",
        "fossil_generation__twh",
        "gas_generation__twh",
        "hydro_generation__twh",
        "low_carbon_generation__twh",
        "nuclear_generation__twh",
        "oil_generation__twh",
        "other_renewables_excluding_bioenergy_generation__twh",
        "other_renewables_including_bioenergy_generation__twh",
        "renewable_generation__twh",
        "solar_generation__twh",
        "total_generation__twh",
        "wind_generation__twh",
        "solar_and_wind_generation__twh",
    ]
    for variable in share_variables:
        new_column = variable.replace("_generation__twh", "_share_of_electricity__pct")
        combined[new_column] = 100 * combined[variable] / combined["total_generation__twh"]

    # Calculate the percentage of electricity as a share of primary energy.
    combined["total_electricity_share_of_primary_energy__pct"] = (
        100 * combined["total_generation__twh"] / combined["primary_energy_consumption__twh"]
    )

    # Calculate the percentage of electricity demand that is imported.
    combined["net_imports_share_of_demand__pct"] = (
        100 * combined["total_net_imports__twh"] / combined["total_demand__twh"]
    )

    # Sanity check.
    error = "Total electricity share does not add up to 100%."
    assert all(abs(combined["total_share_of_electricity__pct"].dropna() - 100) < 0.01), error

    # Remove unnecessary columns.
    combined = combined.drop(columns=["total_share_of_electricity__pct"], errors="raise")

    return combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load EI's statistical review dataset and read its main table.
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review["statistical_review_of_world_energy"]

    # Load Ember's combined electricity dataset and read its main table.
    ds_ember = paths.load_dataset("combined_electricity")
    tb_ember = ds_ember["combined_electricity"]

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Prepare EI and Ember data.
    tb_review = process_statistical_review_data(tb_review=tb_review)
    tb_ember = process_ember_data(tb_ember=tb_ember)

    ####################################################################################################################
    # There is a big discrepancy between Oceania's oil generation from the Energy Institute and Ember.
    # Ember's oil generation is significantly larger. The reason seems to be that the Energy Institute's Statistical
    # Review has no data for Papua New Guinea and New Caledonia (except the zeros on nuclear generation that were
    # manually imputed in the Statistical Review garden step), while Ember does have data for both.
    # Therefore, to avoid spurious jumps in the intersection between EI and Ember data, we remove Oceania data from EI
    # before combining both tables.
    # Specifically, the columns where the discrepancy between EI and Ember is notorious are oil and gas generation (and
    # therefore fossil generation).

    # First check that indeed there is no data for Papua New Guinea in EI.
    error = (
        "Expected no oil or gas generation data for Papua New Guinea and New Caledonia in the Statistical Review. "
        "This is no longer the case. Check if now EI and Ember Oceania data are consistent and if so, remove this code."
    )
    affected_columns = ["oil_generation__twh", "gas_generation__twh", "fossil_generation__twh"]
    assert (
        tb_review[tb_review["country"].isin(["Papua New Guinea", "New Caledonia"])][affected_columns]
        .dropna(how="all")
        .empty
    ), error
    tb_review.loc[tb_review["country"] == "Oceania", affected_columns] = None
    ####################################################################################################################

    # Combine both tables, giving priority to Ember data (on overlapping values).
    combined = combine_two_overlapping_dataframes(df1=tb_ember, df2=tb_review, index_columns=["country", "year"])

    # Add carbon intensities.
    # There is already a variable for this in the Ember dataset, but now that we have combined
    # EI and Ember data, intensities should be recalculated for consistency.
    combined["co2_intensity__gco2_kwh"] = (combined["total_emissions__mtco2"] * MT_TO_G) / (
        combined["total_generation__twh"] * TWH_TO_KWH
    )

    # Add per capita variables.
    combined = add_per_capita_variables(combined=combined, ds_population=ds_population)

    # Add "share" variables.
    combined = add_share_variables(combined=combined)

    # Set an appropriate index and sort rows and columns conveniently.
    combined = combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Update table's short name.
    combined.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[combined], check_variables_metadata=True)
    ds_garden.save()
