"""Garden step that combines BP's statistical review with Ember's combined electricity data (combination of the European
Electricity Review and the Yearly Electricity Data) to create the Electricity Mix (BP & Ember) dataset.

"""

from typing import Dict, List

from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.data_helpers.geo import add_population_to_table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Megatonnes to grams.
MT_TO_G = 1e12


def process_statistical_review_data(tb_review: Table) -> Table:
    """Load necessary columns from BP's Statistical Review dataset, and create some new variables (e.g. electricity
    generation from fossil fuels).

    Parameters
    ----------
    table_bp : Table
        BP's Statistical Review (already processed, with harmonized countries and region aggregates).

    Returns
    -------
    df_bp : Table
        Processed BP data.

    """
    # Columns to load from BP dataset.
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

    # Create new columns, by adding up other columns (and allowing for only one nan in each sum).
    for new_column in aggregates:
        tb_review[new_column] = tb_review[aggregates[new_column]].sum(axis=1, min_count=len(aggregates[new_column]) - 1)

    return tb_review


def process_ember_data(tb_ember: Table) -> Table:
    """Load necessary columns from the Combined Electricity dataset and prepare a dataframe with the required variables.

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

    # In BP data, there is a variable "Geo Biomass Other", which combines all other renewables.
    # In Ember data, "other renewables" excludes bioenergy.
    # To be able to combine both datasets, create a new variable for generation of other renewables including bioenergy.
    tb_ember["other_renewables_including_bioenergy_generation__twh"] = (
        tb_ember["other_renewables_excluding_bioenergy_generation__twh"] + tb_ember["bioenergy_generation__twh"]
    )

    # Create a new variable for solar and wind generation.
    tb_ember["solar_and_wind_generation__twh"] = tb_ember["solar_generation__twh"] + tb_ember["wind_generation__twh"]

    return tb_ember


def add_per_capita_variables(combined: Table, ds_population: Dataset) -> Table:
    """Add per capita variables (in kWh per person) to the combined BP and Ember dataframe.

    The list of variables to make per capita are given in this function. The new variable names will be 'per_capita_'
    followed by the original variable's name.

    Parameters
    ----------
    combined : Table
        Combination of BP's Statistical Review and Ember's Combined Electricity.
    ds_population: Dataset
        Population dataset.

    Returns
    -------
    combined : Table
        Input dataframe after adding per capita variables.

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
        Combination of BP's Statistical Review and Ember's Combined Electricity.

    Returns
    -------
    combined : Table
        Input dataframe after adding share variables.

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
    combined = combined.drop(columns=["total_share_of_electricity__pct"])

    return combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load BP's statistical review dataset and read its main table.
    ds_review: Dataset = paths.load_dependency("statistical_review_of_world_energy")
    tb_review = ds_review["statistical_review_of_world_energy"]

    # Load Ember's combined electricity dataset and read its main table.
    ds_ember: Dataset = paths.load_dependency("combined_electricity")
    tb_ember = ds_ember["combined_electricity"]

    # Load population dataset.
    ds_population: Dataset = paths.load_dependency("population")

    #
    # Process data.
    #
    # Prepare BP and Ember data.
    tb_review = process_statistical_review_data(tb_review=tb_review)
    tb_ember = process_ember_data(tb_ember=tb_ember)

    # Combine both tables, giving priority to Ember data (on overlapping values).
    combined = combine_two_overlapping_dataframes(df1=tb_ember, df2=tb_review, index_columns=["country", "year"])
    ####################################################################################################################
    # NOTE: The previous operation does not propagate metadata properly, so we do it manually.
    for column in combined.columns:
        sources = []
        licenses = []
        # Gather all sources and licenses for this column.
        for table in [tb_ember, tb_review]:
            if column in table.columns:
                sources.extend(table[column].metadata.sources)
                licenses.extend(table[column].metadata.licenses)
        # Assign the gathered sources and licenses to the new column.
        combined[column].sources = sources
        combined[column].licenses = licenses
    combined.metadata.short_name = paths.short_name
    ####################################################################################################################

    # Add carbon intensities.
    # There is already a variable for this in the Ember dataset, but now that we have combined
    # BP and Ember data, intensities should be recalculated for consistency.
    combined["co2_intensity__gco2_kwh"] = (combined["total_emissions__mtco2"] * MT_TO_G) / (
        combined["total_generation__twh"] * TWH_TO_KWH
    )

    # Add per capita variables.
    combined = add_per_capita_variables(combined=combined, ds_population=ds_population)

    # Add "share" variables.
    combined = add_share_variables(combined=combined)

    # Set an appropriate index and sort rows and columns conveniently.
    combined = combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir, tables=[combined], default_metadata=ds_ember.metadata, check_variables_metadata=True
    )
    ds_garden.save()
