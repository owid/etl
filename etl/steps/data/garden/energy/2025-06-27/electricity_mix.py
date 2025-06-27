"""Garden step that combines EI's statistical review with Ember's yearly electricity data to create the Electricity Mix
(EI & Ember) dataset.

"""

from typing import Dict, List

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.geo import add_population_to_table
from etl.helpers import PathFinder

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
        # Load primary energy consumption from fossil fuels and biofuels, to be able to calculate direct primary energy.
        # Direct primary energy consumption is needed to calculate the share of electricity in primary energy.
        # Once direct primary energy consumption and the share of electricity in primary energy are calculated, these
        # columns will be dropped.
        "oil_consumption_twh": "oil_consumption__twh",
        "coal_consumption_twh": "coal_consumption__twh",
        "gas_consumption_twh": "gas_consumption__twh",
        "biofuels_consumption_twh": "biofuels_consumption__twh",
        # Load efficiency factor to be able to convert from electricity generation into input-equivalent primary energy.
        # Currently it is not used, since we do the calculation of the share of electricity in primary energy in terms
        # of direct primary energy consumption.
        # "efficiency_factor": "efficiency_factor",
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
    """Load necessary columns from the Yearly Electricity dataset and prepare a table with the required variables.

    Parameters
    ----------
    table_ember : Table
        Yearly Electricity Data.

    Returns
    -------
    df_ember : Table
        Processed Yearly Electricity data.

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
        Combination of EI's Statistical Review and Ember's Yearly Electricity Data.
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
        "total_demand__twh",
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
        Combination of EI's Statistical Review and Ember's Yearly Electricity Data.

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

    # Calculate the share of primary energy consumption that comes from electricity.
    # One could think that it is enough to divide total electricity generation by primary energy consumption.
    # However, electricity generation is measured in direct outputs, while primary energy consumption (from the
    # statistical review) includes thermal losses from fossil fuels (which is reasonable) plus the thermal losses of
    # non-fossil sources, as if they were as inefficient as fossil fuels.
    # Therefore, to properly calculate the share of electricity in primary energy, we have two options:
    # (A) Share of electricity in direct primary energy consumption.
    # (B) Share of electricity in input-equivalent primary energy consumption (but properly calculated).
    # We decided to use (A) instead of (B), but just in case we change our mind in the future (or decide to have both),
    # below is also the code to achieve (B).

    # (A) Share of electricity in direct primary energy consumption:
    #    100 * total generation / direct primary energy consumption
    #    Here, since the Statistical Review does not provide data for direct primary energy consumption, we can estimate
    #    it as the sum primary energy consumption from fossil fuels and biofuels plus electricity generation from
    #    non-fossil sources (nuclear, hydro, solar, wind and other).
    # NOTE: We impose that at least 3 out of 5 sources in the denominator need to be informed. This would not be
    # necessary once the spurious zeros in the Statistical Review are corrected.
    combined["direct_primary_energy_consumption__twh"] = combined[
        [
            "low_carbon_generation__twh",
            "coal_consumption__twh",
            "oil_consumption__twh",
            "gas_consumption__twh",
            "biofuels_consumption__twh",
        ]
    ].sum(axis=1, min_count=3)
    combined["total_electricity_share_of_primary_energy__pct"] = (
        100 * combined["total_generation__twh"] / combined["direct_primary_energy_consumption__twh"]
    )
    # Drop unnecessary columns.
    combined = combined.drop(
        columns=["coal_consumption__twh", "oil_consumption__twh", "gas_consumption__twh", "biofuels_consumption__twh"],
        errors="raise",
    )

    # (B) Share of electricity in input-equivalent primary energy consumption:
    #    100 * (total generation / efficiency factor) / input-equivalent primary energy consumption
    #    In other words, we assume that all electricity is produced in the same inefficient way as fossil fuel
    #    electricity, and divide by input-equivalent primary energy consumption.
    #    NOTE:
    #      * Here we should not only divide renewables and nuclear by the efficiency factor. If we did that, we
    #    would have in the numerator losses from renewables and nuclear, but not from fossil fuels (while in the
    #    denominator we would be accounting for the losses of all three).
    #      * As explained in the statistical review methodology, "From 2022 onwards, we assume a constant
    #    efficiency of 32% for biomass power to better reflect the actual efficiency of biomass power plants."
    #      * Given that some sources are less often informed, fill some of their missing values with zeros.
    #    Otherwise a lot of valuable data is lost for a small percentage of missing data. This is mostly due to the
    #    statistical review data file having many missing values instead of zeros (which has been manually corrected in
    #    the statistical review garden step for nuclear, but not for other sources).
    # BIOMASS_EFFICIENCY_FACTOR = 0.32
    # combined["total_electricity_share_of_primary_energy__pct"] = (
    #     (
    #         (
    #             (
    #                 combined["nuclear_generation__twh"]
    #                 + combined["hydro_generation__twh"].fillna(0)
    #                 + combined["wind_generation__twh"].fillna(0)
    #                 + combined["solar_generation__twh"].fillna(0)
    #                 + combined["other_renewables_excluding_bioenergy_generation__twh"].fillna(0)
    #                 + (combined["fossil_generation__twh"])
    #             )
    #             / combined["efficiency_factor"]
    #         )
    #         + (combined["bioenergy_generation__twh"].fillna(0) / BIOMASS_EFFICIENCY_FACTOR)
    #     )
    #     / combined["primary_energy_consumption__twh"]
    #     * 100
    # )

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


def fix_discrepancies_in_aggregate_regions(tb_review: Table, tb_ember: Table, combined: Table) -> Table:
    # In Ember's data, we removed data for aggregate regions (e.g. income groups) for the latest year.
    # We did that because the latest year is not informed for all countries, and aggregate regions therefore often
    # present a significant (spurious) dip.
    # This issue does not happen in the Statistical Review, which has data for aggregate regions in the latest year.
    # But now, when combining both, the difference between data from the Statistical Review and Ember is notorious for
    # aggregate regions.

    # Remove data for aggregate regions for the latest year (which was removed from Ember data, as explained above).
    for region in geo.REGIONS:
        for col in combined.drop(columns=["country", "year"]).columns:
            combined.loc[(combined["country"] == region) & (combined["year"] == combined["year"].max()), col] = np.nan

    # Note that this issue does not only affect the latest year, but is also noticeable in the intersection between Statistical Review and Ember data (on year 2000).
    # One solution to this problem would be to simply stick to one of the two sources (namely Ember, which tends to be more complete on the years where it is informed), but then we would lose a significant amount of data (all data prior to 2000 from the Statistical Review).
    # Instead, we remove data prior to 2000 and for latest year only for specific indicators where the discrepancy is particularly significant.

    # Define the maximum median relative error between Statistical Review and Ember (for a given region and indicator).
    # If the error is larger than this, we will only take Ember data.
    maximum_median_error = 0.2
    # Define the regions and indicators where the median error is exceeded.
    segments_not_combined = {region: [] for region in geo.REGIONS}
    segments_not_combined.update(
        {
            "Low-income countries": [
                "coal_generation__twh",
                "fossil_generation__twh",
                "gas_generation__twh",
                "hydro_generation__twh",
                "low_carbon_generation__twh",
                "oil_generation__twh",
                "other_renewables_including_bioenergy_generation__twh",
                "renewable_generation__twh",
                "total_generation__twh",
            ],
            "Lower-middle-income countries": [
                "fossil_generation__twh",
                "gas_generation__twh",
                "hydro_generation__twh",
                "low_carbon_generation__twh",
                "oil_generation__twh",
                "other_renewables_including_bioenergy_generation__twh",
                "renewable_generation__twh",
                "solar_and_wind_generation__twh",
                "solar_generation__twh",
                "wind_generation__twh",
            ],
            "Upper-middle-income countries": [
                "gas_generation__twh",
                "oil_generation__twh",
                "solar_and_wind_generation__twh",
                "solar_generation__twh",
                "wind_generation__twh",
            ],
            "High-income countries": [
                "oil_generation__twh",
                "solar_and_wind_generation__twh",
                "solar_generation__twh",
                "wind_generation__twh",
            ],
            "Europe": [
                "oil_generation__twh",
                "renewable_generation__twh",
                "solar_and_wind_generation__twh",
                "solar_generation__twh",
                "wind_generation__twh",
            ],
            "North America": [
                "oil_generation__twh",
                "solar_and_wind_generation__twh",
                "solar_generation__twh",
                "wind_generation__twh",
            ],
            "European Union (27)": [
                "oil_generation__twh",
                "renewable_generation__twh",
                "solar_and_wind_generation__twh",
                "solar_generation__twh",
                "wind_generation__twh",
            ],
            "Africa": ["solar_and_wind_generation__twh", "solar_generation__twh", "wind_generation__twh"],
            "Asia": ["solar_and_wind_generation__twh", "solar_generation__twh", "wind_generation__twh"],
            "Oceania": [
                "low_carbon_generation__twh",
                "renewable_generation__twh",
                "solar_and_wind_generation__twh",
                "solar_generation__twh",
                "wind_generation__twh",
            ],
            "South America": ["solar_and_wind_generation__twh", "solar_generation__twh", "wind_generation__twh"],
        }
    )
    for region in segments_not_combined:
        _remove_combination = []
        for col in combined.drop(columns=["country", "year"]).columns:
            if (col in tb_review.columns) and (col in tb_ember.columns):
                compared = pd.merge(
                    tb_review[tb_review["country"] == region][["year", col]].dropna(),
                    tb_ember[tb_ember["country"] == region][["year", col]].dropna(),
                    how="inner",
                    on="year",
                    suffixes=("_review", "_ember"),
                )
                if len(compared) > 0:
                    median_error = np.median(
                        (abs(compared[f"{col}_review"] - compared[f"{col}_ember"])) / abs(compared[f"{col}_ember"])
                    )
                    if median_error > maximum_median_error:
                        _remove_combination.append(col)
                        # px.line(compared.melt(id_vars="year"), x="year", y="value", color="variable", markers=True, title=f"{region} - {col}").show()
                        assert compared["year"].min() == 2000, "Minimum year changed."
        error = f"Expected discrepancies between Statistical Review and Ember data for aggregate regions may have changed for region: {region}. Current discrepant indicators: {_remove_combination}. Use this list in 'segments_not_combined'."
        if set(segments_not_combined[region]) != set(_remove_combination):
            log.error(error)

        for col in _remove_combination:
            # Remove data for years prior to 2000 (which correspond to the Statistical Review).
            # NOTE: This may need to be generalized if Ember adds data prior to 2000 (which is the case already for European countries, but they are so far not affected by the discrepancies).
            combined.loc[(combined["country"] == region) & (combined["year"] < 2000), col] = np.nan

    return combined


def run() -> None:
    #
    # Load data.
    #
    # Load EI's statistical review dataset and read its main table.
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy", reset_index=False)

    # Load Ember's yearly electricity dataset and read its main table.
    ds_ember = paths.load_dataset("yearly_electricity")
    tb_ember = ds_ember.read("yearly_electricity", reset_index=False)

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
    # Review has spurious zeros for Papua New Guinea and New Caledonia (all electricity columns are zero)
    # while Ember does have data for both countries.
    # Therefore, to avoid spurious jumps in the intersection between EI and Ember data, we remove Oceania data from EI
    # before combining both tables.
    # Specifically, the columns where the discrepancy between EI and Ember is notorious are oil and gas generation (and
    # therefore fossil generation).

    # First check that indeed there is no data for Papua New Guinea and New Caledonia in EI.
    error = "Expected all electricity data for Papua New Guinea and New Caledania to be zero in the Statistical Review."
    assert (
        (
            tb_review[tb_review["country"].isin(["Papua New Guinea", "New Caledonia"])].fillna(0)[
                [c for c in tb_review.columns if c not in ["country", "year"]]
            ]
            == 0
        )
        .all()
        .all()
    )
    affected_columns = ["oil_generation__twh", "gas_generation__twh", "fossil_generation__twh"]
    tb_review.loc[tb_review["country"] == "Oceania", affected_columns] = None

    # We also remove all electricity data for these countries from the Statistical Review, given that they are all zero
    # (most of them spurious).
    tb_review.loc[
        (tb_review["country"].isin(["Papua New Guinea", "New Caledonia"])),
        tb_review.drop(columns=["country", "year"]).columns,
    ] = None

    # Coal generation in Ember data is missing.
    # The reason may be that Switzerland stopped using coal for electricity before year 2000:
    # https://data.worldbank.org/indicator/EG.ELC.COAL.ZS?locations=CH
    # Ideally, the data should be zero, instead of missing.
    error = "Expected missing data for Switzerland coal generation. That may no longer be the case. Remove this code."
    assert (
        tb_ember.loc[(tb_ember["country"] == "Switzerland") & (tb_ember["year"] > 1999)]["coal_generation__twh"]
        .isnull()
        .all()
    ), error
    tb_ember.loc[(tb_ember["country"] == "Switzerland") & (tb_ember["year"] > 1999), "coal_generation__twh"] = 0
    ####################################################################################################################

    # Combine both tables, giving priority to Ember data (on overlapping values).
    combined = combine_two_overlapping_dataframes(df1=tb_ember, df2=tb_review, index_columns=["country", "year"])

    # Remove combined data for aggregate regions where Ember and the Statistical Review have a strong disagreement.
    # This way we avoid spurious jumps in the combined series.
    combined = fix_discrepancies_in_aggregate_regions(tb_review=tb_review, tb_ember=tb_ember, combined=combined)

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

    ####################################################################################################################
    # There is a sudden drop in the share of fossil generation in 2023 for low-income countries (and hence a sudden increase in the share of renewables).
    # This may be due mostly to Syria and North Korea, that have the largest fossil generation among low-income countries, and are not informed in 2023.
    # For safety, assert that the issue is in the data, and if so, remove the aggregate for all columns (since those countries seem to be missing in almost all columns).
    check = combined.loc[
        (combined["country"].isin(["Low-income countries"])) & (combined["year"].isin([2022, 2023])),
        "fossil_share_of_electricity__pct",
    ]
    error = "Low-income countries fossil generation used to drop by 35% from 2022 to 2023 (due to missing data). This issue may have been fixed, so, remove this temporary solution."
    assert 100 * (check.iloc[-2] - check.iloc[-1]) / check.iloc[-2] > 35, error
    combined.loc[
        (combined["country"].isin(["Low-income countries"])) & (combined["year"] == 2023),
        combined.drop(columns=["country", "year"]).columns,
    ] = pd.NA
    ####################################################################################################################

    # Format table conveniently.
    combined = combined.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[combined])
    ds_garden.save()
