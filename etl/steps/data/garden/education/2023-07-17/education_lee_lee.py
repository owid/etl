"""Load a meadow dataset and create a garden dataset."""

from typing import List

import owid.catalog.processing as pr
import shared
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "World",
]


def add_data_for_regions(tb: Table, ds_regions: Dataset) -> Table:
    tb_with_regions = tb.copy()
    aggregations = {column: "mean" for column in tb_with_regions.columns if column not in ["country", "year"]}

    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(region=region, ds_regions=ds_regions)
        tb_with_regions = shared.add_region_aggregates_education(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.2,
            aggregations=aggregations,
        )
    for column in tb_with_regions.columns:
        if column not in ["year", "country"]:
            # Add origins metadata
            tb_with_regions[column].metadata.origins.append(tb[column].metadata.origins[0])
    return tb_with_regions


def run(dest_dir: str) -> None:
    #
    # Load input datasets
    #
    # Read the main education table from the meadow dataset and reset its index
    ds_meadow = paths.load_dataset("education_lee_lee")
    tb = ds_meadow["education_lee_lee"].reset_index()

    # Load dataset containing regions data.
    ds_regions = paths.load_dataset("regions")

    # Load dataset containing income groups data.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load the World Bank Education Dataset
    ds_garden_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_garden_wdi["wdi"]

    # Extract enrollment rates from the World Bank Education Dataset starting from 2010
    enrolment_wb = extract_related_world_bank_data(tb_wdi)
    # Add origins metadata to the WDI table (remove when the WDI dataset is updated with new metadata)
    from etl.data_helpers.misc import add_origins_to_wdi

    enrolment_wb = add_origins_to_wdi(enrolment_wb)

    # Get the list of columns from the World Bank dataset
    world_bank_indicators = enrolment_wb.columns

    #
    # Data Processing
    #
    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Replace age group values with descriptive labels
    tb["age_group"] = tb["age_group"].replace(
        {
            "15.0-64.0": "Youth and Adults (15-64 years)",
            "15.0-24.0": "Youth (15-24 years)",
            "25.0-64.0": "Adults (25-64 years)",
            "not specified": "Age not specified",
        }
    )

    # Prepare enrollment and attainment data
    tb_enrollment = prepare_enrollment_data(tb)
    tb_attainment = prepare_attainment_data(tb)
    tb_attainment = tb_attainment.dropna(subset=tb_attainment.columns.drop(["country", "year"]), how="all")
    tb_attainment = tb_attainment.reset_index(drop=True)

    # Merge enrollment and attainment data
    merged_tb = pr.merge(tb_enrollment, tb_attainment, on=["year", "country"], how="outer")

    # Drop columns related to historic population values
    merged_tb = merged_tb.drop(columns=[column for column in merged_tb.columns if "__thousands" in column])
    merged_tb.columns = [underscore(col) for col in merged_tb.columns]

    merged_tb = add_data_for_regions(tb=merged_tb, ds_regions=ds_regions)

    # Concatenate historical and more recent enrollment data
    hist_1985_tb = merged_tb[merged_tb["year"] < 1985]
    tb_merged_enrollment = pr.concat([enrolment_wb, hist_1985_tb[world_bank_indicators]])
    tb_merged_enrollment.set_index(["country", "year"], inplace=True)
    # Differentiate these columns from the original data
    tb_merged_enrollment.columns = tb_merged_enrollment.columns + "_combined_wb"
    tb_merged_enrollment = tb_merged_enrollment.reset_index()

    # Merge historical data with combined enrollment data
    tb_merged_wb = pr.merge(tb_merged_enrollment, merged_tb, on=["country", "year"], how="outer")
    tb_merged_wb = tb_merged_wb.dropna(how="all")

    # Create female to male enrollment ratios
    tb_merged_wb["female_over_male_enrollment_rates_primary"] = (
        tb_merged_wb["f_primary_enrollment_rates_combined_wb"] / tb_merged_wb["m_primary_enrollment_rates_combined_wb"]
    )
    tb_merged_wb["female_over_male_enrollment_rates_secondary"] = (
        tb_merged_wb["f_secondary_enrollment_rates_combined_wb"]
        / tb_merged_wb["m_secondary_enrollment_rates_combined_wb"]
    )
    tb_merged_wb["female_over_male_enrollment_rates_tertiary"] = (
        tb_merged_wb["f_tertiary_enrollment_rates_combined_wb"]
        / tb_merged_wb["m_tertiary_enrollment_rates_combined_wb"]
    )

    # Set metadata and format the dataframe for saving.
    tb_merged_wb.metadata.short_name = paths.short_name
    tb_merged_wb = tb_merged_wb.underscore().set_index(["country", "year"], verify_integrity=True)

    columns_to_use = [
        "mf_primary_enrollment_rates_combined_wb",
        "f_primary_enrollment_rates_combined_wb",
        "m_primary_enrollment_rates_combined_wb",
        "mf_secondary_enrollment_rates_combined_wb",
        "f_secondary_enrollment_rates_combined_wb",
        "m_secondary_enrollment_rates_combined_wb",
        "mf_tertiary_enrollment_rates_combined_wb",
        "f_tertiary_enrollment_rates_combined_wb",
        "m_tertiary_enrollment_rates_combined_wb",
        "female_over_male_enrollment_rates_primary",
        "female_over_male_enrollment_rates_secondary",
        "female_over_male_enrollment_rates_tertiary",
        "mf_youth_and_adults__15_64_years__percentage_of_no_education",
        "f_youth_and_adults__15_64_years__percentage_of_no_education",
        "f_adults__25_64_years__percentage_of_no_education",
        "mf_adults__25_64_years__percentage_of_tertiary_education",
        "mf_youth_and_adults__15_64_years__average_years_of_education",
        "f_youth_and_adults__15_64_years__average_years_of_education",
    ]
    tb_merged_wb = tb_merged_wb[columns_to_use]

    #
    # Save outputs
    #
    # Create a new garden dataset with the same metadata as the meadow dataset
    ds_garden = create_dataset(dest_dir, tables=[tb_merged_wb], check_variables_metadata=True)

    # Save the processed data in the new garden dataset
    ds_garden.save()


def extract_related_world_bank_data(tb_wb: Table):
    """
    Extracts enrollment rate indicators from the World Bank dataset.
    The function specifically extracts net enrollment rates up to secondary education and gross enrollment rates for tertiary education.

    :param tb_wb: Table containing World Bank education dataset
    :return: DataFrame with selected enrollment rates for years above 2010
    """

    # Define columns to select for enrolment rates
    select_enrolment_cols = [
        # Primary enrollment columns
        "se_prm_nenr",
        "se_prm_nenr_fe",
        "se_prm_nenr_ma",
        # Tertiary enrollment columns
        "se_ter_enrr",
        "se_ter_enrr_fe",
        "se_ter_enrr_ma",
        # Secondary enrollment columns
        "se_sec_nenr",
        "se_sec_nenr_fe",
        "se_sec_nenr_ma",
    ]

    # Dictionary to rename columns to be consistent with Lee dataset
    dictionary_to_rename_and_combine = {
        "se_prm_nenr": "mf_primary_enrollment_rates",
        "se_prm_nenr_fe": "f_primary_enrollment_rates",
        "se_prm_nenr_ma": "m_primary_enrollment_rates",
        "se_ter_enrr": "mf_tertiary_enrollment_rates",
        "se_ter_enrr_fe": "f_tertiary_enrollment_rates",
        "se_ter_enrr_ma": "m_tertiary_enrollment_rates",
        "se_sec_nenr": "mf_secondary_enrollment_rates",
        "se_sec_nenr_fe": "f_secondary_enrollment_rates",
        "se_sec_nenr_ma": "m_secondary_enrollment_rates",
    }

    # Select and rename columns
    enrolment_wb = tb_wb[select_enrolment_cols]
    enrolment_wb = enrolment_wb.rename(columns=dictionary_to_rename_and_combine)

    # Select data above 1985
    enrolment_wb = enrolment_wb[(enrolment_wb.index.get_level_values("year") >= 1985)]
    enrolment_wb = enrolment_wb.reset_index()

    return enrolment_wb


def melt_and_pivot(tb, id_vars: List[str], value_vars: List[str], index_vars: List[str], columns_vars: List[str]):
    """
    Melt the dataframe to long format and then pivot it to wide format.

    Parameters:
    tb: Original table to process.
    id_vars (List[str]): Identifier variables for melting.
    value_vars (List[str]): Column(s) to unpivot.
    index_vars (List[str]): Column(s) to set as index for pivoting.
    columns_vars (List[str]): Column(s) to pivot on.

    Returns:
    DataFrame: Processed dataframe.
    """
    tb_melted = tb.melt(id_vars=id_vars, value_vars=value_vars, var_name="measurement", value_name="value")
    tb_pivot = tb_melted.pivot(index=index_vars, columns=columns_vars, values="value")

    return tb_pivot


def flatten_columns(tb):
    """
    Flatten hierarchical columns in dataframe.

    Parameters:
    tb: Table with multi-level columns.

    Returns:
    tb: Table with flattened columns.
    """
    tb.columns = ["_".join(col).strip() for col in tb.columns.values]
    tb = tb.reset_index()

    return tb


def prepare_enrollment_data(tb):
    """
    Prepare the table by selecting and processing enrollment columns. This function extracts the enrollment data
    related to primary, secondary, and tertiary education levels. The data is melted and pivoted to create a clean
    table with the desired structure.

    Parameters:
    tb: Original table to process, which must contain columns for country, year, sex, and enrollment
                       rates for primary, secondary, and tertiary levels.

    Returns:
    tb: Processed table with enrollment data.
    """
    id_vars_enrollment = ["country", "year", "sex"]
    enrollment_columns = ["primary_enrollment_rates", "secondary_enrollment_rates", "tertiary_enrollment_rates"]
    tb_enrollment = tb[enrollment_columns + id_vars_enrollment]
    tb_enrollment = tb_enrollment.dropna(subset=enrollment_columns, how="all")
    tb_enrollment.reset_index(drop=True, inplace=True)

    tb_pivot_enrollment = melt_and_pivot(
        tb_enrollment, id_vars_enrollment, enrollment_columns, ["country", "year"], ["sex", "measurement"]
    )
    tb_pivot_enrollment = flatten_columns(tb_pivot_enrollment)

    return tb_pivot_enrollment


def prepare_attainment_data(tb):
    """
    Prepare the table by selecting and processing attainment-related columns. This function extracts the
    attainment data for different education levels, including the percentages of attainment and average years
    of education for primary, secondary, and tertiary levels. The data is then melted and pivoted to create a
    clean DataFrame with the desired structure.

    Parameters:
    tb: Original table to process, which must contain columns for country, year, sex, age_group,
                       and the specified attainment-related columns such as percentages of no education, primary,
                       secondary, tertiary education, etc.

    Returns:
    tb: Processed table with attainment data.
    """
    id_vars_attainment = ["country", "year", "sex", "age_group"]
    attainment_columns = [
        "percentage_of_no_education",
        "percentage_of_primary_education",
        "percentage_of_complete_primary_education_attained",
        "percentage_of_secondary_education",
        "percentage_of_complete_secondary_education_attained",
        "percentage_of_tertiary_education",
        "percentage_of_complete_tertiary_education_attained",
        "average_years_of_education",
        "average_years_of_primary_education",
        "average_years_of_secondary_education",
        "average_years_of_tertiary_education",
        "population__thousands",
    ]

    tb_pivot = melt_and_pivot(
        tb[id_vars_attainment + attainment_columns],
        id_vars_attainment,
        attainment_columns,
        ["country", "year"],
        ["sex", "age_group", "measurement"],
    )

    tb_pivot = flatten_columns(tb_pivot)

    cols_to_drop = [col for col in tb_pivot.columns if "Age not specified" in col]
    tb_pivot = tb_pivot.drop(columns=cols_to_drop)

    return tb_pivot
