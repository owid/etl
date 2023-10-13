"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import pandas as pd
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


def add_data_for_regions(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb_with_regions = tb.copy()
    aggregations = {column: "mean" for column in tb_with_regions.columns if column not in ["country", "year"]}

    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
        )
        tb_with_regions = shared.add_region_aggregates_education(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.2,
            aggregations=aggregations,
        )
    return tb_with_regions


def run(dest_dir: str) -> None:
    #
    # Load input datasets
    #
    # Load the Lee meadow dataset
    ds_meadow = cast(Dataset, paths.load_dependency("education_lee_lee"))

    # Load additional datasets for region and income group information
    ds_regions = paths.load_dependency("regions")
    ds_income_groups = paths.load_dependency("income_groups")

    # Load the World Bank Education Dataset
    ds_garden_wdi = cast(Dataset, paths.load_dependency("wdi"))
    tb_wdi = ds_garden_wdi["wdi"]

    # Extract enrollment rates from the World Bank Education Dataset starting from 2010
    enrolment_wb = extract_related_world_bank_data(tb_wdi)
    world_bank_indicators = enrolment_wb.columns

    # Read the main education table from the meadow dataset and reset its index
    tb = ds_meadow["education_lee_lee"]
    tb.reset_index(inplace=True)
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
    df_enrollment = prepare_enrollment_data(tb)
    df_attainment = prepare_attainment_data(tb)
    df_attainment = df_attainment.dropna(subset=df_attainment.columns.drop(["country", "year"]), how="all")
    df_attainment.reset_index(drop=True, inplace=True)

    # Merge enrollment and attainment data
    merged_df = pd.merge(df_enrollment, df_attainment, on=["year", "country"], how="outer")

    # Drop columns related to historic population values
    merged_df = merged_df.drop(columns=[column for column in merged_df.columns if "__thousands" in column])
    merged_df.columns = [underscore(col) for col in merged_df.columns]
    merged_df = add_data_for_regions(tb=merged_df, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Concatenate historical and more recent enrollment data
    hist_1985_df = merged_df[merged_df["year"] < 1985]
    df_merged_enrollment = pd.concat([enrolment_wb, hist_1985_df[world_bank_indicators]])
    df_merged_enrollment.set_index(["country", "year"], inplace=True)
    # Differentiate these columns from the original data
    df_merged_enrollment.columns = df_merged_enrollment.columns + "_combined_wb"

    # Merge historical data with combined enrollment data
    df_merged_wb = pd.merge(df_merged_enrollment, merged_df, on=["country", "year"], how="outer")
    df_merged_wb = df_merged_wb.dropna(how="all")

    # Create female to male enrollment ratios
    df_merged_wb["female_over_male_enrollment_rates_primary"] = (
        df_merged_wb["f_primary_enrollment_rates_combined_wb"] / df_merged_wb["m_primary_enrollment_rates_combined_wb"]
    )
    df_merged_wb["female_over_male_enrollment_rates_secondary"] = (
        df_merged_wb["f_secondary_enrollment_rates_combined_wb"]
        / df_merged_wb["m_secondary_enrollment_rates_combined_wb"]
    )
    df_merged_wb["female_over_male_enrollment_rates_tertiary"] = (
        df_merged_wb["f_tertiary_enrollment_rates_combined_wb"]
        / df_merged_wb["m_tertiary_enrollment_rates_combined_wb"]
    )

    tb = Table(df_merged_wb, short_name=paths.short_name, underscore=True)
    tb.set_index(["country", "year"], inplace=True)

    #
    # Save outputs
    #
    # Create a new garden dataset with the same metadata as the meadow dataset
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save the processed data in the new garden dataset
    ds_garden.save()


def extract_related_world_bank_data(tb_wb: Table) -> pd.DataFrame:
    """
    Extracts enrollment rate indicators from the WD Bank dataset.
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
    enrolment_wb.rename(columns=dictionary_to_rename_and_combine, inplace=True)

    # Select data above 1985
    enrolment_wb = enrolment_wb[(enrolment_wb.index.get_level_values("year") >= 1985)]
    enrolment_wb.reset_index(inplace=True)

    return enrolment_wb


def melt_and_pivot(
    df: pd.DataFrame, id_vars: List[str], value_vars: List[str], index_vars: List[str], columns_vars: List[str]
) -> pd.DataFrame:
    """
    Melt the dataframe to long format and then pivot it to wide format.

    Parameters:
    df (DataFrame): Original dataframe to process.
    id_vars (List[str]): Identifier variables for melting.
    value_vars (List[str]): Column(s) to unpivot.
    index_vars (List[str]): Column(s) to set as index for pivoting.
    columns_vars (List[str]): Column(s) to pivot on.

    Returns:
    DataFrame: Processed dataframe.
    """
    df_melted = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="measurement", value_name="value")
    df_pivot = df_melted.pivot(index=index_vars, columns=columns_vars, values="value")

    return df_pivot


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten hierarchical columns in dataframe.

    Parameters:
    df (DataFrame): Dataframe with multi-level columns.

    Returns:
    DataFrame: Dataframe with flattened columns.
    """
    df.columns = ["_".join(col).strip() for col in df.columns.values]
    df = df.reset_index()

    return df


def prepare_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the DataFrame by selecting and processing enrollment columns. This function extracts the enrollment data
    related to primary, secondary, and tertiary education levels. The data is melted and pivoted to create a clean
    DataFrame with the desired structure.

    Parameters:
    df (pd.DataFrame): Original DataFrame to process, which must contain columns for country, year, sex, and enrollment
                       rates for primary, secondary, and tertiary levels.

    Returns:
    pd.DataFrame: Processed DataFrame with enrollment data.
    """
    id_vars_enrollment = ["country", "year", "sex"]
    enrollment_columns = ["primary_enrollment_rates", "secondary_enrollment_rates", "tertiary_enrollment_rates"]
    df_enrollment = df[enrollment_columns + id_vars_enrollment]
    df_enrollment = df_enrollment.dropna(subset=enrollment_columns, how="all")
    df_enrollment.reset_index(drop=True, inplace=True)

    df_pivot_enrollment = melt_and_pivot(
        df_enrollment, id_vars_enrollment, enrollment_columns, ["country", "year"], ["sex", "measurement"]
    )
    df_pivot_enrollment = flatten_columns(df_pivot_enrollment)

    return df_pivot_enrollment


def prepare_attainment_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the DataFrame by selecting and processing attainment-related columns. This function extracts the
    attainment data for different education levels, including the percentages of attainment and average years
    of education for primary, secondary, and tertiary levels. The data is then melted and pivoted to create a
    clean DataFrame with the desired structure.

    Parameters:
    df (pd.DataFrame): Original DataFrame to process, which must contain columns for country, year, sex, age_group,
                       and the specified attainment-related columns such as percentages of no education, primary,
                       secondary, tertiary education, etc.

    Returns:
    pd.DataFrame: Processed DataFrame with attainment data.
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

    df_pivot = melt_and_pivot(
        df[id_vars_attainment + attainment_columns],
        id_vars_attainment,
        attainment_columns,
        ["country", "year"],
        ["sex", "age_group", "measurement"],
    )

    df_pivot = flatten_columns(df_pivot)

    cols_to_drop = [col for col in df_pivot.columns if "Age not specified" in col]
    df_pivot = df_pivot.drop(columns=cols_to_drop)

    return df_pivot
