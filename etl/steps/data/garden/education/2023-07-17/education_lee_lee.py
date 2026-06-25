"""Load a meadow dataset and create a garden dataset."""

import shared
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
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
            ds_population=paths.load_dependency("population"),
        )
    return tb_with_regions


def run() -> None:
    #
    # Load input datasets
    #
    # Read the main education table from the meadow dataset and reset its index
    ds_meadow = paths.load_dataset("education_lee_lee")
    tb = ds_meadow["education_lee_lee"].reset_index()

    # Load dataset containing regions data.
    ds_regions = paths.load_dataset("regions")

    #
    # Data Processing
    #
    # Harmonize country names
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Replace age group values with descriptive labels
    tb["age_group"] = (
        tb["age_group"]
        .astype(str)
        .replace(
            {
                "15.0-64.0": "Youth and Adults (15-64 years)",
                "15.0-24.0": "Youth (15-24 years)",
                "25.0-64.0": "Adults (25-64 years)",
                "not specified": "Age not specified",
            }
        )
    )

    # Prepare attainment data (enrollment combination has moved to education_opri)
    tb_attainment = prepare_attainment_data(tb)
    tb_attainment = tb_attainment.dropna(subset=tb_attainment.columns.drop(["country", "year"]), how="all")

    # Drop columns related to historic population values
    tb_attainment = tb_attainment.drop(columns=[column for column in tb_attainment.columns if "__thousands" in column])
    tb_attainment.columns = [underscore(col) for col in tb_attainment.columns]

    tb_attainment = add_data_for_regions(tb=tb_attainment, ds_regions=ds_regions)
    tb_attainment = tb_attainment.dropna(how="all")

    # Set metadata and format the dataframe for saving.
    tb_attainment.metadata.short_name = paths.short_name
    tb_attainment = tb_attainment.underscore().set_index(["country", "year"], verify_integrity=True)

    columns_to_use = [
        "mf_youth_and_adults__15_64_years__percentage_of_no_education",
        "f_youth_and_adults__15_64_years__percentage_of_no_education",
        "f_adults__25_64_years__percentage_of_no_education",
        "mf_adults__25_64_years__percentage_of_tertiary_education",
        "mf_youth_and_adults__15_64_years__average_years_of_education",
        "f_youth_and_adults__15_64_years__average_years_of_education",
    ]
    tb_attainment = tb_attainment[columns_to_use]

    # Prepare enrollment data with regional aggregates (used by education_opri for combined historical series)
    tb_enrollment = prepare_enrollment_data(tb)
    tb_enrollment = tb_enrollment.dropna(subset=tb_enrollment.columns.drop(["country", "year"]), how="all")
    tb_enrollment = add_data_for_regions(tb=tb_enrollment, ds_regions=ds_regions)
    tb_enrollment = tb_enrollment.dropna(how="all")
    tb_enrollment.metadata.short_name = "education_lee_lee_enrollment"
    tb_enrollment = tb_enrollment.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs
    #
    # Create a new garden dataset with the same metadata as the meadow dataset
    ds_garden = paths.create_dataset(tables=[tb_attainment, tb_enrollment], check_variables_metadata=True)

    # Save the processed data in the new garden dataset
    ds_garden.save()


def melt_and_pivot(tb, id_vars: list[str], value_vars: list[str], index_vars: list[str], columns_vars: list[str]):
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
