"""Load a meadow dataset and create a garden dataset."""

import html2text
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Define release year as the year in the version
RELEASE_YEAR = int(paths.version.split("-")[0])

# Define threshold for percentage of unreliable data
UNRELIABLE_THRESHOLD = 1

# Define columns to keep
COLUMNS_TO_KEEP = [
    "ref_area",
    "time",
    "indicator",
    "sex",
    "classif1",
    "classif2",
    "obs_value",
    "obs_status",
]

# Redefine column categories from the original dataset
COLUMN_CATEGORIES = {
    "indicator": {
        "SDG indicator 1.1.1 - Working poverty rate (percentage of employed living below US$2.15 PPP) (%)": "sdg_1_1_1_working_poverty_rate",
        "SDG indicator 1.3.1 - Proportion of population covered by social protection floors/systems (%)": "sdg_1_3_1_population_covered_by_social_protection",
        "SDG indicator 5.5.2 - Proportion of women in senior and middle management positions (%)": "sdg_5_5_2_women_in_senior_middle_management",
        "SDG indicator 5.5.2 - Proportion of women in managerial positions (%)": "sdg_5_5_2_women_in_management",
        "SDG indicator 8.2.1 - Annual growth rate of output per worker (GDP constant 2021 international $ at PPP) (%)": "sdg_8_2_1_growth_rate_output_per_worker",
        "SDG indicator 8.3.1 - Proportion of informal employment in total employment by sex and sector (%)": "sdg_8_3_1_informal_employment",
        "SDG indicator 8.5.1 - Average hourly earnings of employees by sex (Local currency)": "sdg_8_5_1_average_hourly_earnings",
        "SDG indicator 8.5.2 - Unemployment rate (%)": "sdg_8_5_2_unemployment_rate",
        "SDG indicator 8.5.2 - Unemployment rate by disability status (%)": "sdg_8_5_2_unemployment_rate_by_disability_status",
        "SDG indicator 8.6.1 - Proportion of youth (aged 15-24 years) not in education, employment or training": "sdg_8_6_1_youth_neet",
        "SDG indicator 8.7.1 - Proportion of children engaged in economic activity (%)": "sdg_8_7_1_children_engaged_in_economic_activity",
        "SDG indicator 8.7.1 - Proportion of children engaged in economic activity and household chores": "sdg_8_7_1_children_engaged_in_economic_activity_and_household_chores",
        "SDG indicator 8.8.1 - Fatal occupational injuries per 100'000 workers": "sdg_8_8_1_fatal_occupational_injuries",
        "SDG indicator 8.8.1 - Non-fatal occupational injuries per 100'000 workers": "sdg_8_8_1_non_fatal_occupational_injuries",
        "SDG indicator 8.8.2 - Level of national compliance with labour rights (freedom of association and collective bargaining)": "sdg_8_8_2_national_compliance_labour_rights",
        "SDG indicator 8.b.1: Existence of a developed and operationalized national strategy for youth employment": "sdg_8_b_1_national_strategy_youth_employment",
        "SDG indicator 9.2.2 - Manufacturing employment as a proportion of total employment (%)": "sdg_9_2_2_manufacturing_employment",
        "SDG indicator 10.4.1 - Labour income share as a percent of GDP (%)": "sdg_10_4_1_labour_income_share",
        "Average hourly earnings of employees by sex": "average_hourly_earnings_employees_by_sex",
        "Gender wage gap by occupation (%)": "gender_wage_gap_by_occupation",
        "Share of children in child labour by sex and age (%)": "share_of_children_in_child_labour_by_sex_and_age",
        "Female share of low pay earners (%)": "female_share_of_low_pay_earners",
        "Labour force by sex and age -- ILO modelled estimates, Nov. 2024 (thousands)": "labour_force_by_sex_and_age",
        "Labour force participation rate by sex and age -- ILO modelled estimates, Nov. 2024 (%)": "labour_force_participation_rate_by_sex_and_age",
        "Employment by sex and status in employment -- ILO modelled estimates, Nov. 2024 (thousands)": "employment_by_sex_and_status_in_employment",
        "Unemployment rate by sex and age -- ILO modelled estimates, Nov. 2024 (%)": "unemployment_rate_by_sex_and_age",
        "Informal employment rate by sex -- ILO modelled estimates, Nov. 2024 (%)": "informal_employment_rate_by_sex",
    },
    "sex": {
        "Total": "Total",
        "Male": "Male",
        "Female": "Female",
        # "Other": "Other",
        pd.NA: pd.NA,
    },
    "classif1": {
        "Age (Aggregate bands): 15-24": "Age (aggregate): 15-24",
        "Age (Aggregate bands): 25-54": "Age (aggregate): 25-54",
        "Age (Aggregate bands): 55-64": "Age (aggregate): 55-64",
        "Age (Aggregate bands): 65+": "Age (aggregate): 65+",
        "Age (Aggregate bands): Total": "Age (aggregate): Total",
        "Age (Child labour bands): '5-11": "Age (child labour): 5-11",
        "Age (Child labour bands): '5-17": "Age (child labour): 5-17",
        "Age (Child labour bands): 12-14": "Age (child labour): 12-14",
        "Age (Child labour bands): 15-17": "Age (child labour): 15-17",
        "Age (Youth, adults): 15+": "Age (youth, adults): 15+",
        "Age (Youth, adults): 15-24": "Age (youth, adults): 15-24",
        "Age (Youth, adults): 15-64": "Age (youth, adults): 15-64",
        "Age (Youth, adults): 25+": "Age (youth, adults): 25+",
        "Contingency: Children/households receiving child/family cash benefits": "Contingency: Households receiving child/family cash benefits",
        "Contingency: Employed covered in the event of work injury": "Contingency: Employed covered in the event of work injury",
        "Contingency: Mothers with newborns receiving maternity benefits": "Contingency: Mothers with newborns receiving maternity benefits",
        "Contingency: Persons above retirement age receiving a pension": "Contingency: Persons above retirement age receiving a pension",
        "Contingency: Poor persons covered by social protection systems": "Contingency: Poor persons covered by social protection systems",
        "Contingency: Population covered by at least one social protection benefit": "Contingency: Population covered by at least one social protection benefit",
        "Contingency: Unemployed receiving unemployment benefits": "Contingency: Unemployed receiving unemployment benefits",
        "Contingency: Vulnerable persons covered by social assistance": "Contingency: Vulnerable persons covered by social assistance",
        "Contingency: Persons with severe disabilities collecting disability social protection benefits": "Contingency: Persons with severe disabilities collecting disability social protection benefits",
        "Currency: 2021 PPP $": "Currency: 2021 PPP $",
        # "Currency: Local currency": "Currency: Local currency",
        # "Currency: U.S. dollars": "Currency: U.S. dollars",
        "Disability status (Aggregate): Persons with disability": "Disability status: Persons with disability",
        "Disability status (Aggregate): Persons without disability": "Disability status: Persons without disability",
        "Disability status (Aggregate): Not elsewhere classified": "Disability status: Not elsewhere classified",
        "Economic activity (Aggregate): Total": "Economic activity: Total",
        "Economic activity (Aggregate): Agriculture": "Economic activity (aggregate): Agriculture",
        "Economic activity (Aggregate): Construction": "Economic activity (aggregate): Construction",
        "Economic activity (Aggregate): Manufacturing": "Economic activity (aggregate): Manufacturing",
        "Economic activity (Aggregate): Not classified": "Economic activity (aggregate): Not classified",
        "Economic activity (Aggregate): Trade, Transportation, Accommodation and Food, and Business and Administrative Services": "Economic activity (aggregate): Trade, Transportation, Accommodation and Food, and Business and Administrative Services",
        "Economic activity (Agriculture, Non-Agriculture): Agriculture": "Economic activity (agriculture vs. non-agriculture): Agriculture",
        "Economic activity (Agriculture, Non-Agriculture): Non-agriculture": "Economic activity (agriculture vs. non-agriculture): Non-agriculture",
        "Economic activity (Broad sector): Agriculture": "Economic activity (broad): Agriculture",
        "Economic activity (Broad sector): Industry": "Economic activity (broad): Industry",
        "Economic activity (Broad sector): Services": "Economic activity (broad): Services",
        "Migrant status: Migrants": "Migrant status: Migrants",
        "Migrant status: Non migrants": "Migrant status: Non migrants",
        "Migrant status: Total": "Migrant status: Total",
        "Occupation (ISCO-08): 0. Armed forces occupations": "Occupation (ISCO-08): 0. Armed forces occupations",
        "Occupation (ISCO-08): 1. Managers": "Occupation (ISCO-08): 1. Managers",
        "Occupation (ISCO-08): 2. Professionals": "Occupation (ISCO-08): 2. Professionals",
        "Occupation (ISCO-08): 3. Technicians and associate professionals": "Occupation (ISCO-08): 3. Technicians and associate professionals",
        "Occupation (ISCO-08): 4. Clerical support workers": "Occupation (ISCO-08): 4. Clerical support workers",
        "Occupation (ISCO-08): 5. Service and sales workers": "Occupation (ISCO-08): 5. Service and sales workers",
        "Occupation (ISCO-08): 6. Skilled agricultural, forestry and fishery workers": "Occupation (ISCO-08): 6. Skilled agricultural, forestry and fishery workers",
        "Occupation (ISCO-08): 7. Craft and related trades workers": "Occupation (ISCO-08): 7. Craft and related trades workers",
        "Occupation (ISCO-08): 8. Plant and machine operators, and assemblers": "Occupation (ISCO-08): 8. Plant and machine operators, and assemblers",
        "Occupation (ISCO-08): 9. Elementary occupations": "Occupation (ISCO-08): 9. Elementary occupations",
        "Occupation (ISCO-08): X. Not elsewhere classified": "Occupation (ISCO-08): X. Not elsewhere classified",
        "Occupation (ISCO-08): Total": "Occupation (ISCO-08): Total",
        "Occupation (ISCO-88): 0. Armed forces": "Occupation (ISCO-88): 0. Armed forces",
        "Occupation (ISCO-88): 1. Legislators, senior officials and managers": "Occupation (ISCO-88): 1. Legislators, senior officials and managers",
        "Occupation (ISCO-88): 2. Professionals": "Occupation (ISCO-88): 2. Professionals",
        "Occupation (ISCO-88): 3. Technicians and associate professionals": "Occupation (ISCO-88): 3. Technicians and associate professionals",
        "Occupation (ISCO-88): 4. Clerks": "Occupation (ISCO-88): 4. Clerks",
        "Occupation (ISCO-88): 5. Service workers and shop and market sales workers": "Occupation (ISCO-88): 5. Service workers and shop and market sales workers",
        "Occupation (ISCO-88): 6. Skilled agricultural and fishery workers": "Occupation (ISCO-88): 6. Skilled agricultural and fishery workers",
        "Occupation (ISCO-88): 7. Craft and related trades workers": "Occupation (ISCO-88): 7. Craft and related trades workers",
        "Occupation (ISCO-88): 8. Plant and machine operators and assemblers": "Occupation (ISCO-88): 8. Plant and machine operators and assemblers",
        "Occupation (ISCO-88): 9. Elementary occupations": "Occupation (ISCO-88): 9. Elementary occupations",
        "Occupation (ISCO-88): X. Not elsewhere classified": "Occupation (ISCO-88): X. Not elsewhere classified",
        "Occupation (ISCO-88): Total": "Occupation (ISCO-88): Total",
        "Occupation (Skill level): Skill level 1 ~ low": "Occupation (Skill level): Low",
        "Occupation (Skill level): Skill level 2 ~ medium": "Occupation (Skill level): Medium",
        "Occupation (Skill level): Skill levels 3 and 4 ~ high": "Occupation (Skill level): High",
        "Occupation (Skill level): Not elsewhere classified": "Occupation (Skill level): Not elsewhere classified",
        "Occupation (Skill level): Total": "Occupation (Skill level): Total",
        "Status in employment (Aggregate): Self-employed": "Status in employment (aggregate): Self-employed",
        "Status in employment (Aggregate): Total": "Status in employment (aggregate): Total",
        "Status in employment (ICSE-93): 1. Employees": "Status in employment (ICSE-93): 1. Employees",
        "Status in employment (ICSE-93): 2. Employers": "Status in employment (ICSE-93): 2. Employers",
        "Status in employment (ICSE-93): 3. Own-account workers": "Status in employment (ICSE-93): 3. Own-account workers",
        "Status in employment (ICSE-93): 5. Contributing family workers": "Status in employment (ICSE-93): 5. Contributing family workers",
        "Status in employment (ICSE-93): Total": "Status in employment (ICSE-93): Total",
        pd.NA: pd.NA,
    },
    "obs_status": {
        "Real value": "Real value",
        "Adjusted": "Adjusted",
        "Imputation": "Imputation",
        "Break in series": "Break in series",
        # "Unreliable": "Unreliable",
        pd.NA: pd.NA,
    },
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_ilostat = paths.load_dataset("ilostat")

    # Read table from meadow dataset.
    tb = ds_ilostat.read("ilostat", safe_types=False)
    tb_regions = ds_ilostat.read("table_of_contents_country")
    tb_indicator = ds_ilostat.read("dictionary_indicator")
    tb_sex = ds_ilostat.read("dictionary_sex")
    tb_classif1 = ds_ilostat.read("dictionary_classif1")
    tb_classif2 = ds_ilostat.read("dictionary_classif2")
    tb_obs_status = ds_ilostat.read("dictionary_obs_status")

    #
    # Process data.
    #

    # Keep relevant columns
    tb = tb[COLUMNS_TO_KEEP]

    # Make indicator_description in Markdown format instead of HTML
    tb_indicator["indicator_description"] = tb_indicator["indicator_description"].apply(html_to_markdown)

    tb = add_indicator_metadata(tb=tb, tb_metadata=tb_indicator, column="indicator")
    tb = add_indicator_metadata(tb=tb, tb_metadata=tb_sex, column="sex")
    tb = add_indicator_metadata(tb=tb, tb_metadata=tb_classif1, column="classif1")
    tb = add_indicator_metadata(tb=tb, tb_metadata=tb_classif2, column="classif2")
    tb = add_indicator_metadata(tb=tb, tb_metadata=tb_obs_status, column="obs_status")

    tb = make_table_wide(tb=tb)

    tb_regions = format_ilo_regions(tb_regions=tb_regions)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )
    tb_regions = geo.harmonize_countries(
        df=tb_regions, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Improve table format.
    tb = tb.format(["country", "year", "sex", "classif1"])
    tb_regions = tb_regions.format(["country", "year"], short_name="regions")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_regions], default_metadata=ds_ilostat.metadata)

    # Save garden dataset.
    ds_garden.save()


def format_ilo_regions(tb_regions: Table) -> Table:
    """
    Format the ILO regions table.
    """

    tb_regions = tb_regions.copy()

    # Filter freq
    tb_regions = tb_regions[tb_regions["freq"] == "A"].reset_index(drop=True)

    # Keep relevant columns
    tb_regions = tb_regions[["ref_area", "ilo_region_label", "ilo_subregion_detailed_label"]]

    # Rename columns
    tb_regions = tb_regions.rename(
        columns={
            "ref_area": "country",
            "ilo_region_label": "ilo_region",
            "ilo_subregion_detailed_label": "ilo_subregion",
        },
        errors="raise",
    )

    # Add year column as RELEASE_YEAR
    tb_regions["year"] = RELEASE_YEAR

    return tb_regions


def add_indicator_metadata(tb: Table, tb_metadata: Table, column: str) -> Table:
    """
    Add column metadata to the table.
    """

    tb = tb.copy()

    # Skip and drop if there are no values in column
    if tb[column].isnull().all():
        tb = tb.drop(columns=[column], errors="raise")
        return tb

    if column == "obs_status":
        # Count the % of obs_status that are U, from the total (including nulls)
        pct_u = (tb[column] == "U").sum() / len(tb[column]) * 100
        message = f"{pct_u:.2f}% of the observations are 'U', unreliable"
        if pct_u > UNRELIABLE_THRESHOLD:
            if "Unreliable" in COLUMN_CATEGORIES[column].keys():
                log.warning(f"{message}, but we are keeping them")
            else:
                log.warning(f"{message}, and we are dropping them")

    # Assert that there is info for every column
    assert set(
        tb[column].dropna().unique()
    ).issubset(
        set(tb_metadata[column].unique())
    ), f"Some values are missing in the {column} metadata table: {set(tb[column].dropna().unique()) ^ set(tb_metadata[column].dropna().unique())}"

    tb = pr.merge(tb, tb_metadata, on=column, how="left")

    # Drop column column
    tb = tb.drop(columns=[column], errors="raise")

    # Rename column_label to column
    tb = tb.rename(columns={f"{column}_label": column}, errors="raise")

    # Filter tb by the values in COLUMN_CATEGORIES[column].keys()
    tb = tb[tb[column].isin(COLUMN_CATEGORIES[column].keys())].reset_index(drop=True)

    # Assert that all renamed columns are the same as COLUMN_CATEGORIES[column].keys()
    assert (
        set(tb[column].unique()) == set(COLUMN_CATEGORIES[column].keys())
    ), f"Some {column} are missing in the column categories mapping: {set(tb[column].unique()) ^ set(COLUMN_CATEGORIES[column].keys())}"

    if column == "indicator":
        # Multiply observations by 1000 when the indicator is in thousands
        tb.loc[tb[column].str.contains("(thousands)", case=False, na=False, regex=False), "obs_value"] *= 1000

    # Rename categories in column
    tb[column] = tb[column].replace(COLUMN_CATEGORIES[column])

    return tb


def make_table_wide(tb: Table) -> Table:
    """
    Make the table wide by pivoting the indicators.
    """

    tb = tb.copy()

    # Rename columns
    tb = tb.rename(
        columns={
            "ref_area": "country",
            "time": "year",
        },
        errors="raise",
    )

    # Build mapping from indicator to its description
    indicator_dict = (
        tb[["indicator", "indicator_description"]]
        .drop_duplicates()
        .set_index("indicator")["indicator_description"]
        .to_dict()
    )

    # Pivot the table
    tb = tb.pivot(
        index=[
            "country",
            "year",
            "sex",
            "classif1",
        ],
        columns=["indicator"],
        values=["obs_value"],
        join_column_levels_with="_",
    )

    # Remove "obs_value_" from the column names
    tb.columns = [col.replace("obs_value_", "") for col in tb.columns]

    # For each indicator column, add description_from_producer metadata
    for indicator in indicator_dict.keys():
        tb[indicator].metadata.description_from_producer = indicator_dict[indicator]

    return tb


def html_to_markdown(text: str) -> str:
    """Convert HTML text to Markdown using html2text."""
    if pd.isna(text):
        return pd.NA

    # Configure html2text
    h = html2text.HTML2Text()
    h.body_width = 0  # don't wrap lines
    h.ignore_images = True
    h.ignore_links = False
    h.single_line_break = False
    md = h.handle(text).strip()

    return md or pd.NA
