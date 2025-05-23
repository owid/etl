"""Process and aggregate WHO mortality data based on specified cause groupings to highlight the share of deaths from cardiovascular diseases vs other categories."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix for all causes
PREFIX = "share_of_total_deaths_in_both_sexes_aged_all_ages_years_that_are_from_"

# Define a constant mapping for aggregating specific causes of deaths into broader categories.
CAUSE_MAPPING = {
    # Congenital & Maternal Conditions
    "Congenital anomalies": "Congenital and maternal conditions",
    "Maternal conditions": "Congenital and maternal conditions",
    "Perinatal conditions": "Congenital and maternal conditions",
    "Sudden infant death syndrome": "Congenital and maternal conditions",
    # Endocrine & Nutritional Disorders
    "Diabetes mellitus, blood and endocrine disorders": "Endocrine and nutritional disorders",
    "Nutritional deficiencies": "Endocrine and nutritional disorders",
    # Digestive & Oral Health
    "Digestive diseases": "Digestive and oral health",
    "Oral conditions": "Digestive and oral health",
    # Ill-defined Causes & Injuries
    "Ill-defined diseases": "Ill-defined causes and injuries",
    "Ill-defined injuries": "Ill-defined causes and injuries",
    "Intentional injuries": "Ill-defined causes and injuries",
    "Unintentional injuries": "Ill-defined causes and injuries",
    # Infectious Diseases
    "Infectious and parasitic diseases": "Infectious diseases",
    "Respiratory infections": "Infectious diseases",
    # Cancer & Neoplasms
    "Malignant neoplasms": "Cancer and neoplasms",
    "Other neoplasms": "Cancer and neoplasms",
    # Other Diseases & Conditions
    "Sense organ diseases": "Other diseases and conditions",
    "Musculoskeletal diseases": "Other diseases and conditions",
    "Skin diseases": "Other diseases and conditions",
    # Neurological & Mental Health
    "Neuropsychiatric conditions": "Neurological and mental health",
    # Respiratory and CVD Diseases
    "Respiratory diseases": "Respiratory diseases",
    "Cardiovascular diseases": "Cardiovascular diseases",
}


def run(dest_dir: str) -> None:
    # Load the 'mortality_database' dataset.
    ds_garden_who = paths.load_dataset("mortality_database")

    tb = ds_garden_who["mortality_database"].reset_index()
    # Grab the origins for reuse later
    origins = tb["number"].metadata.origins
    tb = tb[(tb["cause"].isin(CAUSE_MAPPING.keys())) & (tb["age_group"] == "all ages") & (tb["sex"] == "Both sexes")]
    # Assert all the causes are included
    assert len(tb["cause"].unique()) == len(
        set(CAUSE_MAPPING.keys())
    ), "Not all causes are included in the table, check spelling."

    tb = tb[["country", "year", "cause", "percentage_of_cause_specific_deaths_out_of_total_deaths"]]

    tb = tb.replace({"cause": CAUSE_MAPPING})
    # Group by country and year and sum the percentage of deaths for each cause.
    tb = tb.groupby(["country", "year", "cause"], observed=True).sum().reset_index()

    # Some countries don't sum to 100% - compute the percentage of deaths with unidentified causes.
    tb_group = (
        tb.groupby(["country", "year"], observed=True)["percentage_of_cause_specific_deaths_out_of_total_deaths"]
        .sum()
        .reset_index()
    )
    tb_group["Unidentified causes"] = 100 - tb_group["percentage_of_cause_specific_deaths_out_of_total_deaths"]
    tb_group["cause"] = "Unidentified causes"
    tb_group = tb_group[["country", "year", "cause", "Unidentified causes"]]
    tb_group = tb_group.rename(
        columns={"Unidentified causes": "percentage_of_cause_specific_deaths_out_of_total_deaths"}
    )
    # Append the unidentified causes to the main table.
    tb = pr.concat([tb, tb_group])
    # Pivot the table to have causes as columns
    tb = tb.pivot_table(
        index=["country", "year"],
        columns="cause",
        values="percentage_of_cause_specific_deaths_out_of_total_deaths",
        observed=True,
    ).reset_index()

    tb = tb.format(["country", "year"], short_name="deaths_from_cardiovascular_diseases_vs_other")
    # Add the origins back in
    for col in tb.columns:
        tb[col].metadata.origins = origins
    # Save the processed data.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
