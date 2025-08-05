"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mortality_database_cancer")
    tb = ds_meadow.read("mortality_database_cancer", safe_types=False)

    #
    # Process data.
    #
    tb = tidy_sex_dimension(tb)
    tb = tidy_age_dimension(tb)
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Calculate death rates for  combined age groups.
    # The death rate is per 100,000 population, so we reverse-calculate the population size.
    tb["estimated_population"] = tb["number"] / tb["death_rate_per_100_000_population"] * 100000
    tb = add_age_group_aggregate(tb, ["less than 1 year", "1-4 years"], "< 5 years")
    tb = add_age_group_aggregate(tb, ["less than 1 year", "1-4 years", "5-9 years"], "< 10 years")
    tb = tb.drop(columns=["estimated_population"])

    tb = tb.format(["country", "year", "sex", "age_group", "cause", "icd10_codes"])
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=False, default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()


def add_age_group_aggregate(tb: Table, age_groups: list[str], label: str) -> Table:
    """
    Aggregates death numbers and recalculates death rates for a combined age group.

    Parameters:
    - tb (Table): Original table with disaggregated age group data.
    - age_groups (list of str): List of age group labels to combine (e.g., ["less than 1 year", "1-4 years"]).
    - label (str): New age group label to assign to the aggregated rows (e.g., "< 5 years").

    Returns:
    - Table: Aggregated rows with updated death rate and age group label merged into the original table.
    """
    # Filter relevant age groups
    tb_filtered = tb[tb["age_group"].isin(age_groups)].copy()

    # Group by relevant dimensions and sum values
    tb_filtered = tb_filtered.groupby(["country", "year", "sex", "cause", "icd10_codes"], as_index=False).agg(
        {"number": "sum", "estimated_population": "sum"}
    )

    # Recalculate the death rate for the new age group
    tb_filtered["death_rate_per_100_000_population"] = (
        tb_filtered["number"] / tb_filtered["estimated_population"] * 100000
    )

    # Assign new age group label
    tb_filtered["age_group"] = label

    # Drop the helper column
    tb = pr.concat([tb, tb_filtered])
    return tb


def tidy_sex_dimension(tb: Table) -> Table:
    """
    Improve the labelling of the sex column
    """
    sex_dict = {"All": "Both sexes", "Female": "Females", "Male": "Males", "Unknown": "Unknown sex"}
    tb["sex"] = tb["sex"].replace(sex_dict)
    return tb


def tidy_age_dimension(tb: Table) -> Table:
    age_dict = {
        "[Unknown]": "Unknown age",
        "[85+]": "over 85 years",
        "[80-84]": "80-84 years",
        "[75-79]": "75-79 years",
        "[70-74]": "70-74 years",
        "[65-69]": "65-69 years",
        "[60-64]": "60-64 years",
        "[55-59]": "55-59 years",
        "[50-54]": "50-54 years",
        "[45-49]": "45-49 years",
        "[40-44]": "40-44 years",
        "[35-39]": "35-39 years",
        "[30-34]": "30-34 years",
        "[25-29]": "25-29 years",
        "[20-24]": "20-24 years",
        "[15-19]": "15-19 years",
        "[10-14]": "10-14 years",
        "[5-9]": "5-9 years",
        "[1-4]": "1-4 years",
        "[0]": "less than 1 year",
        "[All]": "all ages",
    }

    tb["age_group"] = tb["age_group"].replace(age_dict)
    return tb
