"""Load a meadow dataset and create a garden dataset."""

import re

import numpy as np
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("who")
    tb = ds_meadow["who"].reset_index()
    tb = tb.rename(columns={"name": "country"})
    tb = tb.drop(columns=["iso3"], axis=1)
    tb = drop_erroneous_rows(tb)
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # The population is given as 'population (thousands)
    tb["pop"] = tb["pop"].astype(float).multiply(1000)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=tb, check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def drop_erroneous_rows(tb: Table) -> Table:
    """
    The values for Kosovo are poorly formatted and the country name is given as a year. Let's remove those.
    """
    tb = tb[~tb["country"].str.startswith("2")]
    return tb


def clean_values(tb: Table) -> Table:
    """
    Some values are strings that begin with either < or >. These are not valid numbers, so we need to clean them by removing those non-numeric characters.
    Additionally, NAs are represented with - in the data, we need to replace those with NaNs.

    """
    # Replace - with NaNs.
    tb = tb.replace("-", np.nan)

    # Remove < and > from strings.
    tb = tb.applymap(lambda x: re.sub(r"[<>]", "", x) if isinstance(x, str) else x)

    # There is a strange column at the end of each table that is just the row number. We can drop this.

    tb = tb.drop(
        columns=[
            "year_survey_name",
            "pct_urban__of_total_population",
            "total_sl",
            "total_proportion_of_population_using__improved_water_supplies_sl",
            "total_proportion_of_population_using_improved__sanitation_facilities__including_shared__sl",
        ],
        axis=1,
        errors="ignore",  # not all tables have these columns
    )

    return tb


def drop_region_columns(tb: Table) -> Table:
    """
    Drop columns that contain region information.

    """
    columns_to_drop = [col for col in tb.columns if "region" in col.lower()]
    tb = tb.drop(columns_to_drop, axis=1)
    return tb


def calculate_population_with_each_category(tb: Table, table_name: str) -> Table:
    """
    For water, hygiene and sanitation calculate the population living with each category and also by urban/rural.

    For menstrual health we just calculate number of women (aged 15-49) in each category.

    """
    areas = ["urban", "rural", "total"]
    categories_water = ["at_least_basic", "limited__more_than_30_mins", "unimproved", "surface_water"]
    categories_hygiene = ["basic", "limited__without_water_or_soap", "no_facility"]
    categories_sanitation = ["at_least_basic", "limited__shared", "unimproved", "open_defecation"]

    if table_name in ["sanitation", "water", "hygiene"]:
        # Determine the appropriate list of categories
        if table_name == "water":
            categories = categories_water
        elif table_name == "hygiene":
            categories = categories_hygiene
        elif table_name == "sanitation":
            categories = categories_sanitation
        else:
            raise ValueError("Invalid table name")

        # Perform the calculation
        for area in areas:
            for category in categories:
                tb[f"population_{area}_{category}"] = (tb[f"{area}_{category}"].astype(float) / 100) * tb[
                    f"{area}_population"
                ]
            tb = tb.drop(columns=f"{area}_population", axis=1)
    elif table_name == "menstrual_health":
        categories_menstrual = [
            "awareness_of_menstruation_before_menarche",
            "private_place_to_wash_and_change",
            "participation_in_activities_during_menstruation",
            "use_of_menstrual_materials",
            "use_of_reusable_materials",
            "use_of_single_use_materials",
        ]
        for category in categories_menstrual:
            tb[f"population_women_age_15_49_{category}"] = (
                tb[
                    f"total_proportion_of_women_and_girls_age_15_49_who_have_menstruated_in_the_previous_year_{category}"
                ].astype(float)
                / 100
            ) * tb["population_women_age_15_49"]
        tb = tb.drop(columns="population_women_age_15_49", axis=1)
    return tb
