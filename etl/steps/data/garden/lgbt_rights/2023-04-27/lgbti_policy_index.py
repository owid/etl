"""Load a meadow dataset and create a garden dataset."""

from typing import List

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2

# Define categories for merged columns and their new names
CATEGORIES_RENAMING = {
    "age_of_consent": {
        "equal: 1 unequal: 0": "Equal",
        "equal: 0.5 unequal: 0.5": "Partial implementation",
        "equal: 0 unequal: 0.5": "Partial implementation",
        "equal: 0 unequal: 0": "No legal provisions",
        "equal: 0 unequal: 1": "Unequal",
    },
    "marriage": {
        "equality: 1 ban: 0 civil_unions: 0": "Legal",
        "equality: 1 ban: 0 civil_unions: 1": "Legal",
        "equality: 1 ban: 0 civil_unions: 0.5": "Legal",
        "equality: 0.5 ban: 0 civil_unions: 0.5": "Partial",
        "equality: 0 ban: 0 civil_unions: 1": "Partial",
        "equality: 0 ban: 0 civil_unions: 0.5": "Partial",
        "equality: 0 ban: 0 civil_unions: 0": "No legal provisions",
        "equality: 0 ban: 0.5 civil_unions: 0.5": "Marriage and ban both partial",
        "equality: 0.5 ban: 0.5 civil_unions: 0.5": "Marriage and ban both partial",
        "equality: 0.5 ban: 1 civil_unions: 0.5": "Partial ban",
        "equality: 0 ban: 0.5 civil_unions: 0": "Partial ban",
        "equality: 0 ban: 1 civil_unions: 1": "Partial ban",
        "equality: 0 ban: 1 civil_unions: 0.5": "Partial ban",
        "equality: 0 ban: 1 civil_unions: 0": "Banned",
    },
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow, regions and population datasets.
    ds_meadow = paths.load_dataset("lgbti_policy_index")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["lgbti_policy_index"].reset_index()

    #
    # Process data.
    # Drop region column
    tb = tb.drop(columns=["region"])

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add regional aggregations
    tb = add_regional_aggregations(tb, ds_regions=ds_regions, ds_population=ds_population, regions=REGIONS)

    tb = create_combined_columns(tb)

    tb = add_country_counts_and_population_by_status(
        tb=tb,
        columns=["age_of_consent", "marriage", "lgb_military_join"],
        ds_regions=ds_regions,
        ds_population=ds_population,
        regions=REGIONS,
        missing_data_on_columns=True,
    )

    # Verify index and sort
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regional_aggregations(tb: Table, ds_regions: Dataset, ds_population: Dataset, regions: List) -> Table:
    """Add regional aggregations using population and country-region mapping."""

    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)

    # Define a list of variable to make them binary
    binary_vars = [
        "equal_age",
        "constitution",
        "conversion_therapies",
        "employment_discrim",
        "gender_surgery",
        "hate_crimes",
        "incite_hate",
        "joint_adoption",
        "lgb_military",
        "lgb_military_ban",
        "marriage_equality",
        "samesex_legal",
        "third_gender",
        "trans_military",
        "civil_unions",
        "gendermarker",
    ]

    # List of regressive binary variables
    regressive_vars = ["death_penalty", "propaganda", "unequal_age", "marriage_ban"]

    # Create a new column _yes that is 1 if the variable is 1 and 0 otherwise; and _no that is 1 if the variable is < 1 and 0 otherwise
    # Also create a new column _yes_pop that is the product of _yes and population; and _no_pop that is the product of _no and population
    binary_vars_yes = []
    binary_vars_no = []
    binary_vars_yes_pop = []
    binary_vars_no_pop = []
    for var in binary_vars:
        binary_vars_yes.append(f"{var}_yes")
        binary_vars_no.append(f"{var}_no")
        binary_vars_yes_pop.append(f"{var}_yes_pop")
        binary_vars_no_pop.append(f"{var}_no_pop")

        tb[f"{var}_yes"] = tb[f"{var}"].apply(lambda x: 1 if x == 1 else 0)
        tb[f"{var}_no"] = tb[f"{var}"].apply(lambda x: 1 if x < 1 else 0)

        tb[f"{var}_yes_pop"] = tb[f"{var}_yes"] * tb["population"]
        tb[f"{var}_no_pop"] = tb[f"{var}_no"] * tb["population"]

    # Run a similar code for regressive policy variables (yes and partially should be together)
    regressive_vars_yes = []
    regressive_vars_no = []
    regressive_vars_yes_pop = []
    regressive_vars_no_pop = []
    for var in regressive_vars:
        regressive_vars_yes.append(f"{var}_yes")
        regressive_vars_no.append(f"{var}_no")
        regressive_vars_yes_pop.append(f"{var}_yes_pop")
        regressive_vars_no_pop.append(f"{var}_no_pop")

        tb[f"{var}_yes"] = tb[f"{var}"].apply(lambda x: 1 if x > 0 else 0)
        tb[f"{var}_no"] = tb[f"{var}"].apply(lambda x: 1 if x == 0 else 0)

        tb[f"{var}_yes_pop"] = tb[f"{var}_yes"] * tb["population"]
        tb[f"{var}_no_pop"] = tb[f"{var}_no"] * tb["population"]

    # Define variables which their aggregation is weighted by population
    pop_vars = ["policy_index", "samesex_illegal"]

    # Multiply variables by population
    pop_vars_weighted = []
    for var in pop_vars:
        pop_vars_weighted.append(f"{var}_weighted")
        tb[f"{var}_weighted"] = tb[var] * tb["population"]

    # Define the variables and aggregation method to be used in the following function loop
    aggregations = dict.fromkeys(
        pop_vars_weighted
        + binary_vars_yes
        + binary_vars_no
        + binary_vars_yes_pop
        + binary_vars_no_pop
        + regressive_vars_yes
        + regressive_vars_no
        + regressive_vars_yes_pop
        + regressive_vars_no_pop
        + ["population"],
        "sum",
    )

    # Add regional aggregates, by summing up the variables in `aggregations`
    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        regions=regions,
        aggregations=aggregations,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Filter dataset by regions to make additional calculations and drop regions in tb
    tb_regions = tb[tb["country"].isin(regions)].reset_index(drop=True)
    tb = tb[~tb["country"].isin(regions)].reset_index(drop=True)

    # Also drop binary vars in tb (they are only useful for regions)
    tb = tb.drop(
        columns=binary_vars_yes
        + binary_vars_no
        + binary_vars_yes_pop
        + binary_vars_no_pop
        + regressive_vars_yes
        + regressive_vars_no
        + regressive_vars_yes_pop
        + regressive_vars_no_pop
    )

    # Calculate average variables for regions
    for var in pop_vars:
        tb_regions[var] = tb_regions[f"{var}_weighted"] / tb_regions["population"]

    # Concatenate tb with tb_regions
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    # Drop weighted and population columns
    tb = tb.drop(columns=["population"] + pop_vars_weighted)

    return tb


def create_combined_columns(tb: Table) -> Table:
    """Create combined columns for some variables covering the same issue"""

    # AGE OF CONSENT
    # Create categories to classify equal_age and unequal_age numbers
    tb = create_temporary_category_cols(tb, ["equal_age", "unequal_age"])
    tb["age_of_consent"] = "equal: " + tb["equal_age_category"] + " unequal: " + tb["unequal_age_category"]

    # MARRIAGE
    # Create categories to classify marriage_equality, marriage_ban and civil_unions numbers
    tb = create_temporary_category_cols(tb, ["marriage_equality", "marriage_ban", "civil_unions"])
    tb["marriage"] = (
        "equality: "
        + tb["marriage_equality_category"]
        + " ban: "
        + tb["marriage_ban_category"]
        + " civil_unions: "
        + tb["civil_unions_category"]
    )

    # LGBT MILITARY
    tb.loc[(tb["lgb_military"] == 1) & (tb["lgb_military_ban"] == 0), "lgb_military_join"] = "Allowed"
    tb.loc[(tb["lgb_military"] == 0) & (tb["lgb_military_ban"] == 1), "lgb_military_join"] = "Banned"
    tb.loc[(tb["lgb_military"] == 0) & (tb["lgb_military_ban"] == 0), "lgb_military_join"] = "No policy"

    # Copy metadata to lgbt_military_join
    tb["lgb_military_join"] = tb["lgb_military_join"].copy_metadata(tb["country"])

    # Remove temporary columns
    tb = tb.drop(
        columns=[
            "equal_age_category",
            "unequal_age_category",
            "marriage_equality_category",
            "marriage_ban_category",
            "civil_unions_category",
        ]
    )

    # Remap categories
    # Define columns
    column_list = list(CATEGORIES_RENAMING.keys())

    # Rename categories
    for col in column_list:
        tb[col] = tb[col].astype("string")
        tb[col] = map_series(
            series=tb[col],
            mapping=CATEGORIES_RENAMING[col],
            warn_on_missing_mappings=False,
            warn_on_unused_mappings=False,
            show_full_warning=False,
        )
        tb[col] = tb[col].copy_metadata(tb["country"])

    return tb


def create_temporary_category_cols(tb: Table, columns: List) -> Table:
    """
    Create categories for numerical values and avoid code repetition
    """

    for col in columns:
        tb.loc[tb[col] == 1, f"{col}_category"] = "1"
        tb.loc[(tb[col] > 0) & (tb[col] < 1), f"{col}_category"] = "0.5"
        tb.loc[tb[col] == 0, f"{col}_category"] = "0"

    return tb


def add_country_counts_and_population_by_status(
    tb: Table,
    columns: List[str],
    ds_regions: Dataset,
    ds_population: Dataset,
    regions: List[str],
    missing_data_on_columns: bool = False,
) -> Table:
    """
    Add country counts and population by status for the columns in the list
    """

    tb_regions = tb.copy()

    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    # Define empty dictionaries for each of the columns
    columns_count_dict = {columns[i]: [] for i in range(len(columns))}
    columns_pop_dict = {columns[i]: [] for i in range(len(columns))}
    for col in columns:
        if missing_data_on_columns:
            # Fill nan values with "missing"
            tb_regions[col] = tb_regions[col].fillna("missing")
        # Get the unique values in the column
        status_list = list(tb_regions[col].unique())
        for status in status_list:
            # Calculate count and population for each status in the column
            tb_regions[f"{col}_{status}_count"] = tb_regions[col].apply(lambda x: 1 if x == status else 0)
            tb_regions[f"{col}_{status}_pop"] = tb_regions[f"{col}_{status}_count"] * tb_regions["population"]

            # Add the new columns to the list
            columns_count_dict[col].append(f"{col}_{status}_count")
            columns_pop_dict[col].append(f"{col}_{status}_pop")

    # Create a new list with all the count columns and population columns
    columns_count = [item for sublist in columns_count_dict.values() for item in sublist]
    columns_pop = [item for sublist in columns_pop_dict.values() for item in sublist]

    aggregations = dict.fromkeys(
        columns_count + columns_pop + ["population"],
        "sum",
    )

    tb_regions = geo.add_regions_to_table(
        tb=tb_regions,
        ds_regions=ds_regions,
        regions=regions,
        aggregations=aggregations,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Remove population column
    tb_regions = tb_regions.drop(columns=["population"])

    # Add population again
    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    # Calculate the missing population for each region
    for col in columns:
        # Calculate the missing population for each column, by subtracting the population of the countries with data from the total population
        tb_regions[f"{col}_missing_pop_other_countries"] = tb_regions["population"] - tb_regions[
            columns_pop_dict[col]
        ].sum(axis=1)
        if missing_data_on_columns:
            tb_regions[f"{col}_missing_pop"] = (
                tb_regions[f"{col}_missing_pop"] + tb_regions[f"{col}_missing_pop_other_countries"]
            )

        else:
            # Rename column
            tb_regions = tb_regions.rename(columns={f"{col}_missing_pop_other_countries": f"{col}_missing_pop"})

            # Append this missing population column to the list of population columns
            columns_pop.append(f"{col}_missing_pop")

    # Keep only the regions in the country column
    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].copy().reset_index(drop=True)

    # Keep only the columns I need
    tb_regions = tb_regions[["country", "year"] + columns_count + columns_pop]

    # Merge the two tables
    tb = pr.merge(tb, tb_regions, on=["country", "year"], how="outer")

    return tb
