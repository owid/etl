"""Load a meadow dataset and create a garden dataset."""

import re
from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("yougov_end_of_humanity.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("yougov_end_of_humanity"))

    # Read table from meadow dataset.
    tb = ds_meadow["yougov_end_of_humanity"]

    #
    # Process data.
    #
    # Shorten variable names
    tb = tb.rename(columns=lambda x: x.replace("how_", "").replace("__if_at_all__", "_").replace("__", "_"))
    tb = tb.rename(columns=lambda x: x.replace("do_you_think_it_is_that_the_following_would_cause_the_", ""))
    tb = tb.rename(columns=lambda x: x.replace("are_you_about_the_possibility_that_the_following_will_cause_the_", ""))
    tb = tb.rename(columns=lambda x: "answers_age" + x if x.startswith("_") else x)

    # Use the function 'reshape_survey_data()' to reshape the dataframe so that there is a column for each 'cause' for end of humanity and an 'age-group' column specifying the respondents age.
    df_by_age = reshape_survey_data(tb)

    # Now extract percentages for all adults (not split by age group - columns that include concerned)
    # Filter and select columns in 'tb' that start with 'concerned' and store them in 'selected_all_age_groups_columns'.
    selected_all_age_groups_columns = tb.filter(regex=r"^concerned")

    # Reshape 'tb' using the 'melt()' function, where 'options' are kept intact,
    # 'selected_all_age_groups_columns' are converted to rows under 'melted_columns',
    # and their corresponding values are placed under 'value'.
    melted_df_all_age_groups = pd.melt(
        tb,
        id_vars="options",
        value_vars=selected_all_age_groups_columns.columns,
        var_name="melted_columns",
        value_name="value",
    )

    # Remove the prefix before '_earth_' from the values in 'melted_columns' and update the dataframe.
    melted_df_all_age_groups["melted_columns"] = melted_df_all_age_groups["melted_columns"].str.replace(
        r".*_earth_", "", regex=True
    )

    # Transform the 'melted_df_all_age_groups' dataframe into a pivot table with 'options' as index and
    # each unique value in 'melted_columns' as a column. Store the pivot table in 'pivot_df_all_age_groups'.
    pivot_df_all_age_groups = melted_df_all_age_groups.pivot_table(
        index=["options"], columns="melted_columns", values="value"
    )

    # Reset the index of 'pivot_df_all_age_groups' to make 'options' a normal column again, updating the dataframe inplace.
    pivot_df_all_age_groups.reset_index(inplace=True)

    # Add a new column 'age_group' in 'pivot_df_all_age_groups' and fill it with the value 'All adults'.
    pivot_df_all_age_groups["age_group"] = "All adults"

    # Concatenate 'pivot_df_all_age_groups' and 'df' along the row axis and store the result in 'merged'.
    merged = pd.concat([pivot_df_all_age_groups, df_by_age])

    # Reset the index of 'merged', dropping the original index and replacing it with a default integer index.
    merged.reset_index(drop=True, inplace=True)
    tb_garden = Table(merged, short_name=paths.short_name, underscore=True)
    tb_garden.set_index(["options", "age_group"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("yougov_end_of_humanity.end")


def reshape_survey_data(df):
    """
    This function reshapes the given DataFrame, extracting information from column names
    and creating a pivot table with one column for each unique 'cause'.

    It first selects columns that match the regular expression pattern '^answers_age_' and melts these columns into two:
    one for the original column name and one for the values. Then it extracts the 'age group' and 'cause' information
    from the original column names. The extracted 'cause' and 'age group' information are cleaned up by replacing
    specific patterns with more readable strings. Finally, it transforms the DataFrame into a pivot table, which
    is easier to analyze.

    Parameters:
    df (pd.DataFrame): The DataFrame to reshape. It's expected to contain columns named with the pattern 'answers_age_<age>_<cause>'.

    Returns:
    pd.DataFrame: The reshaped DataFrame with each unique 'cause' as a separate column and a multi-index ['options', 'age_group'].
    """

    # Filter and select columns that start with 'answers_age_'
    selected_columns = df.filter(regex="^answers_age_", axis=1)

    # Convert the wide DataFrame to long format. Keep 'options' intact, convert 'selected_columns' to rows under 'melted_columns' and their corresponding values under 'value'.
    melted_df = pd.melt(
        df, id_vars="options", value_vars=selected_columns.columns, var_name="melted_columns", value_name="value"
    )

    # Apply the helper function 'extract_age_group' to extract age group information from 'melted_columns'.
    melted_df["age_group"] = melted_df["melted_columns"].apply(extract_age_group)
    # Apply the helper function 'exctrac_cause_group' to extract cause information from 'melted_columns'.
    melted_df["cause"] = melted_df["melted_columns"].apply(exctrac_cause_group)

    # Define replacements to clean up 'cause' names
    replacements_cause = {
        "_artificial_intelligence": "artificial_intelligence",
        "plus_artificial_intelligence": "artificial_intelligence",
        "_nuclear_weapons": "nuclear_weapons",
        "plus_nuclear_weapons": "nuclear_weapons",
        "_a_pandemic": "a_pandemic",
        "plus_a_pandemic": "a_pandemic",
        "_climate_change": "climate_change",
        "plus_climate_change": "climate_change",
        "_world_war": "world_war",
        "plus_world_war": "world_war",
        "_asteroid_impact": "asteroid_impact",
        "plus_asteroid_impact": "asteroid_impact",
        "_alien_invasion": "alien_invasion",
        "plus_alien_invasion": "alien_invasion",
        "_an_act_of_god": "an_act_of_god",
        "plus_an_act_of_god": "an_act_of_god",
        "_global_inability_to_have_children": "global_inability_to_have_children",
        "plus_global_inability_to_have_children": "global_inability_to_have_children",
    }

    # Apply the replacements to the 'cause' column
    melted_df["cause"] = melted_df["cause"].replace(replacements_cause)

    # Drop the now unnecessary 'melted_columns' column
    melted_df.drop("melted_columns", axis=1, inplace=True)

    # Define age groups to clean up 'age_group' names
    replacements_age = {"18": "18-29 years", "30": "30-44 years", "45": "45-64 years", "65": "65+ years"}

    # Apply the replacements to the 'age_group' column
    melted_df["age_group"] = melted_df["age_group"].replace(replacements_age)

    # Pivot the DataFrame to have 'options', 'age_group' as the index and each unique 'cause' as a column.
    pivot_df = melted_df.pivot_table(index=["options", "age_group"], columns="cause", values="value")

    # Reset the index to make 'options' and 'age_group' normal columns again.
    pivot_df = pivot_df.reset_index()

    return pivot_df


def extract_age_group(text):
    """
    Extract the age group information from the text.

    It does so by searching for the first occurrence of a 2-digit number which represents the age group.

    Parameters:
    text (str): The string to extract the age group from.

    Returns:
    str or None: The age group as a string if found, else None.
    """

    match = re.search(r"\d{2}", text)
    if match:
        return match.group()
    else:
        return None


def exctrac_cause_group(text):
    """
    Extract the cause group information from the text.

    It does so by searching for all non-digits at the end of the string which represent the cause.

    Parameters:
    text (str): The string to extract the cause from.

    Returns:
    str or None: The cause group as a string if found, else None.
    """

    match = re.search(r"\D+$", text)
    if match:
        return match.group()
    else:
        return None
