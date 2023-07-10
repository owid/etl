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

    df = reshape_survey_data(tb)

    selected_all_age_groups_columns = tb.filter(regex=r"^concerned")
    # Reshape the DataFrame using 'melt()'
    melted_df_all_age_groups = pd.melt(
        tb,
        id_vars="options",
        value_vars=selected_all_age_groups_columns.columns,
        var_name="melted_columns",
        value_name="value",
    )
    melted_df_all_age_groups["melted_columns"] = melted_df_all_age_groups["melted_columns"].str.replace(
        r".*_earth_", "", regex=True
    )
    pivot_df_all_age_groups = melted_df_all_age_groups.pivot_table(
        index=["options"], columns="melted_columns", values="value"
    )
    pivot_df_all_age_groups.reset_index(inplace=True)
    pivot_df_all_age_groups["age_group"] = "All adults"

    merged = pd.concat([pivot_df_all_age_groups, df])
    merged.reset_index(inplace=True)
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

    Parameters:
    df (DataFrame): The DataFrame to reshape.

    Returns:
    DataFrame: The reshaped DataFrame.
    """

    # Select the necessary columns for reshaping
    selected_columns = df.filter(regex="^answers_age_", axis=1)

    # Reshape the DataFrame using 'melt()'
    melted_df = pd.melt(
        df, id_vars="options", value_vars=selected_columns.columns, var_name="melted_columns", value_name="value"
    )

    # Extract 'Age Group' and the rest of the string 'Topic'
    melted_df["age_group"] = melted_df["melted_columns"].apply(lambda x: re.search(r"\d{2}", x).group())
    melted_df["cause"] = melted_df["melted_columns"].apply(lambda x: re.search(r"\D+$", x).group())

    # Define a dictionary with the values to replace and their new values
    replacements = {
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

    # Replace the values in the 'cause' column
    melted_df["cause"] = melted_df["cause"].replace(replacements)

    # Drop the unnecessary 'melted_columns' column
    melted_df.drop("melted_columns", axis=1, inplace=True)

    replacements_age = {"18": "18-29 years", "30": "30-44 years", "45": "45-64 years", "65": "65+ years"}

    # Replace the values in the 'age_group' column
    melted_df["age_group"] = melted_df["age_group"].replace(replacements_age)

    # Pivot the DataFrame
    pivot_df = melted_df.pivot_table(index=["options", "age_group"], columns="cause", values="value")

    # Reset the index
    pivot_df = pivot_df.reset_index()

    # Return the reshaped DataFrame
    return pivot_df
