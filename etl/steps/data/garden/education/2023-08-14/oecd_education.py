"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Process the meadow dataset, merge it with the World Bank dataset,
    and save the results in a new garden dataset.

    :param dest_dir: Destination directory to save the output dataset.
    """

    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("oecd_education"))
    tb = ds_meadow["oecd_education"].reset_index()

    # Load the World Bank Education Dataset
    ds_garden_wb = cast(Dataset, paths.load_dependency("education"))
    tb_wb = ds_garden_wb["education"]

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Convert columns to numeric type and set the multi-index
    for column in tb.columns:
        if column not in ["country", "year"]:
            tb[column] = pd.to_numeric(tb[column], errors="coerce")
            tb[column] = tb[column].astype("float64")

    # Combine best guess (1820) with educational attainment (1870 > ) of estimates for some formal education
    mask = (tb["year"] == 1820) & (tb["country"] == "World")
    value_to_assign = tb.loc[mask, "best_guess"].values[0]  # Get the specific value
    tb.loc[mask, "population_with_basic_education"] = value_to_assign

    # Create an indicator with historical estimates of share of population with no education
    tb["no_formal_education"] = 100 - tb["population_with_basic_education"]

    # Extract literacy and formal education indicators from World Bank Education Dataset post-2010
    df_above_2010 = extract_related_world_bank_data(tb_wb)

    # Merge data with World Bank literacy and education data
    merged_wb = pd.concat([tb, df_above_2010])
    merged_wb["illiterate"] = 100 - merged_wb["literacy"]

    merged_wb.set_index(["country", "year"], verify_integrity=True, inplace=True)

    tb = Table(merged_wb, short_name=paths.short_name, underscore=True)

    # Save the processed data in a new garden dataset
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def extract_related_world_bank_data(tb_wb: Table) -> pd.DataFrame:
    """
    Extracts indicators for combining historical data on share of people with no education and literacy from the World Bank dataset.

    :param tb_wb: Table containing World Bank education dataset
    :return: DataFrame with selected literacy and share of people with no education for years above 2010
    """

    # Define columns to select for no education estimates and literacy
    select_wb_cols = [
        # Primary enrollment columns
        "wittgenstein_projection__percentage_of_the_population_age_15plus_by_highest_level_of_educational_attainment__no_education__total",
        "literacy_rate__adult_total__pct_of_people_ages_15_and_above",
    ]

    # Dictionary to rename columns to be consistent with the OECD dataset
    dictionary_to_rename_and_combine = {
        "literacy_rate__adult_total__pct_of_people_ages_15_and_above": "literacy",
        "wittgenstein_projection__percentage_of_the_population_age_15plus_by_highest_level_of_educational_attainment__no_education__total": "no_formal_education",
    }

    # Select and rename columns
    df_wb = tb_wb[select_wb_cols]
    df_wb.rename(columns=dictionary_to_rename_and_combine, inplace=True)

    # Filter the DataFrame for years above 2010 (OECD dataset stops in 2010)
    df_above_2010 = df_wb[(df_wb.index.get_level_values("year") > 2010)]
    df_above_2010["population_with_basic_education"] = 100 - df_above_2010["no_formal_education"]
    df_above_2010.reset_index(inplace=True)

    return df_above_2010
