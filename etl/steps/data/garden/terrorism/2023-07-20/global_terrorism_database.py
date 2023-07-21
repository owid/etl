"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions for which aggregates will be created.
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def add_data_for_regions(tb: Table, regions: List[str], ds_regions: Dataset) -> Table:
    tb_with_regions = tb.copy()
    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
        )
        tb_with_regions = geo.add_region_aggregates(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.99999,
        )

    return tb_with_regions


def add_deaths_and_population(tb_terrorism: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches the given terrorism DataFrame with population and deaths data.

    Parameters:
        tb_terrorism (pd.DataFrame): A DataFrame containing terrorism data with columns 'country' and 'year'.

    Returns:
        pd.DataFrame: The enriched DataFrame with additional 'population' and 'deaths' columns.
    """

    # Step 1: Load population data and merge with terrorism data
    ds_population = cast(Dataset, paths.load_dependency("population"))
    tb_population = ds_population["population"].reset_index(drop=False)
    df_pop_add = pd.merge(
        tb_terrorism, tb_population[["country", "year", "population"]], how="left", on=["country", "year"]
    )

    # Step 2: Load deaths data and merge with terrorism data
    ds_meadow_un = cast(Dataset, paths.load_dependency("un_wpp"))
    tb_un = ds_meadow_un["un_wpp"]

    # Step 3: Extract 'deaths' data with specific criteria
    deaths_df = tb_un.xs("deaths", level="metric")
    deaths_df_all_sex = deaths_df.xs("all", level="sex")
    deaths_df_estimates = deaths_df_all_sex.xs("estimates", level="variant")
    deaths_final_df = deaths_df_estimates.xs("all", level="age")
    deaths_final_df.reset_index(inplace=True)
    deaths_final_df.rename(columns={"location": "country", "value": "deaths"}, inplace=True)

    # Step 4: Merge 'deaths' data with the enriched DataFrame
    df_deaths_add = pd.merge(df_pop_add, deaths_final_df, how="left", on=["country", "year"])

    return df_deaths_add


def run(dest_dir: str) -> None:
    """
    Process terrorism data and save the results in a new garden dataset.

    This function loads the terrorism data from the meadow dataset, calculates yearly sums of:
        - terroris attacks
        - number of people killed
        - number of people wounded
        - number of terrorism attacks by target
        - number of terrorism attacks by weapon type
        - number of terrorism attacks by attack type

    It then calculates per capita metrics, adds decadal averages, and saves the results in a new garden
    dataset.

    Parameters:
        dest_dir (str): The destination directory where the new garden dataset will be saved.

    Returns:
        None
    """

    # Load inputs from the meadow dataset.
    ds_meadow_terrorism = cast(Dataset, paths.load_dependency("global_terrorism_database"))
    tb_terrorism = ds_meadow_terrorism["global_terrorism_database"]
    tb_terrorism.reset_index(inplace=True)

    # Process data to calculate statistics related to terrorism incidents.
    tb: Table = geo.harmonize_countries(df=tb_terrorism, countries_file=paths.country_mapping_path)
    total_df = pd.DataFrame()
    total_df["total_killed"] = tb.groupby(["country", "year"])["nkill"].sum()
    total_df["total_wounded"] = tb.groupby(["country", "year"])["nwound"].sum()
    total_df["total_incident_counts"] = tb.groupby(["country", "year"]).size()
    total_df["total_casualties"] = total_df["total_wounded"] + total_df["total_incident_counts"]

    # Process terrorism targets, attacks, and weapons data.
    pivot_weapon_type, pivot_target_type, pivot_df_attack_type = terrorism_targets_attacks_weapons(tb)
    merge_weapon_target = pd.merge(pivot_target_type, pivot_weapon_type, on=["country", "year"])
    merge_attack = pd.merge(pivot_df_attack_type, merge_weapon_target, on=["country", "year"])
    merge_all = pd.merge(total_df, merge_attack, on=["country", "year"])

    # Add deaths and population data, and region aggregates.
    df_pop_deaths = add_deaths_and_population(merge_all)
    ds_regions: Dataset = paths.load_dependency("regions")
    df_pop_deaths = add_data_for_regions(tb=df_pop_deaths, regions=REGIONS, ds_regions=ds_regions)

    # Calculate statistics per capita
    df_pop_deaths["terrorism_wounded_per_capita"] = df_pop_deaths["total_wounded"] / df_pop_deaths["population"]
    df_pop_deaths["terrorism_deaths_per_capita"] = df_pop_deaths["total_killed"] / df_pop_deaths["population"]
    df_pop_deaths["terrorism_casualties_per_capita"] = df_pop_deaths["total_casualties"] / df_pop_deaths["population"]

    # Perform decadal averaging for selected columns
    cols_for_decadal_av = [
        "total_killed",
        "total_wounded",
        "total_incident_counts",
        "total_casualties",
        "terrorism_wounded_per_capita",
        "terrorism_deaths_per_capita",
        "terrorism_casualties_per_capita",
        "Armed Assault_attack",
        "Assassination_attack",
        "Bombing/Explosion_attack",
        "Facility/Infrastructure Attack_attack",
        "Hijacking_attack",
        "Hostage Taking (Barricade Incident)_attack",
        "Hostage Taking (Kidnapping)_attack",
        "Unarmed Assault_attack",
        "Unknown_attack",
        "Abortion Related_target",
        "Airports & Aircraft_target",
        "Business_target",
        "Educational Institution_target",
        "Food or Water Supply_target",
        "Government (Diplomatic)_target",
        "Government (General)_target",
        "Journalists & Media_target",
        "Maritime_target",
        "Military_target",
        "NGO_target",
        "Other_target",
        "Police_target",
        "Private Citizens & Property_target",
        "Religious Figures/Institutions_target",
        "Telecommunication_target",
        "Terrorists/Non-State Militia_target",
        "Tourists_target",
        "Transportation_target",
        "Unknown_target",
        "Utilities_target",
        "Violent Political Party_target",
        "Biological_weapon",
        "Chemical_weapon",
        "Explosives_weapon",
        "Fake Weapons_weapon",
        "Firearms_weapon",
        "Incendiary_weapon",
        "Melee_weapon",
        "Other_weapon",
        "Radiological_weapon",
        "Sabotage Equipment_weapon",
        "Unknown_weapon",
        "Vehicle (not to include vehicle-borne explosives, i.e., car or truck bombs)_weapon",
    ]

    df_pop_deaths = perform_decadal_averaging(df_pop_deaths, cols_for_decadal_av=cols_for_decadal_av)

    # Convert relevant columns to float64 data type (to avoid errors related to this issue -  https://github.com/owid/etl/issues/1334)
    df_pop_deaths["terrorism_casualties_per_capita"] = df_pop_deaths["terrorism_casualties_per_capita"].astype(
        "float64"
    )
    df_pop_deaths["terrorism_deaths_per_capita"] = df_pop_deaths["terrorism_deaths_per_capita"].astype("float64")
    df_pop_deaths["terrorism_wounded_per_capita"] = df_pop_deaths["terrorism_wounded_per_capita"].astype("float64")

    # Convert DataFrame to a new garden dataset table.
    tb_garden = Table(df_pop_deaths, short_name=paths.short_name)
    tb_garden.set_index(["country", "year"], inplace=True)

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow_terrorism.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def perform_decadal_averaging(df: pd.DataFrame, cols_for_decadal_av: list) -> pd.DataFrame:
    """
    Perform decadal averaging on the specified columns in the DataFrame.

    This function calculates the mean value for each specified column over the decades. It groups the data by decade
    based on the 'year' column and computes the mean for each decadal group. Non-decadal years are replaced with NaN.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the data.
        cols_for_decadal_av (list): A list of column names for which decadal averaging should be performed.

    Returns:
        pd.DataFrame: A modified DataFrame with additional columns representing decadal averages for the specified
                      columns.
    """

    for column in df[cols_for_decadal_av]:
        # Create a new column name for decadal average
        decadal_column = f"decadal_{column}"

        # Group the data by decade and compute the mean for each decadal group
        df[decadal_column] = df.groupby(df["year"] // 10 * 10)[column].transform("mean")

        # Replace non-decadal years with NaN
        df[decadal_column] = df[decadal_column].mask(df["year"] % 10 != 0, pd.NA)

    # Return the modified DataFrame
    return df


def terrorism_targets_attacks_weapons(df: pd.DataFrame) -> tuple:
    """
    Perform data processing on the given DataFrame to extract information related to terrorism targets, attacks,
    and weapons.

    This function groups the DataFrame by 'country', 'year', and different attributes like 'attacktype1_txt',
    'weaptype1_txt', and 'targtype1_txt'. It then calculates the total occurrences for each group. The data is
    further transformed into pivot tables, with suffixes added to column names.

    Parameters:
        df (pd.DataFrame): The DataFrame containing terrorism data.

    Returns:
        tuple: A tuple containing three pivot tables representing the occurrences of weapons, targets, and attacks
               across different countries and years.
    """

    # Group the data by 'country', 'year', and 'attacktype1_txt', and calculate the size
    total_df_attack_type = (
        df.groupby(["country", "year", "attacktype1_txt"]).size().reset_index(name="attack_type_year")
    )

    # Group the data by 'country', 'year', and 'weaptype1_txt', and calculate the size
    total_df_weapon_type = df.groupby(["country", "year", "weaptype1_txt"]).size().reset_index(name="weapon_type_year")

    # Group the data by 'country', 'year', and 'targtype1_txt', and calculate the size
    total_df_target_type = df.groupby(["country", "year", "targtype1_txt"]).size().reset_index(name="target_type_year")

    # Pivot the dataframes so that each target, attack, weapon is a column
    pivot_weapon_type = pd.pivot(
        total_df_weapon_type, index=["country", "year"], columns="weaptype1_txt", values="weapon_type_year"
    )

    pivot_target_type = pd.pivot(
        total_df_target_type, index=["country", "year"], columns="targtype1_txt", values="target_type_year"
    )

    pivot_df_attack_type = pd.pivot(
        total_df_attack_type, index=["country", "year"], columns="attacktype1_txt", values="attack_type_year"
    )
    # Add suffix for easier identification of what these columns mean later
    pivot_weapon_type = add_suffix(pivot_weapon_type, "_weapon")
    pivot_target_type = add_suffix(pivot_target_type, "_target")
    pivot_df_attack_type = add_suffix(pivot_df_attack_type, "_attack")

    return pivot_weapon_type, pivot_target_type, pivot_df_attack_type


def add_suffix(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """
    Add a suffix to the column names of the given DataFrame, excluding 'year' and 'country' columns.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the data.
        suffix (str): The suffix to be added to the column names.

    Returns:
        pd.DataFrame: The DataFrame with column names suffixed (excluding 'year' and 'country').
    """

    # loop over each column and add the suffix if the column name is not 'year' or 'country'
    for column in df.columns:
        if column not in ["year", "country"]:
            df.rename(columns={column: column + suffix}, inplace=True)

    return df
