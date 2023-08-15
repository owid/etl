"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["Europe", "Africa", "Asia", "Oceania", "World"]


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
    tb_terrorism = tb_terrorism.astype(
        {
            "nkill": float,
            "nwound": float,
        }
    )
    tb_terrorism.reset_index(inplace=True)

    # Process data to calculate statistics related to terrorism incidents.
    tb: Table = geo.harmonize_countries(df=tb_terrorism, countries_file=paths.country_mapping_path)
    total_df = pd.DataFrame()
    total_df["total_killed"] = tb.groupby(["country", "year"])["nkill"].sum()
    total_df["total_wounded"] = tb.groupby(["country", "year"])["nwound"].sum()
    total_df["total_incident_counts"] = tb.groupby(["country", "year"]).size()
    # Add GTD regions to number of deaths, attacks and wounded
    total_df = add_regions(tb, total_df)

    tb.loc[tb["nkill"] == 0, "severity"] = "0 deaths"
    tb.loc[(tb["nkill"] >= 1) & (tb["nkill"] <= 5), "severity"] = "1-5 deaths"
    tb.loc[(tb["nkill"] >= 6) & (tb["nkill"] <= 10), "severity"] = "6-10 deaths"
    tb.loc[(tb["nkill"] >= 11) & (tb["nkill"] <= 20), "severity"] = "11-20 deaths"
    tb.loc[(tb["nkill"] >= 21) & (tb["nkill"] <= 50), "severity"] = "21-50 deaths"
    tb.loc[(tb["nkill"] >= 51) & (tb["nkill"] <= 100), "severity"] = "51-99 deaths"
    tb.loc[(tb["nkill"] > 100), "severity"] = "100+"

    # For the total_attack_type
    total_attack_type = generate_summary_dataframe(tb, "attacktype1_txt", ["nkill", "nwound"])

    # For the total_suicide
    total_suicide = generate_summary_dataframe(tb, "suicide", ["nkill", "nwound"])
    suicide_mapping = {0: "No Suicide", 1: "Suicide"}
    total_suicide["suicide"] = total_suicide["suicide"].map(suicide_mapping)

    # For the total_target
    total_target = generate_summary_dataframe(tb, "targtype1_txt", ["nkill", "nwound"])

    # For the total_severity
    total_severity = severity(tb)
    # Create a dictionary to store all pivot tables
    pivot_tables = {
        "attack_type": pivot_dataframe(
            total_attack_type,
            index_columns=["year", "country"],
            pivot_column="attacktype1_txt",
            value_columns=["total_nkill", "total_nwound", "total_incident_counts"],
        ),
        "target": pivot_dataframe(
            total_target,
            index_columns=["year", "country"],
            pivot_column="targtype1_txt",
            value_columns=["total_nkill", "total_nwound", "total_incident_counts"],
        ),
        "suicide": pivot_dataframe(
            total_suicide,
            index_columns=["year", "country"],
            pivot_column="suicide",
            value_columns=["total_nkill", "total_nwound", "total_incident_counts"],
        ),
        "total_severity": pivot_dataframe(
            total_severity,
            index_columns=["year", "country"],
            pivot_column="severity",
            value_columns="total_incident_severity",
        ),
    }

    # Merge all pivot tables
    merged_df = pivot_tables["attack_type"]
    for key in pivot_tables:
        if key == "target":
            pivot_tables[key] = add_suffix(pivot_tables[key], "_target")
        if key != "attack_type":
            merged_df = pd.merge(merged_df, pivot_tables[key], on=["year", "country"], how="outer")

    merge_all = pd.merge(total_df, merged_df, on=["country", "year"], how="outer")
    # Add deaths and population data, and region aggregates.
    df_pop_deaths = add_deaths_and_population(merge_all)
    ds_regions: Dataset = paths.load_dependency("regions")
    df_pop_deaths = add_data_for_regions(tb=df_pop_deaths, regions=REGIONS, ds_regions=ds_regions)
    # Calculate statistics per capita
    df_pop_deaths["terrorism_wounded_per_100k"] = df_pop_deaths["total_wounded"] / (
        df_pop_deaths["population"] / 100000
    )
    df_pop_deaths["terrorism_deaths_per_100k"] = df_pop_deaths["total_killed"] / (df_pop_deaths["population"] / 100000)
    df_pop_deaths["share_of_deaths"] = (df_pop_deaths["total_killed"] / df_pop_deaths["deaths"]) * 100

    # Drop total deaths and population columns
    df_pop_deaths.drop(["deaths", "population"], axis=1, inplace=True)

    # Convert DataFrame to a new garden dataset table.
    tb_garden = Table(df_pop_deaths, short_name=paths.short_name, underscore=True)
    tb_garden.set_index(["country", "year"], inplace=True)
    # Creat a copy of deaths for plotting across sources
    tb_garden["total_killed_gtd"] = tb_garden["total_killed"].copy()

    # Add deaths and attacks per suicide/ non-suicide terrorist attack
    tb_garden["killed_per_suicide_attack"] = (
        tb_garden["total_nkill_suicide"] / tb_garden["total_incident_counts_suicide"]
    )
    tb_garden["killed_per_non_suicide_attack"] = (
        tb_garden["total_nkill_no_suicide"] / tb_garden["total_incident_counts_no_suicide"]
    )

    tb_garden["injured_per_suicide_attack"] = (
        tb_garden["total_nwound_suicide"] / tb_garden["total_incident_counts_suicide"]
    )
    tb_garden["injured_per_non_suicide_attack"] = (
        tb_garden["total_nwound_no_suicide"] / tb_garden["total_incident_counts_no_suicide"]
    )

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow_terrorism.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


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


def add_regions(df, total_df):
    """
    Aggregates incident data by regions and years, summarizing the total killed, wounded, and incident counts.

    Args:
        df (pd.DataFrame): A DataFrame containing the columns 'region_txt', 'year', 'nkill', 'nwound',
                           and 'total_incident_counts'. 'region_txt' refers to the region text, 'nkill'
                           to the number of people killed, 'nwound' to the number of people wounded,
                           and 'total_incident_counts' to the total number of incidents.

    Returns:
        pd.DataFrame: A concatanated DataFrame with original data concatenated with aggregated data grouped by region and year, containing the total number
                      of killed, wounded, and incidents.

    """
    grouped_regions_df = df.groupby(["region_txt", "year"])
    summary_regions_df = pd.DataFrame()

    for column in ["nkill", "nwound"]:
        summary_regions_df[f"total_{column}"] = grouped_regions_df[column].sum()

    summary_regions_df["total_incident_counts"] = grouped_regions_df.size()
    summary_regions_df = summary_regions_df.rename_axis(index={"region_txt": "country"})

    summary_regions_df.rename(columns={"total_nkill": "total_killed", "total_nwound": "total_wounded"}, inplace=True)
    merged_df = pd.concat([summary_regions_df, total_df])

    return merged_df


def generate_summary_dataframe(df, group_column, target_columns):
    """
    Generate a summary DataFrame based on specified group and target columns. The function groups
    data by year and the specified group column, which may include "country" or "region_txt."
    If the group column is not "region_txt," it will also include regional summaries.

    Parameters:
        df (pandas.DataFrame): The original DataFrame containing the data.
        group_column (str): The column name by which to group the data. If "region_txt," data
                            will be grouped by "year" and "region_txt"; otherwise, data will be
                            grouped by "country," "year," and the specified group column.
        target_columns (list of str): List of column names for calculating summary statistics
                                      (e.g., sum of "nkill" and "nwound").

    Returns:
        pandas.DataFrame: A summary DataFrame with grouped data and corresponding summary statistics.
                          If group_column is not "region_txt," the DataFrame also includes regional
                          summaries, and the index name "region_txt" is renamed to "country."
    """
    if group_column != "region_txt":
        grouped_df = df.groupby(["country", "year", group_column])
    else:
        grouped_df = df.groupby(["year", group_column])

    summary_df = pd.DataFrame()

    for column in target_columns:
        summary_df[f"total_{column}"] = grouped_df[column].sum()

    summary_df["total_incident_counts"] = grouped_df.size()
    if group_column != "region_txt":
        grouped_regions_df = df.groupby(["region_txt", "year", group_column])
        summary_regions_df = pd.DataFrame()

        for column in target_columns:
            summary_regions_df[f"total_{column}"] = grouped_regions_df[column].sum()

        summary_regions_df["total_incident_counts"] = grouped_regions_df.size()
        summary_regions_df = summary_regions_df.rename_axis(index={"region_txt": "country"})
        merge_GTD_regions = pd.concat([summary_regions_df, summary_df])

        return merge_GTD_regions.reset_index()
    else:
        return summary_df.reset_index()


def pivot_dataframe(dataframe, index_columns, pivot_column, value_columns):
    """
    Pivot the dataframe based on the given parameters.

    Parameters:
        dataframe (pd.DataFrame): The input DataFrame to be pivoted.
        index_columns (list): List of column names to be used as index in the pivot.
        pivot_column (str): Column name to be used for creating columns in the pivot.
        value_columns (list): List of column names to be aggregated in the pivot.

    Returns:
        pd.DataFrame: The pivoted DataFrame.
    """
    pivot_df = pd.pivot(dataframe, index=index_columns, columns=pivot_column, values=value_columns)
    pivot_df.reset_index(inplace=True)

    # If 'country' is not a column, add a column with default value 'GTD' id Global Tourism Dataset (when pivoting using GTD defined regions)
    if "country" not in pivot_df.columns:
        pivot_df["country"] = "GTD"

    # If pivot_column is not 'severity' (not hierarchical) modify column names
    if pivot_column != "severity":
        pivot_df.columns = [
            f"{col[0]}_{col[1]}" if col[0] not in ["year", "country"] else col[0] for col in pivot_df.columns
        ]

    return pivot_df


def severity(tb):
    """
    Calculate the total incident severity for each country and region by year and severity level.
    The function groups the input DataFrame by country, year, and severity, and then by region (region_txt),
    year, and severity. The results are concatenated into a single DataFrame, where the region_txt index
    level is renamed to "country."

    Parameters:
        tb (pandas.DataFrame): The original DataFrame containing columns "country," "region_txt,"
                              "year," and "severity."

    Returns:
        pandas.DataFrame: A DataFrame containing the total incident severity for each country and region,
                          grouped by year and severity level. The DataFrame has columns "country," "year,"
                          "severity," and "total_incident_severity."
    """
    total_severity_country = pd.DataFrame()
    total_severity_country["total_incident_severity"] = tb.groupby(["country", "year", "severity"]).size()

    total_severity_regions = pd.DataFrame()
    total_severity_regions["total_incident_severity"] = tb.groupby(["region_txt", "year", "severity"]).size()

    total_severity_regions = total_severity_regions.rename_axis(index={"region_txt": "country"})
    merge_GTD_regions = pd.concat([total_severity_country, total_severity_regions])

    return merge_GTD_regions.reset_index()
