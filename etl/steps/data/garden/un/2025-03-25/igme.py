"""Load a meadow dataset and create a garden dataset."""

from math import trunc
from typing import List

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = geo.REGIONS

UNIT = {
    "D": "Number of deaths",
    "D_PER_1000_B": "Deaths per 1,000 live births",
    "D_PER_1000_1": "Deaths per 1,000 children aged 1",
    "D_PER_1000_1M": "Deaths per 1,000 children aged 1 month",
    "D_PER_1000_5": "Deaths per 1,000 children aged 5",
    "D_PER_1000_10": "Deaths per 1,000 children aged 10",
    "D_PER_1000_15": "Deaths per 1,000 children aged 15",
    "D_PER_1000_20": "Deaths per 1,000 children aged 20",
    "STILLBIRTHS": "Stillbirths",
    "SB_PER_1000_B": "Stillbirths per 1,000 total births",
}


INDICATOR = {
    "MRY0T4": "Under-five mortality rate",
    "MRY0": "Infant mortality rate",
    "MRY1T4": "Mortality rate age 1-4",
    "TMY0": "Infant deaths",
    "TMY0T4": "Under-five deaths",
    "TMY1T4": "Child deaths age 1-4",
    "MRM0": "Neonatal mortality rate",
    "MRM1T11": "Mortality rate age 1-11 months",
    "MRM1T59": "Mortality rate age 1-59 months",
    "TMM0": "Neonatal deaths",
    "TMM1T11": "Deaths age 1-11 months",
    "TMM1T59": "Deaths age 1-59 months",
    "MRY10T14": "Mortality rate age 10-14",
    "MRY10T19": "Mortality rate age 10-19",
    "MRY15T19": "Mortality rate age 15-19",
    "MRY15T24": "Mortality rate age 15-24",
    "MRY20T24": "Mortality rate age 20-24",
    "MRY5T14": "Mortality rate age 5-14",
    "MRY5T24": "Mortality rate age 5-24",
    "MRY5T9": "Mortality rate age 5-9",
    "TMY10T14": "Deaths age 10-14",
    "TMY10T19": "Deaths age 10-19",
    "TMY15T19": "Deaths age 15-19",
    "TMY15T24": "Deaths age 15-24",
    "TMY20T24": "Deaths age 20-24",
    "TMY5T14": "Deaths age 5-14",
    "TMY5T24": "Deaths age 5-24",
    "TMY5T9": "Deaths age 5-9",
    "SB": "Stillbirths",
    "SBR": "Stillbirth rate",
    "PR_MRM0": "Additional neonatal deaths",  # Not sure what these measure currently
    "PR_MRY0T4": "Additional under-five deaths",  # Not sure what these measure currently
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    # Load countries-regions dataset (required to get ISO codes).
    ds_regions = paths.load_dataset("regions")
    # Load the population dataset
    ds_population = paths.load_dataset("population")
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("igme", version=paths.version)
    # Load vintage dataset which has older data needed for youth mortality
    ds_vintage = paths.load_dataset("igme", version="2018")

    # Read table from meadow dataset.
    tb = ds_meadow["igme"].reset_index()
    tb_vintage = ds_vintage["igme"].reset_index()
    tb_vintage = process_vintage_data(tb_vintage)
    tb_vintage["unit_measure"] = tb_vintage["unit_measure"].str.replace(",", "", regex=False)
    # Appending the region to country where it exists
    # tb["country"] = tb.apply(
    #    lambda row: f"{row['country']} ({row['regional_group']})"
    #    if pd.notna(row["regional_group"])
    #    else row["country"],
    #    axis=1,
    # )

    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Filter out just the bits of the data we want
    tb = filter_data(tb)
    tb = round_down_year(tb)
    tb["unit_measure"] = tb["unit_measure"].replace(UNIT)
    tb["indicator"] = tb["indicator"].replace(INDICATOR)

    # get regional data for count variables
    tb_counts_regions = regional_aggregates_counts(tb, ds_regions, ds_population, threshold=0.8)
    # get regional population weighted averages for rate variables
    tb_rates_regions = population_weighted_regional_averages(tb, ds_population, ds_regions, threshold=0.8)

    # Adding regional aggregates to table
    tb = pr.concat([tb, tb_counts_regions, tb_rates_regions])

    # Removing commas from the unit of measure
    tb["unit_measure"] = tb["unit_measure"].str.replace(",", "", regex=False)
    tb["source"] = "igme (current)"

    # Separate out the variables needed to calculate the under-fifteen mortality rate.
    tb_under_fifteen = tb[
        (
            tb["indicator"].isin(
                ["Under-five deaths", "Deaths age 5-14", "Under-five mortality rate", "Mortality rate age 5-14"]
            )
        )
    ]
    assert len(tb_under_fifteen["indicator"].unique()) == 4

    # Combine datasets with a preference for the current data when there is a conflict - this is needed to calculate the youth mortality rate.

    tb_com = combine_datasets(
        tb_a=tb_under_fifteen, tb_b=tb_vintage, table_name="igme_combined", preferred_source="igme (current)"
    )
    tb_com = calculate_under_fifteen_deaths(tb_com)
    tb_com = calculate_under_fifteen_mortality_rates(tb_com)
    tb_com = tb_com.format(["country", "year", "indicator", "sex", "wealth_quintile", "unit_measure"])

    # Convert per 1000 live births to a percentage
    tb = convert_to_percentage(tb)
    # Calculate post neonatal deaths
    tb = add_post_neonatal_deaths(tb)
    tb = tb.drop(columns=["source"])
    tb = tb.format(["country", "year", "indicator", "sex", "wealth_quintile", "unit_measure"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_com], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def regional_aggregates_counts(tb: Table, ds_regions: Dataset, ds_population: Dataset, threshold: float = 0.8) -> Table:
    """Adds regional aggregates for count variables. Only includes year and regions where enough countries have data such that the population coverage is above the threshold.
    Returns: Table with regional aggregates for count variables. ONLY includes regions"""

    tb_counts = tb[tb["unit_measure"] == "Number of deaths"]

    tb_counts = geo.add_population_to_table(tb_counts, ds_population)
    tb_counts = tb_counts.dropna(subset=["population"])

    # add regions to table (summing obs_value, lower_bound and upper_bound and population)
    tb_counts = geo.add_regions_to_table(
        tb_counts,
        ds_regions,
        index_columns=["country", "year", "indicator", "sex", "unit_measure", "wealth_quintile"],
    )

    # filtering only on regions
    tb_counts = tb_counts[tb_counts["country"].isin(REGIONS)]

    # renaming population column and adding total population (for regions)
    tb_counts = tb_counts.rename(columns={"population": "population_covered"})
    tb_counts = geo.add_population_to_table(tb_counts, ds_population, population_col="total_population")

    # calculating share of population covered and filtering on years above threshold
    tb_counts["share_of_population"] = tb_counts["population_covered"] / tb_counts["total_population"]
    tb_counts = tb_counts[tb_counts["share_of_population"] >= threshold]

    tb_counts = tb_counts.drop(columns=["population_covered", "total_population", "share_of_population"])

    return tb_counts


def population_weighted_regional_averages(
    tb: Table, ds_population: Dataset, ds_regions: Dataset, threshold: float = 0.8
) -> Table:
    """Adds population-weighted averages of death rates for the regions. Only includes year and regions where enough countries have data such that the population coverage is above the threshold.
    Returns: Table with population-weighted averages of death rates for the regions. ONLY includes regions"""

    tb_rates = tb[tb["unit_measure"] != "Number of deaths"]

    # adding population to the table and dropping rows with missing population
    tb_rates = geo.add_population_to_table(tb_rates, ds_population)
    tb_rates = tb_rates.dropna(subset=["population"])

    # calculating column for population weighted death rates
    tb_rates["obs_value_pop"] = tb_rates["obs_value"] * tb_rates["population"]
    tb_rates["lower_bound_pop"] = tb_rates["lower_bound"] * tb_rates["population"]
    tb_rates["upper_bound_pop"] = tb_rates["upper_bound"] * tb_rates["population"]
    tb_rates = tb_rates.drop(columns=["lower_bound", "upper_bound", "obs_value"])

    # adding regions to the table (summing obs_value_pop, obs_value and population)
    tb_rates = geo.add_regions_to_table(
        tb_rates,
        ds_regions,
        index_columns=["country", "year", "indicator", "sex", "unit_measure", "wealth_quintile"],
    )

    # filtering only on regions
    tb_rates = tb_rates[tb_rates["country"].isin(REGIONS)]

    # renaming population column and adding total population (for regions)
    tb_rates = tb_rates.rename(columns={"population": "population_covered"})
    tb_rates = geo.add_population_to_table(tb_rates, ds_population, population_col="total_population")

    # calculating population weighted death rates & share of population covered
    tb_rates["obs_value"] = tb_rates["obs_value_pop"] / tb_rates["population_covered"]
    tb_rates["lower_bound"] = tb_rates["lower_bound_pop"] / tb_rates["population_covered"]
    tb_rates["upper_bound"] = tb_rates["upper_bound_pop"] / tb_rates["population_covered"]
    tb_rates["share_of_population"] = tb_rates["population_covered"] / tb_rates["total_population"]

    # filtering out regions where the share of population covered is below the threshold
    tb_rates = tb_rates[tb_rates["share_of_population"] >= threshold]

    # dropping unnecessary columns
    tb_rates = tb_rates.drop(
        columns=[
            "obs_value_pop",
            "lower_bound_pop",
            "upper_bound_pop",
            "population_covered",
            "total_population",
            "share_of_population",
        ]
    )

    return tb_rates


def convert_to_percentage(tb: Table) -> Table:
    """
    Convert the units which are given as 'per 1,000...' into percentages.
    """
    rate_conversions = {
        "Deaths per 1000 live births": "Deaths per 100 live births",
        "Deaths per 1000 children aged 1 month": "Deaths per 100 children aged 1 month",
        "Deaths per 1000 children aged 1": "Deaths per 100 children aged 1",
        "Deaths per 1000 children aged 5": "Deaths per 100 children aged 5",
        "Deaths per 1000 children aged 10": "Deaths per 100 children aged 10",
        "Deaths per 1000 children aged 15": "Deaths per 100 children aged 15",
        "Deaths per 1000 children aged 20": "Deaths per 100 children aged 20",
        "Stillbirths per 1000 total births": "Stillbirths per 100 births",
    }

    # Dividing values of selected rows by 10

    selected_rows = tb["unit_measure"].isin(rate_conversions.keys())
    assert all(
        key in tb["unit_measure"].values for key in rate_conversions.keys()
    ), "Not all keys are in tb['unit_measure']"
    tb.loc[selected_rows, ["obs_value", "lower_bound", "upper_bound"]] = tb.loc[
        selected_rows, ["obs_value", "lower_bound", "upper_bound"]
    ].div(10)

    tb = tb.replace({"unit_measure": rate_conversions})

    return tb


def add_post_neonatal_deaths(tb: Table) -> Table:
    """
    Calculate the deaths for the post-neonatal age-group, 28 days - 1 year
    """
    infant_deaths = tb[(tb["indicator"] == "Infant deaths")]
    neonatal_deaths = tb[(tb["indicator"] == "Neonatal deaths")]

    tb_merge = pr.merge(
        infant_deaths,
        neonatal_deaths,
        on=["country", "year", "wealth_quintile", "sex", "unit_measure", "source"],
        suffixes=("_infant", "_neonatal"),
    )
    tb_merge["obs_value"] = tb_merge["obs_value_infant"] - tb_merge["obs_value_neonatal"]
    tb_merge["lower_bound"] = tb_merge["lower_bound_infant"] - tb_merge["lower_bound_neonatal"]
    tb_merge["upper_bound"] = tb_merge["upper_bound_infant"] - tb_merge["upper_bound_neonatal"]
    tb_merge["indicator"] = "Post-neonatal deaths"
    result_tb = tb_merge[
        [
            "country",
            "year",
            "indicator",
            "sex",
            "unit_measure",
            "wealth_quintile",
            "obs_value",
            "lower_bound",
            "upper_bound",
            "source",
        ]
    ]
    # There are some cases where the neonatal deaths are greater than the infant deaths, so we need to set these to 0, e.g. for some years in Monaco.
    # Perhaps due to the transitory nature of the population.
    result_tb[["obs_value", "lower_bound", "upper_bound"]] = result_tb[
        ["obs_value", "lower_bound", "upper_bound"]
    ].clip(lower=0)
    assert all(result_tb["obs_value"] >= 0), "Negative values in post-neonatal deaths!"
    tb = pr.concat([tb, result_tb])

    return tb


def calculate_under_fifteen_deaths(tb: Table) -> Table:
    # Filter for under-five deaths
    tb_u5 = (
        tb[(tb["indicator"] == "Under-five deaths") & (tb["unit_measure"] == "Number of deaths")]
        .rename(columns={"obs_value": "obs_value_u5", "lower_bound": "lower_bound_u5", "upper_bound": "upper_bound_u5"})
        .drop(columns="indicator")
    )

    # Filter for deaths age 5 to 14
    tb_5_14 = (
        tb[(tb["indicator"] == "Deaths age 5 to 14") & (tb["unit_measure"] == "Number of deaths")]
        .rename(
            columns={
                "obs_value": "obs_value_5_14",
                "lower_bound": "lower_bound_5_14",
                "upper_bound": "upper_bound_5_14",
            }
        )
        .drop(columns="indicator")
    )

    # Merge both filtered tables
    tb_merge = pr.merge(tb_u5, tb_5_14, on=["country", "year", "sex", "wealth_quintile", "unit_measure", "source"])

    # Calculate the under-fifteen deaths
    tb_merge["obs_value"] = tb_merge["obs_value_u5"] + tb_merge["obs_value_5_14"]
    tb_merge["lower_bound"] = tb_merge["lower_bound_u5"] + tb_merge["lower_bound_5_14"]
    tb_merge["upper_bound"] = tb_merge["upper_bound_u5"] + tb_merge["upper_bound_5_14"]

    # Drop intermediate columns
    tb_merge = tb_merge.drop(
        columns=[
            "obs_value_u5",
            "obs_value_5_14",
            "lower_bound_u5",
            "lower_bound_5_14",
            "upper_bound_u5",
            "upper_bound_5_14",
        ]
    )

    # Add under-fifteen deaths as a new row
    tb_merge["indicator"] = "Under-fifteen deaths"
    # Combine with original data
    tb = pr.concat([tb, tb_merge], ignore_index=True)
    return tb


def calculate_under_fifteen_mortality_rates(tb_com: Table) -> Table:
    """
    Adjust and calculate the under-fifteen mortality rate by combining
    the under-five mortality rate with the mortality rate for ages 5-14.
    """
    # Filter common conditions in a single step
    common_conditions = (tb_com["sex"] == "Total") & (tb_com["wealth_quintile"].isin(["All wealth quintiles", "Total"]))

    # Filter and rename mortality data
    u5_mortality = tb_com[(tb_com["indicator"] == "Under-five mortality rate") & common_conditions]
    mortality_5_14 = tb_com[(tb_com["indicator"] == "Mortality rate age 5-14") & common_conditions]

    # Merge the two tables on common columns
    tb_merge = pr.merge(
        u5_mortality, mortality_5_14, on=["country", "year", "wealth_quintile", "sex"], suffixes=("_u5", "_5_14")
    )

    # Calculate the adjusted 5-14 mortality rate and combine with the under-five rate
    tb_merge["adjusted_5_14_mortality_rate"] = (1000 - tb_merge["obs_value_u5"]) / 1000 * tb_merge["obs_value_5_14"]
    tb_merge["obs_value"] = (tb_merge["obs_value_u5"] + tb_merge["adjusted_5_14_mortality_rate"]) / 10

    # Create the new indicator and unit of measure
    tb_merge["indicator"] = "Under-fifteen mortality rate"
    tb_merge["unit_measure"] = "Deaths per 100 live births"

    # Select the relevant columns
    result_tb = tb_merge[["country", "year", "indicator", "sex", "unit_measure", "wealth_quintile", "obs_value"]]

    # Combine with the original youth mortality table
    youth_mortality = tb_com[(tb_com["indicator"] == "Under-fifteen deaths") & common_conditions].drop(
        columns=["source", "lower_bound", "upper_bound"]
    )
    result_tb = pr.concat([youth_mortality, result_tb])

    # Add metadata (if needed)
    result_tb.metadata = tb_com.metadata  # Preserving metadata
    result_tb.metadata.short_name = "igme_under_fifteen_mortality"

    return result_tb


def remove_duplicates(tb: Table, preferred_source: str, dimensions: List[str]) -> Table:
    """
    Removing rows where there are overlapping years with a preference for IGME data.

    """
    assert any(tb["source"] == preferred_source)
    tb = tb.copy(deep=True)
    duplicate_rows = tb.duplicated(subset=dimensions, keep=False)

    tb_no_duplicates = tb[~duplicate_rows]

    tb_duplicates = tb[duplicate_rows]

    tb_duplicates_removed = tb_duplicates[tb_duplicates["source"] == preferred_source]

    tb = pr.concat([tb_no_duplicates, tb_duplicates_removed], ignore_index=True)

    assert len(tb[tb.duplicated(subset=dimensions, keep=False)]) == 0, "Duplicates still in table!"

    return tb


def combine_datasets(tb_a: Table, tb_b: Table, table_name: str, preferred_source: str) -> Table:
    """
    Combine two tables with a preference for one source of data.
    """
    tb_combined = pr.concat([tb_a, tb_b], short_name=table_name).sort_values(
        ["country", "year", "source"], ignore_index=True
    )
    assert any(tb_combined["source"] == preferred_source), "Preferred source not in table!"
    tb_combined = remove_duplicates(
        tb_combined,
        preferred_source=preferred_source,
        dimensions=["country", "year", "sex", "wealth_quintile", "indicator", "unit_measure"],
    )

    return tb_combined


def process_vintage_data(tb_youth: Table) -> Table:
    # Filter the data for relevant indicators
    tb_youth = tb_youth[
        tb_youth["indicator_name"].isin(["Deaths age 5 to 14", "Mortality rate age 5 to 14", "Under-5 mortality rate"])
    ]

    # Rename columns
    tb_youth = tb_youth.rename(
        columns={
            "indicator_name": "indicator",
            "sex_name": "sex",
            "unit_measure_name": "unit_measure",
            "obs_value": "Observation value",
            "lower_bound": "Lower bound",
            "upper_bound": "Upper bound",
        }
    ).drop(columns=["series_name_name"])

    # Assign new values to specific columns
    tb_youth["wealth_quintile"] = "Total"
    tb_youth["source"] = "igme (2018)"

    # Update the categories using cat.rename_categories for categorical columns
    tb_youth["indicator"] = tb_youth["indicator"].cat.rename_categories(
        {
            "Mortality rate age 5 to 14": "Mortality rate age 5-14",
            "Under-5 mortality rate": "Under-five mortality rate",
        }
    )

    tb_youth["unit_measure"] = tb_youth["unit_measure"].cat.rename_categories(
        {"Deaths per 1000 live births": "Deaths per 1,000 live births"}
    )

    # Rename columns back to standardized form
    tb_youth = tb_youth.rename(
        columns={"Observation value": "obs_value", "Lower bound": "lower_bound", "Upper bound": "upper_bound"}
    )

    return tb_youth


def filter_data(tb: Table) -> Table:
    """
    Filtering out the unnecessary columns and rows from the data.
    """
    # Keeping only the UN IGME estimates and the total wealth quintile
    cols_to_keep = [
        "country",
        "year",
        "indicator",
        "sex",
        "unit_measure",
        "wealth_quintile",
        "obs_value",
        "lower_bound",
        "upper_bound",
    ]
    # Keeping only the necessary columns.
    tb = tb[cols_to_keep]

    return tb


def round_down_year(tb: Table) -> Table:
    """
    Round down the year value given - to match what is shown on https://childmortality.org
    """

    tb["year"] = tb["year"].apply(trunc)

    return tb
