"""Load a meadow dataset and create a garden dataset."""

from math import trunc

import structlog
from igme_helpers import check_expected_changes
from owid.catalog import Table
from owid.catalog import processing as pr
from tqdm import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder

LOG = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = geo.REGIONS


def run() -> None:
    #
    # Load inputs.
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("igme", version=paths.version)
    # Load vintage dataset which has older data needed for youth mortality
    ds_vintage = paths.load_dataset("igme", version="2018")

    # Read table from meadow dataset.
    tb = ds_meadow.read("igme")
    tb_vintage = ds_vintage["igme"].reset_index()
    tb_vintage = process_vintage_data(tb_vintage)
    tb_vintage["unit_of_measure"] = tb_vintage["unit_of_measure"].str.replace(",", "", regex=False)
    tb_vintage = tb_vintage.rename(columns={"obs_value": "observation_value"})

    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # Filter out just the bits of the data we want
    tb = filter_data(tb)
    tb = round_down_year(tb)

    # get regional data for count variables
    tb_counts_regions = regional_aggregates_counts(tb, threshold=0.8)
    # get regional population weighted averages for rate variables
    # gets weighted averages based on original denominators
    tb_rates_regions = regional_averages_by_denominator(tb, threshold=0.8)

    # Remove any OWID region rows from the country-level data before adding back
    # our own regional aggregates (source data may contain regional entries from UN
    # that collide with OWID region names, e.g. income groups).
    tb = tb[~tb["country"].isin(REGIONS)]

    # Adding regional aggregates to table
    tb = pr.concat([tb, tb_counts_regions, tb_rates_regions])

    # Removing commas from the unit of measure
    tb["unit_of_measure"] = tb["unit_of_measure"].str.replace(",", "", regex=False)
    tb["source"] = "igme (current)"

    # Separate out the variables needed to calculate the under-fifteen mortality rate.
    tb_under_fifteen = tb[
        (
            tb["indicator"].isin(
                ["Under-five deaths", "Deaths age 5 to 14", "Under-five mortality rate", "Mortality rate age 5-14"]
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
    tb_com = tb_com.drop(columns=["sex", "wealth_quintile"])
    tb_com = tb_com.format(
        ["country", "year", "indicator", "unit_of_measure"],
        short_name="igme_under_fifteen_mortality",
    )

    # Convert per 1000 live births to a percentage
    tb = convert_to_percentage(tb)
    # Calculate post neonatal deaths
    tb = add_post_neonatal_deaths(tb)
    # Remove 'Number of' prefix from "Number of deaths" and "Number of stillbirths"

    tb["unit_of_measure"] = tb["unit_of_measure"].str.replace("Number of deaths", "Deaths", regex=False)
    tb["unit_of_measure"] = tb["unit_of_measure"].str.replace("Number of stillbirths", "Stillbirths", regex=False)
    tb["unit_of_measure"] = tb["unit_of_measure"].str.replace(
        "Stillbirths per 100 births", "Deaths per 100 births", regex=False
    )

    tb["indicator"] = tb["indicator"].str.replace("Under-five mortality rate", "Child mortality rate", regex=False)
    # Drop unused columns
    tb = tb.drop(columns=["source", "lower_bound", "upper_bound"])

    # sanity check (uncomment when updating)
    sanity_checks(tb)

    tb = tb.format(
        ["country", "year", "indicator", "sex", "wealth_quintile", "unit_of_measure"],
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_com], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def check_rapid_changes(tb: Table, country: str, indicator: str, sex, wealth_quintile, threshold) -> None:
    """
    Check for rapid increases/ decreases in the observation_value column for a given country and indicator.
    A rapid increase/ decrease is defined as a change of more than the threshold percentage from one year to the next.
    """

    tb_country_indicator = tb[
        (tb["country"] == country)
        & (tb["indicator"] == indicator)
        & (tb["sex"] == sex)
        & (tb["wealth_quintile"] == wealth_quintile)
    ].sort_values("year")

    if len(tb_country_indicator) < 2:
        return None

    # if the indicator is total deaths or stillbirths, check that average is over 500, otherwise we might be picking up rapid changes in small populations which are not necessarily errors.
    # this is the case if the unit is "Deaths" or "Stillbirths"
    unit = tb_country_indicator["unit_of_measure"].iloc[0]
    if unit in ["Deaths", "Stillbirths"] and tb_country_indicator["observation_value"].mean() < 100:
        # LOG.info(
        #    f"Skipping rapid change check for {indicator} in {country} for {sex} and {wealth_quintile} (small population)"
        # )
        return None

    tb_country_indicator["pct_change"] = tb_country_indicator["observation_value"].pct_change()
    rapid_changes = tb_country_indicator[abs(tb_country_indicator["pct_change"]) > threshold]

    rapid_changes = check_expected_changes(rapid_changes, country)

    if not rapid_changes.empty:
        LOG.warning(
            f"Rapid changes found in {indicator} for {country}: {rapid_changes[['year', 'observation_value', 'pct_change']]}"
        )
    return None


def sanity_checks(tb: Table) -> None:
    """
    Perform sanity checks on the data.
    """
    # Check that there are no negative values in the observation_value column.
    assert (tb["observation_value"] >= 0).all(), "Negative values in observation_value column!"

    # Check that the year column is of type int and has reasonable values.
    assert tb["year"].dtype == int, "Year column is not of type int!"
    assert (tb["year"] >= 1930).all() and (tb["year"] <= 2025).all(), (
        f"Year column has values <1930 {tb[tb['year'] < 1930]} or >2025! {tb[tb['year'] > 2025]}"
    )

    # Check that percentage values are between 0 and 100.
    percentage_units = [unit for unit in tb["unit_of_measure"].unique() if "per 100" in unit]
    percentage_rows = tb["unit_of_measure"].isin(percentage_units)
    msk_per = percentage_rows & ((tb["observation_value"] < 0) | (tb["observation_value"] > 100))
    assert not msk_per.any(), (
        f"Percentage values in observation_value column are not between 0 and 100: {tb.loc[msk_per]}"
    )

    # Check for rapid increases/ decreases in the observation_value column for each country and indicator.
    # wrap in tqdm to track progress, as this can take a while given the number of combinations of country, indicator

    for country in tqdm(tb["country"].unique()):
        for indicator in tb["indicator"].unique():
            for sex in tb["sex"].unique():
                for wealth_quintile in tb["wealth_quintile"].unique():
                    check_rapid_changes(tb, country, indicator, sex, wealth_quintile, threshold=0.3)

    return None


def regional_aggregates_counts(tb: Table, threshold: float = 0.8) -> Table:
    """Adds regional aggregates for count variables. Only includes year and regions where enough countries have data such that the population coverage is above the threshold.
    Returns: Table with regional aggregates for count variables. ONLY includes regions"""

    units_deaths = ["Number of deaths", "Number of stillbirths"]

    tb_counts = tb[tb["unit_of_measure"].isin(units_deaths)]

    tb_counts = paths.regions.add_population(tb_counts)
    tb_counts = tb_counts.dropna(subset=["population"])

    # add regions to table (summing observation_value, lower_bound and upper_bound and population)
    tb_counts = paths.regions.add_aggregates(
        tb_counts,
        index_columns=["country", "year", "indicator", "sex", "unit_of_measure", "wealth_quintile"],
    )

    # filtering only on regions
    tb_counts = tb_counts[tb_counts["country"].isin(REGIONS)]

    # renaming population column and adding total population (for regions)
    tb_counts = tb_counts.rename(columns={"population": "population_covered"})
    tb_counts = paths.regions.add_population(tb_counts, population_col="total_population")

    # calculating share of population covered and filtering on years above threshold
    tb_counts["share_of_population"] = tb_counts["population_covered"] / tb_counts["total_population"]
    tb_counts = tb_counts[tb_counts["share_of_population"] >= threshold]

    tb_counts = tb_counts.drop(columns=["population_covered", "total_population", "share_of_population"])

    return tb_counts


def get_absolute_death_number(death_lookup, rate_indicator, country, year, sex, wealth_quintile, indicator_mapping):
    death_indicator = indicator_mapping[rate_indicator]
    death_row = death_lookup.get((country, year, sex, wealth_quintile, death_indicator))
    if death_row is not None:
        return death_row
    else:
        # LOG.warning(f"No data found for {death_indicator} in {country}, {year}, {sex}, {wealth_quintile}")
        return None


def regional_averages_by_denominator(tb: Table, threshold: float = 0.8) -> Table:
    """Adds averages of death rates (based on original denominators) for the regions.

    Only includes year and regions where enough countries have data such that the population coverage is above the threshold.
    Returns: Table with death rate averages of death rates for the regions. ONLY includes regions"""

    units_deaths = ["Number of deaths", "Number of stillbirths"]

    tb_deaths = tb[tb["unit_of_measure"].isin(units_deaths)]

    tb_rates = tb[~(tb["unit_of_measure"].isin(units_deaths))]

    unit_to_death_mapping = {
        "Child Mortality rate age 1-4": "Child deaths age 1 to 4",
        "Infant mortality rate": "Infant deaths",
        "Under-five mortality rate": "Under-five deaths",
        "Mortality rate 1-59 months": "Deaths age 1-59 months",
        "Mortality rate age 1-11 months": "Deaths age 1-11 months",
        "Neonatal mortality rate": "Neonatal deaths",
        "Mortality rate age 10-14": "Deaths age 10 to 14",
        "Mortality rate age 10-19": "Deaths age 10 to 19",
        "Mortality rate age 15-19": "Deaths age 15 to 19",
        "Mortality rate age 15-24": "Deaths age 15 to 24",
        "Mortality rate age 20-24": "Deaths age 20 to 24",
        "Mortality rate age 5-14": "Deaths age 5 to 14",
        "Mortality rate age 5-24": "Deaths age 5 to 24",
        "Mortality rate age 5-9": "Deaths age 5 to 9",
        "Stillbirth rate": "Stillbirths",
    }

    # create a lookup table for the absolute number of deaths based on the rate indicators
    tb_death_lookup = tb_deaths.set_index(["country", "year", "sex", "wealth_quintile", "indicator"])[
        "observation_value"
    ]

    tb_rates["absolute_deaths"] = tb_rates.apply(
        lambda row: get_absolute_death_number(
            death_lookup=tb_death_lookup,
            rate_indicator=row["indicator"],
            country=row["country"],
            year=row["year"],
            sex=row["sex"],
            wealth_quintile=row["wealth_quintile"],
            indicator_mapping=unit_to_death_mapping,
        ),
        axis=1,
    )

    tb_rates["inferred_denominator"] = tb_rates["absolute_deaths"] / tb_rates["observation_value"]

    # adding population to the table and dropping rows with missing population
    tb_rates = paths.regions.add_population(tb_rates)
    tb_rates = tb_rates.dropna(subset=["population"])

    # calculating column for population weighted death rates
    tb_rates["observation_value_denom"] = tb_rates["observation_value"] * tb_rates["inferred_denominator"]
    tb_rates["lower_bound_denom"] = tb_rates["lower_bound"] * tb_rates["inferred_denominator"]
    tb_rates["upper_bound_denom"] = tb_rates["upper_bound"] * tb_rates["inferred_denominator"]
    tb_rates = tb_rates.drop(columns=["lower_bound", "upper_bound", "observation_value"])

    # adding regions to the table (summing observation_value_denom, lower_bound_denom, upper_bound_denom, and population)
    tb_rates = paths.regions.add_aggregates(
        tb_rates,
        index_columns=["country", "year", "indicator", "sex", "unit_of_measure", "wealth_quintile"],
    )

    # filtering only on regions
    tb_rates = tb_rates[tb_rates["country"].isin(REGIONS)]

    # renaming population column and adding total population (for regions)
    tb_rates = tb_rates.rename(columns={"population": "population_covered"})
    tb_rates = paths.regions.add_population(tb_rates, population_col="total_population")

    # calculating population weighted death rates & share of population covered
    tb_rates["observation_value"] = tb_rates["observation_value_denom"] / tb_rates["inferred_denominator"]
    tb_rates["lower_bound"] = tb_rates["lower_bound_denom"] / tb_rates["inferred_denominator"]
    tb_rates["upper_bound"] = tb_rates["upper_bound_denom"] / tb_rates["inferred_denominator"]
    tb_rates["share_of_population"] = tb_rates["population_covered"] / tb_rates["total_population"]

    # filtering out regions where the share of population covered is below the threshold
    tb_rates = tb_rates[tb_rates["share_of_population"] >= threshold]

    # dropping unnecessary columns
    tb_rates = tb_rates.drop(
        columns=[
            "observation_value_denom",
            "lower_bound_denom",
            "upper_bound_denom",
            "population_covered",
            "total_population",
            "share_of_population",
            "inferred_denominator",
            "absolute_deaths",
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
        "Deaths per 1000 youths aged 15": "Deaths per 100 children aged 15",
        "Deaths per 1000 youths aged 20": "Deaths per 100 children aged 20",
        "Stillbirths per 1000 total births": "Stillbirths per 100 births",
    }

    # Dividing values of selected rows by 10

    selected_rows = tb["unit_of_measure"].isin(rate_conversions.keys())
    assert all(key in tb["unit_of_measure"].values for key in rate_conversions.keys()), (
        "Not all keys are in tb['unit_of_measure']"
    )
    tb.loc[selected_rows, ["observation_value", "lower_bound", "upper_bound"]] = tb.loc[
        selected_rows, ["observation_value", "lower_bound", "upper_bound"]
    ].div(10)

    tb = tb.replace({"unit_of_measure": rate_conversions})

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
        on=["country", "year", "wealth_quintile", "sex", "unit_of_measure", "source"],
        suffixes=("_infant", "_neonatal"),
    )
    tb_merge["observation_value"] = tb_merge["observation_value_infant"] - tb_merge["observation_value_neonatal"]
    tb_merge["lower_bound"] = tb_merge["lower_bound_infant"] - tb_merge["lower_bound_neonatal"]
    tb_merge["upper_bound"] = tb_merge["upper_bound_infant"] - tb_merge["upper_bound_neonatal"]
    tb_merge["indicator"] = "Post-neonatal deaths"
    result_tb = tb_merge[
        [
            "country",
            "year",
            "indicator",
            "sex",
            "unit_of_measure",
            "wealth_quintile",
            "observation_value",
            "lower_bound",
            "upper_bound",
            "source",
        ]
    ]
    # There are some cases where the neonatal deaths are greater than the infant deaths, so we need to set these to 0, e.g. for some years in Monaco.
    # Perhaps due to the transitory nature of the population.
    result_tb[["observation_value", "lower_bound", "upper_bound"]] = result_tb[
        ["observation_value", "lower_bound", "upper_bound"]
    ].clip(lower=0)
    assert all(result_tb["observation_value"] >= 0), "Negative values in post-neonatal deaths!"
    tb = pr.concat([tb, result_tb])

    return tb


def calculate_under_fifteen_deaths(tb: Table) -> Table:
    # Filter for under-five deaths
    tb_u5 = (
        tb[(tb["indicator"] == "Under-five deaths") & (tb["unit_of_measure"] == "Number of deaths")]
        .rename(
            columns={
                "observation_value": "observation_value_u5",
                "lower_bound": "lower_bound_u5",
                "upper_bound": "upper_bound_u5",
            }
        )
        .drop(columns="indicator")
    )

    # Filter for deaths age 5 to 14
    tb_5_14 = (
        tb[(tb["indicator"] == "Deaths age 5 to 14") & (tb["unit_of_measure"] == "Number of deaths")]
        .rename(
            columns={
                "observation_value": "observation_value_5_14",
                "lower_bound": "lower_bound_5_14",
                "upper_bound": "upper_bound_5_14",
            }
        )
        .drop(columns="indicator")
    )

    # Merge both filtered tables
    tb_merge = pr.merge(tb_u5, tb_5_14, on=["country", "year", "sex", "wealth_quintile", "unit_of_measure", "source"])

    # Calculate the under-fifteen deaths
    tb_merge["observation_value"] = tb_merge["observation_value_u5"] + tb_merge["observation_value_5_14"]
    tb_merge["lower_bound"] = tb_merge["lower_bound_u5"] + tb_merge["lower_bound_5_14"]
    tb_merge["upper_bound"] = tb_merge["upper_bound_u5"] + tb_merge["upper_bound_5_14"]

    # Drop intermediate columns
    tb_merge = tb_merge.drop(
        columns=[
            "observation_value_u5",
            "observation_value_5_14",
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
    tb["unit_of_measure"] = "Deaths"
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
    tb_merge["adjusted_5_14_mortality_rate"] = (
        (1000 - tb_merge["observation_value_u5"]) / 1000 * tb_merge["observation_value_5_14"]
    )
    tb_merge["observation_value"] = (tb_merge["observation_value_u5"] + tb_merge["adjusted_5_14_mortality_rate"]) / 10

    # Create the new indicator and unit of measure
    tb_merge["indicator"] = "Under-fifteen mortality rate"
    tb_merge["unit_of_measure"] = "Deaths per 100 live births"

    # Select the relevant columns
    result_tb = tb_merge[
        ["country", "year", "indicator", "sex", "unit_of_measure", "wealth_quintile", "observation_value"]
    ]

    # Combine with the original youth mortality table
    youth_mortality = tb_com[(tb_com["indicator"] == "Under-fifteen deaths") & common_conditions].drop(
        columns=["source", "lower_bound", "upper_bound"]
    )
    result_tb = pr.concat([youth_mortality, result_tb])

    # Add metadata (if needed)
    result_tb.metadata = tb_com.metadata  # Preserving metadata

    return result_tb


def remove_duplicates(tb: Table, preferred_source: str, dimensions: list[str]) -> Table:
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
        dimensions=["country", "year", "sex", "wealth_quintile", "indicator", "unit_of_measure"],
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
            "unit_measure_name": "unit_of_measure",
            "observation_value": "Observation value",
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

    tb_youth["unit_of_measure"] = tb_youth["unit_of_measure"].cat.rename_categories(
        {"Deaths per 1000 live births": "Deaths per 1,000 live births"}
    )

    # Rename columns back to standardized form
    tb_youth = tb_youth.rename(
        columns={"Observation value": "observation_value", "Lower bound": "lower_bound", "Upper bound": "upper_bound"}
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
        "unit_of_measure",
        "wealth_quintile",
        "observation_value",
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
