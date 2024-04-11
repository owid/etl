"""Process and harmonize EM-DAT natural disasters dataset.

NOTES:
1. We don't have population for some historical regions (e.g. East Germany, or North Yemen).
2. Some issues in the data were detected (see below, we may report them to EM-DAT). Some of them could not be fixed.
   Namely, some disasters affect, in one year, a number of people that is larger than the entire population.
   For example, the number of people affected by one drought event in Botswana 1981 is 1037300 while population
   was 982753. I suppose this could be due to inaccuracies in the estimates of affected people or in the population
   (which may not include people living temporarily in the country or visitors).
3. There are some potential issues that can't be fixed:
   * On the one hand, we may be underestimating the real impacts of events. The reason is that the original data does
   not include zeros. Therefore we can't know if the impacts of a certain event were zero, or unknown. Our only option
   is to treat missing data as zeros.
   * On the other hand, we may overestimate the real impacts on a given country-year, because events may affect the same
   people multiple times during the same year. This can't be fixed, but I suppose it's not common.
   * Additionally, it is understandable that some values are rough estimates, that some events are not recorded, and
   that there may be duplicated events.

"""

import datetime

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table, utils
from shared import (
    HISTORIC_TO_CURRENT_REGION,
    REGIONS,
    add_region_aggregates,
    correct_data_points,
    get_last_day_of_month,
)

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List of expected disaster types in the raw data to consider, and how to rename them.
# We consider only natural disasters of subgroups Geophysical, Meteorological, Hydrological and Climatological.
# We therefore ignore Extra-terrestrial (of which there is just one meteorite impact event) and Biological subgroups.
# For completeness, add all existing types here, and rename them as np.nan if they should not be used.
# If new types are included on a data update, simply add them here.
EXPECTED_DISASTER_TYPES = {
    "Animal incident": np.nan,
    "Drought": "Drought",
    "Earthquake": "Earthquake",
    "Epidemic": np.nan,
    "Extreme temperature": "Extreme temperature",
    "Flood": "Flood",
    "Fog": "Fog",
    "Glacial lake outburst flood": "Glacial lake outburst flood",
    "Impact": np.nan,
    "Infestation": np.nan,
    # "Landslide (dry)": "Landslide",
    "Mass movement (dry)": "Dry mass movement",
    "Mass movement (wet)": "Wet mass movement",
    "Storm": "Extreme weather",
    "Volcanic activity": "Volcanic activity",
    "Wildfire": "Wildfire",
}

# List of columns to select from raw data, and how to rename them.
COLUMNS = {
    "country": "country",
    "type": "type",
    "total_dead": "total_dead",
    "injured": "injured",
    "affected": "affected",
    "homeless": "homeless",
    "total_affected": "total_affected",
    "reconstruction_costs": "reconstruction_costs",
    "insured_damages": "insured_damages",
    "total_damages": "total_damages",
    "start_year": "start_year",
    "start_month": "start_month",
    "start_day": "start_day",
    "end_year": "end_year",
    "end_month": "end_month",
    "end_day": "end_day",
}

# Columns of values related to natural disaster impacts.
IMPACT_COLUMNS = [
    "total_dead",
    "injured",
    "affected",
    "homeless",
    "total_affected",
    "reconstruction_costs",
    "insured_damages",
    "total_damages",
]

# Variables related to costs, measured in thousand current US$ (not adjusted for inflation or PPP).
COST_VARIABLES = ["reconstruction_costs", "insured_damages", "total_damages"]

# Variables to calculate per 100,000 people.
VARIABLES_PER_100K_PEOPLE = [column for column in IMPACT_COLUMNS if column not in COST_VARIABLES] + ["n_events"]

# New natural disaster type corresponding to the sum of all disasters.
ALL_DISASTERS_TYPE = "all_disasters"

# List issues found in the data:
# Each element is a tuple with a dictionary that fully identifies the wrong row,
# and another dictionary that specifies the changes.
# Note: Countries here should appear as in the raw data (i.e. not harmonized).
DATA_CORRECTIONS = []


def prepare_input_data(tb: Table) -> Table:
    """Prepare input data, and fix some known issues."""
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Add a year column (assume the start of the event).
    tb["year"] = tb["start_year"].copy()

    # Correct wrong data points (defined above in DATA_CORRECTIONS).
    tb = correct_data_points(tb=tb, corrections=DATA_CORRECTIONS)

    # Remove spurious spaces in entities.
    tb["type"] = tb["type"].str.strip()

    # Sanity check
    error = "List of expected disaster types has changed. Consider updating EXPECTED_DISASTER_TYPES."
    assert set(tb["type"]) == set(EXPECTED_DISASTER_TYPES), error

    # Rename disaster types conveniently.
    tb["type"] = tb["type"].replace(EXPECTED_DISASTER_TYPES)

    # Drop rows for disaster types that are not relevant.
    tb = tb.dropna(subset="type").reset_index(drop=True)

    return tb


def sanity_checks_on_inputs(tb: Table) -> None:
    """Run sanity checks on input data."""
    error = "All values should be positive."
    assert (tb.select_dtypes("number").fillna(0) >= 0).all().all(), error

    error = "Column 'total_affected' should be the sum of columns 'injured', 'affected', and 'homeless'."
    assert (
        tb["total_affected"].fillna(0) >= tb[["injured", "affected", "homeless"]].sum(axis=1).fillna(0)
    ).all(), error

    error = "Natural disasters are not expected to last more than 9 years."
    assert (tb["end_year"] - tb["start_year"]).max() < 10, error

    error = "Some of the columns that can't have nan do have one or more nans."
    assert tb[["country", "year", "type", "start_year", "end_year"]].notnull().all().all(), error

    for column in ["year", "start_year", "end_year"]:
        error = f"Column '{column}' has a year prior to 1900 or posterior to current year."
        assert 1900 < tb[column].max() <= datetime.datetime.now().year, error

    error = "Some rows have end_day specified, but not end_month."
    assert tb[(tb["end_month"].isnull()) & (tb["end_day"].notnull())].empty, error


def fix_faulty_dtypes(tb: Table) -> Table:
    """Fix an issue related to column dtypes.

    Dividing a UInt32 by float64 results in a faulty Float64 that does not handle nans properly (which may be a bug:
    https://github.com/pandas-dev/pandas/issues/49818).
    To avoid this, there are various options:
    1. Convert all UInt32 into standard int before dividing by a float. But, if there are nans, int dtype is not valid.
    2. Convert all floats into Float64 before dividing.
    3. Convert all Float64 into float, after dividing.

    We adopt option 3.

    """
    tb = tb.astype({column: float for column in tb[tb.columns[tb.dtypes == "Float64"]]})

    return tb


def calculate_start_and_end_dates(tb: Table) -> Table:
    """Calculate start and end dates of disasters.

    The original data had year, month and day of start and end, and some of those fields were missing. This function
    deals with those missing fields and creates datetime columns for start and end of events.

    """
    tb = tb.copy()

    # When start month is not given, assume the beginning of the year.
    tb["start_month"] = tb["start_month"].fillna(1)
    # When start day is not given, assume the beginning of the month.
    tb["start_day"] = tb["start_day"].fillna(1)

    # When end month is not given, assume the end of the year.
    tb["end_month"] = tb["end_month"].fillna(12)

    # When end day is not given, assume the last day of the month.
    last_day_of_month = pd.Series(
        [get_last_day_of_month(year=row["end_year"], month=row["end_month"]) for i, row in tb.iterrows()]
    )
    tb["end_day"] = tb["end_day"].fillna(last_day_of_month)

    # Create columns for start and end dates.
    tb["start_date"] = (
        tb["start_year"].astype(str)
        + "-"
        + tb["start_month"].astype(str).str.zfill(2)
        + "-"
        + tb["start_day"].astype(str).str.zfill(2)
    )
    tb["end_date"] = (
        tb["end_year"].astype(str)
        + "-"
        + tb["end_month"].astype(str).str.zfill(2)
        + "-"
        + tb["end_day"].astype(str).str.zfill(2)
    )

    # Convert dates into datetime objects.
    # Note: This may fail if one of the dates is wrong, e.g. September 31 (if so, check error message for row index).
    tb["start_date"] = pd.to_datetime(tb["start_date"])
    tb["end_date"] = pd.to_datetime(tb["end_date"])

    error = "Events can't have an end_date prior to start_date."
    assert ((tb["end_date"] - tb["start_date"]).dt.days >= 0).all(), error

    # Drop unnecessary columns.
    tb = tb.drop(columns=["start_year", "start_month", "start_day", "end_year", "end_month", "end_day"])

    return tb


def calculate_yearly_impacts(tb: Table) -> Table:
    """Equally distribute the impact of disasters lasting longer than one year among the individual years, as separate
    events.

    Many disasters last more than one year. Therefore, we need to spread their impact among the different years.
    Otherwise, if we assign the impact of a disaster to, say, the first year, we may overestimate the impacts on a
    particular country-year.
    Hence, for events that started and ended in different years, we distribute their impact equally across the
    time spanned by the disaster.

    """
    tb = tb.copy()

    # There are many rows that have no data on impacts of disasters.
    # I suppose those are known disasters for which we don't know the impact.
    # Given that we want to count overall impact, fill them with zeros (to count them as disasters that had no victims).
    tb[IMPACT_COLUMNS] = tb[IMPACT_COLUMNS].fillna(0)

    # Select rows of disasters that last more than one year.
    multi_year_rows_mask = tb["start_date"].dt.year != tb["end_date"].dt.year
    multi_year_rows = tb[multi_year_rows_mask].reset_index(drop=True)

    # Go row by row, and create a new disaster event with the impact normalized by the fraction of days it happened
    # in a specific year.
    added_events = Table().copy_metadata(tb)
    for i, row in multi_year_rows.iterrows():
        # Start dataframe for new event.
        new_event = Table(row).transpose().copy_metadata(tb)
        # Years spanned by the disaster.
        years = np.arange(row["start_date"].year, row["end_date"].year + 1).tolist()
        # Calculate the total number of days spanned by the disaster (and add 1 day to include the day of the end date).
        days_total = (row["end_date"] + pd.DateOffset(1) - row["start_date"]).days

        for year in years:
            if year == years[0]:
                # Get number of days.
                days_affected_in_year = (pd.Timestamp(year=year + 1, month=1, day=1) - row["start_date"]).days
                # Fraction of days affected this year.
                days_fraction = days_affected_in_year / days_total
                # Impacts this years.
                impacts = (row[IMPACT_COLUMNS] * days_fraction).astype(int)  # type: ignore
                # Start a series that counts the impacts accumulated over the years.
                cumulative_impacts = impacts
                # Normalize data by the number of days affected in this year.
                new_event[IMPACT_COLUMNS] = impacts
                # Correct dates.
                new_event["end_date"] = pd.Timestamp(year=year, month=12, day=31)
            elif years[0] < year < years[-1]:
                # The entire year was affected by the disaster.
                # Note: Ignore leap years.
                days_fraction = 365 / days_total
                # Impacts this year.
                impacts = (row[IMPACT_COLUMNS] * days_fraction).astype(int)  # type: ignore
                # Add impacts to the cumulative impacts series.
                cumulative_impacts += impacts  # type: ignore
                # Normalize data by the number of days affected in this year.
                new_event[IMPACT_COLUMNS] = impacts
                # Correct dates.
                new_event["start_date"] = pd.Timestamp(year=year, month=1, day=1)
                new_event["end_date"] = pd.Timestamp(year=year, month=12, day=31)
            else:
                # Assign all remaining impacts to the last year.
                impacts = row[IMPACT_COLUMNS] - cumulative_impacts  # type: ignore
                new_event[IMPACT_COLUMNS] = impacts
                # Correct dates.
                new_event["start_date"] = pd.Timestamp(year=year, month=1, day=1)
            added_events = pr.concat([added_events, new_event], ignore_index=True)

    # Remove multi-year rows from main dataframe, and add those rows after separating events year by year.
    tb_yearly = pr.concat([tb[~(multi_year_rows_mask)], added_events], ignore_index=True)  # type: ignore

    # Sort conveniently.
    tb_yearly = tb_yearly.sort_values(["country", "year", "type"]).reset_index(drop=True)

    return tb_yearly


def get_total_count_of_yearly_impacts(tb: Table) -> Table:
    """Get the total count of impacts in the year, ignoring the individual events.

    We are not interested in each individual event, but the number of events of each kind and their impacts.
    This function will produce the total count of impacts per country, year and type of disaster.

    """
    # Get the total count of impacts per country, year and type of disaster.
    counts = (
        tb.reset_index()
        .groupby(["country", "year", "type"], observed=True)
        .agg({"index": "count"})
        .reset_index()
        .rename(columns={"index": "n_events"})
    )
    # Copy metadata from any other column into the new column of counts of events.
    counts["n_events"] = counts["n_events"].copy_metadata(tb["total_dead"])
    # Get the sum of impacts per country, year and type of disaster.
    tb = tb.groupby(["country", "year", "type"], observed=True).sum(numeric_only=True, min_count=1).reset_index()
    # Add the column of the number of events.
    tb = tb.merge(counts, on=["country", "year", "type"], how="left")

    return tb


def create_a_new_type_for_all_disasters_combined(tb: Table) -> Table:
    """Add a new disaster type that has the impact of all other disasters combined."""
    all_disasters = (
        tb.groupby(["country", "year"], observed=True)
        .sum(numeric_only=True, min_count=1)
        .assign(**{"type": ALL_DISASTERS_TYPE})
        .reset_index()
    )
    tb = (
        pr.concat([tb, all_disasters], ignore_index=True)
        .sort_values(["country", "year", "type"])
        .reset_index(drop=True)
    )

    return tb


def create_additional_variables(tb: Table, ds_population: Dataset, tb_gdp: Table) -> Table:
    """Create additional variables, namely damages per GDP, and impacts per 100,000 people."""
    # Add population to table.
    tb = geo.add_population_to_table(
        tb=tb, ds_population=ds_population, expected_countries_without_population=["North Yemen", "South Yemen"]
    )

    # Combine natural disasters with GDP data.
    tb = tb.merge(tb_gdp.rename(columns={"ny_gdp_mktp_cd": "gdp"}), on=["country", "year"], how="left")
    # Prepare cost variables.
    for variable in COST_VARIABLES:
        # Convert costs (given in '000 US$, aka thousand current US$) into current US$.
        tb[variable] *= 1000
        # Create variables of costs (in current US$) as a share of GDP (in current US$).
        tb[f"{variable}_per_gdp"] = tb[variable] / tb["gdp"] * 100

    # Add rates per 100,000 people.
    for column in VARIABLES_PER_100K_PEOPLE:
        tb[f"{column}_per_100k_people"] = tb[column] * 1e5 / tb["population"]

    # Fix issue with faulty dtypes (see more details in the function's documentation).
    tb = fix_faulty_dtypes(tb=tb)

    return tb


def create_decade_data(tb: Table) -> Table:
    """Create data of average impacts over periods of 10 years.

    For example (as explained in the footer of the natural disasters explorer), the value for 1900 of any column should
    represent the average of that column between 1900 and 1909.

    """
    tb_decadal = tb.copy()

    # Ensure each country has data for all years (and fill empty rows with zeros).
    # Otherwise, the average would only be performed only across years for which we have data.
    # For example, if we have data only for 1931 (and no other year in the 1930s) we want that data point to be averaged
    # over all years in the decade (assuming they are all zero).
    # Note that, for the current decade, since it's not complete, we want to average over the number of current years
    # (not the entire decade).

    # List all countries, years and types in the data.
    countries = sorted(set(tb_decadal["country"]))
    years = np.arange(tb_decadal["year"].min(), tb_decadal["year"].max() + 1).tolist()
    types = sorted(set(tb_decadal["type"]))

    # Create a new index covering all combinations of countries, years and types.
    new_indexes = pd.MultiIndex.from_product([countries, years, types], names=["country", "year", "type"])

    # Reindex data so that all countries and types have data for each year (filling with zeros when there's no data).
    tb_decadal = tb_decadal.set_index(["country", "year", "type"]).reindex(new_indexes, fill_value=0).reset_index()

    # For each year, calculate the corresponding decade (e.g. 1951 -> 1950, 1929 -> 1920).
    tb_decadal["decade"] = (tb_decadal["year"] // 10) * 10

    # Group by that country-decade-type and get the mean for each column.
    tb_decadal = (
        tb_decadal.drop(columns=["year"])
        .groupby(["country", "decade", "type"], observed=True)
        .mean(numeric_only=True)
        .reset_index()
        .rename(columns={"decade": "year"})
    )

    return tb_decadal


def sanity_checks_on_outputs(tb: Table, is_decade: bool) -> None:
    """Run sanity checks on output (yearly or decadal) data.

    Parameters
    ----------
    tb : Table
        Output (yearly or decadal) data.
    is_decade : bool
        True if tb is decadal data; False if it is yearly data.

    """
    # Common sanity checks for yearly and decadal data.
    error = "All values should be positive."
    assert (tb.select_dtypes("number").fillna(0) >= 0).all().all(), error

    error = (
        "List of expected disaster types has changed. "
        "Consider updating EXPECTED_DISASTER_TYPES (or renaming ALL_DISASTERS_TYPE)."
    )
    expected_disaster_types = [ALL_DISASTERS_TYPE] + [
        utils.underscore(EXPECTED_DISASTER_TYPES[disaster])
        for disaster in EXPECTED_DISASTER_TYPES
        if not pd.isna(EXPECTED_DISASTER_TYPES[disaster])
    ]
    assert set(tb["type"]) == set(expected_disaster_types), error

    columns_that_should_not_have_nans = [
        "country",
        "year",
        "type",
        "total_dead",
        "injured",
        "affected",
        "homeless",
        "total_affected",
        "reconstruction_costs",
        "insured_damages",
        "total_damages",
        "n_events",
    ]
    error = "There are unexpected nans in data."
    assert tb[columns_that_should_not_have_nans].notnull().all(axis=1).all(), error

    # Sanity checks only for yearly data.
    if not is_decade:
        all_countries = sorted(set(tb["country"]) - set(REGIONS) - set(HISTORIC_TO_CURRENT_REGION))

        # Check that the aggregate of all countries and disasters leads to the same numbers we have for the world.
        # This check would not pass when adding historical regions (since we know there are some overlaps between data
        # from historical and successor countries). So check for a specific year.
        year_to_check = 2022
        all_disasters_for_world = tb[
            (tb["country"] == "World") & (tb["year"] == year_to_check) & (tb["type"] == ALL_DISASTERS_TYPE)
        ].reset_index(drop=True)
        all_disasters_check = (
            tb[(tb["country"].isin(all_countries)) & (tb["year"] == year_to_check) & (tb["type"] != ALL_DISASTERS_TYPE)]
            .groupby("year")
            .sum(numeric_only=True)
            .reset_index()
        )

        cols_to_check = [
            "total_dead",
            "injured",
            "affected",
            "homeless",
            "total_affected",
            "reconstruction_costs",
            "insured_damages",
            "total_damages",
        ]
        error = f"Aggregate for the World in {year_to_check} does not coincide with the sum of all countries."
        assert all_disasters_for_world[cols_to_check].equals(all_disasters_check[cols_to_check]), error

        error = "Column 'total_affected' should be the sum of columns 'injured', 'affected', and 'homeless'."
        assert (
            tb["total_affected"].fillna(0) >= tb[["injured", "affected", "homeless"]].sum(axis=1).fillna(0)
        ).all(), error

        # Another sanity check would be that certain disasters (e.g. an earthquake) cannot last for longer than 1 day.
        # However, for some disasters we don't have exact day, or even exact month, just the year.

        # List of columns whose value should not be larger than population.
        columns_to_inspect = [
            "total_dead",
            "total_dead_per_100k_people",
        ]
        error = "One disaster should not be able to cause the death of the entire population of a country in one year."
        for column in columns_to_inspect:
            informed_rows = tb[column].notnull() & tb["population"].notnull()
            assert (tb[informed_rows][column] <= tb[informed_rows]["population"]).all(), error


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load natural disasters dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("natural_disasters")
    tb_meadow = ds_meadow["natural_disasters"].reset_index()

    # Load WDI dataset, read its main table and select variable corresponding to GDP (in current US$).
    ds_wdi = paths.load_dataset("wdi")
    tb_gdp = ds_wdi["wdi"][["ny_gdp_mktp_cd"]].reset_index()

    ####################################################################################################################
    # TODO: Remote this temporary solution once WDI has origins.
    from etl.data_helpers.misc import add_origins_to_wdi

    tb_gdp = add_origins_to_wdi(tb_wdi=tb_gdp)
    ####################################################################################################################

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Prepare input data (and fix some known issues).
    tb = prepare_input_data(tb=tb_meadow)

    # Sanity checks.
    sanity_checks_on_inputs(tb=tb)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, warn_on_missing_countries=True, warn_on_unused_countries=True
    )

    # Calculate start and end dates of disasters.
    tb = calculate_start_and_end_dates(tb=tb)

    # Distribute the impacts of disasters lasting longer than a year among separate yearly events.
    tb = calculate_yearly_impacts(tb=tb)

    # Get total count of impacts per year (regardless of the specific individual events during the year).
    tb = get_total_count_of_yearly_impacts(tb=tb)

    # Add a new category (or "type") corresponding to the total of all natural disasters.
    tb = create_a_new_type_for_all_disasters_combined(tb=tb)

    # Add region aggregates.
    tb = add_region_aggregates(
        data=tb,
        index_columns=["country", "year", "type"],
        regions_to_add=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
    )

    # Add damages per GDP, and rates per 100,000 people.
    tb = create_additional_variables(tb=tb, ds_population=ds_population, tb_gdp=tb_gdp)

    # Change disaster types to snake, lower case.
    tb["type"] = tb["type"].replace({value: utils.underscore(value) for value in tb["type"].unique()})

    # Create data aggregated (using a simple mean) in intervals of 10 years.
    tb_decadal = create_decade_data(tb=tb)

    # Run sanity checks on output yearly data.
    sanity_checks_on_outputs(tb=tb, is_decade=False)

    # Run sanity checks on output decadal data.
    sanity_checks_on_outputs(tb=tb_decadal, is_decade=True)

    # Set an appropriate index to yearly data and sort conveniently.
    tb = tb.format(keys=["country", "year", "type"], sort_columns=True)

    # Set an appropriate index to decadal data and sort conveniently.
    tb_decadal = tb_decadal.format(keys=["country", "year", "type"], sort_columns=True)

    # Rename yearly and decadal tables.
    tb.metadata.short_name = "natural_disasters_yearly"
    tb_decadal.metadata.short_name = "natural_disasters_decadal"

    #
    # Save outputs.
    #
    # Create new Garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_decadal], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()
