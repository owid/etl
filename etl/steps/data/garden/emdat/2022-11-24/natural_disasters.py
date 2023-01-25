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
import pandas as pd
from owid import catalog
from shared import (
    BUILD_POPULATION_FOR_HISTORICAL_COUNTRIES,
    CURRENT_DIR,
    EXPECTED_COUNTRIES_WITHOUT_POPULATION,
    HISTORIC_TO_CURRENT_REGION,
    REGIONS,
    add_population,
    add_region_aggregates,
    correct_data_points,
    get_last_day_of_month,
)

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

# Define inputs.
MEADOW_VERSION = "2022-11-24"
WDI_DATASET_PATH = DATA_DIR / "garden/worldbank_wdi/2022-05-26/wdi"
# Define outputs.
VERSION = MEADOW_VERSION

# List of expected disaster types in the raw data to consider, and how to rename them.
# We consider only natural disasters of subgroups Geophysical, Meteorological, Hydrological and Climatological.
# We therefore ignore Extra-terrestrial (of which there is just one meteorite impact event) and Biological subgroups.
# For completeness, add all existing types here, and rename them as np.nan if they should not be used.
# If new types are included on a data update, simply add them here.
EXPECTED_DISASTER_TYPES = {
    "Animal accident": np.nan,
    "Drought": "Drought",
    "Earthquake": "Earthquake",
    "Epidemic": np.nan,
    "Extreme temperature": "Extreme temperature",
    "Flood": "Flood",
    "Fog": "Fog",
    "Glacial lake outburst": "Glacial lake outburst",
    "Impact": np.nan,
    "Insect infestation": np.nan,
    "Landslide": "Landslide",
    "Mass movement (dry)": "Dry mass movement",
    "Storm": "Extreme weather",
    "Volcanic activity": "Volcanic activity",
    "Wildfire": "Wildfire",
}

# List of columns to select from raw data, and how to rename them.
COLUMNS = {
    "country": "country",
    "year": "year",
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
DATA_CORRECTIONS = [
    # The end year of 1969 Morocco earthquake can't be 2019.
    ({"country": "Morocco", "start_year": 1969, "end_year": 2019, "type": "Earthquake"}, {"end_year": 1969}),
    # The date of the 1992 Afghanistan flood can't be September 31.
    ({"country": "Afghanistan", "start_year": 1992, "start_month": 9, "start_day": 31}, {"start_day": 3, "end_day": 3}),
    # The date of the 1992 India flood can't be September 31.
    # Also, there is one entry for 1992 India flood on 1992-09-08 (500 dead) and another for 1992-09 (86 dead).
    # They will be treated as separate events (maybe the monthly one refers to other smaller floods that month?).
    ({"country": "India", "start_year": 1992, "start_month": 9, "start_day": 8, "end_day": 31}, {"end_day": 8}),
    # Sierra Leone epidemic outbreak in november 1996 can't end in April 31.
    (
        {"country": "Sierra Leone", "start_year": 1996, "start_month": 11, "end_month": 4, "end_day": 31},
        {"end_day": 30},
    ),
    # Peru 1998 epidemic can't end in February 31.
    ({"country": "Peru", "start_year": 1998, "start_month": 1, "end_month": 2, "end_day": 31}, {"end_day": 28}),
    # India 2017 flood can't end in June 31.
    ({"country": "India", "start_year": 2017, "start_month": 6, "end_month": 6, "end_day": 31}, {"end_day": 30}),
    # US 2021 wildfires can't end in September 31.
    ({"country": "United States of America (the)", "start_year": 2021, "end_month": 9, "end_day": 31}, {"end_day": 30}),
    # Cameroon 2012 drought can't end before it started.
    # I will remove the month and day, since I can't pinpoint the exact dates.
    (
        {"country": "Cameroon", "start_year": 2012, "start_month": 6, "end_month": 1},
        {"start_month": np.nan, "start_day": np.nan, "end_month": np.nan, "end_day": np.nan},
    ),
]
# Other potential issues, where more people were affected than the entire population of the country:
# country             |   year | type    |   affected |   homeless |       population |
# --------------------|-------:|:--------|-----------:|-----------:|-----------------:|
# Antigua and Barbuda |   1983 | Drought |      75000 |          0 |  65426           |
# Botswana            |   1981 | Drought |    1037300 |          0 | 982753           |
# Dominica            |   2017 | Storm   |      71293 |          0 |  70422           |
# Ghana               |   1980 | Drought |   12500000 |          0 |      1.18653e+07 |
# Laos                |   1977 | Drought |    3500000 |          0 |      3.12575e+06 |
# Mauritania          |   1969 | Drought |    1300000 |          0 |      1.08884e+06 |
# Mauritania          |   1976 | Drought |    1420000 |          0 |      1.34161e+06 |
# Mauritania          |   1980 | Drought |    1600000 |          0 |      1.5067e+06  |
# Montserrat          |   1989 | Storm   |          0 |      12000 |  10918           |
# Saint Lucia         |   2010 | Storm   |     181000 |          0 | 170950           |
# Samoa               |   1990 | Storm   |     170000 |      25000 | 168202           |
# Tonga               |   1982 | Storm   |     100000 |      46500 |  96951           |
# Finally, there are events registered on the same year for both a historical region and one of its
# successor countries (we are ignoring this issue).
# 1902: {'Azerbaijan', 'USSR'},
# 1990: {'Tajikistan', 'USSR'},
# 1991: {'Georgia', 'USSR'},

# Get naming conventions.
N = PathFinder(str(CURRENT_DIR / "natural_disasters"))


def prepare_input_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare input data, and fix some known issues."""
    # Select and rename columns.
    df = df[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Correct wrong data points (defined above in DATA_CORRECTIONS).
    df = correct_data_points(df=df, corrections=DATA_CORRECTIONS)

    # Remove spurious spaces in entities.
    df["type"] = df["type"].str.strip()

    # Sanity check
    error = "List of expected disaster types has changed. Consider updating EXPECTED_DISASTER_TYPES."
    assert set(df["type"]) == set(EXPECTED_DISASTER_TYPES), error

    # Rename disaster types conveniently.
    df["type"] = df["type"].replace(EXPECTED_DISASTER_TYPES)

    # Drop rows for disaster types that are not relevant.
    df = df.dropna(subset="type").reset_index(drop=True)

    return df


def sanity_checks_on_inputs(df: pd.DataFrame) -> None:
    """Run sanity checks on input data."""
    error = "All values should be positive."
    assert (df.select_dtypes("number").fillna(0) >= 0).all().all(), error

    error = "Column 'total_affected' should be the sum of columns 'injured', 'affected', and 'homeless'."
    assert (
        df["total_affected"].fillna(0) >= df[["injured", "affected", "homeless"]].sum(axis=1).fillna(0)
    ).all(), error

    error = "Natural disasters are not expected to last more than 9 years."
    assert (df["end_year"] - df["start_year"]).max() < 10, error

    error = "Some of the columns that can't have nan do have one or more nans."
    assert df[["country", "year", "type", "start_year", "end_year"]].notnull().all().all(), error

    for column in ["year", "start_year", "end_year"]:
        error = f"Column '{column}' has a year prior to 1900 or posterior to current year."
        assert 1900 < df[column].max() <= datetime.datetime.now().year, error

    error = "Some rows have end_day specified, but not end_month."
    assert df[(df["end_month"].isnull()) & (df["end_day"].notnull())].empty, error


def fix_faulty_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Fix an issue related to column dtypes.

    Dividing a UInt32 by float64 results in a faulty Float64 that does not handle nans properly (which may be a bug:
    https://github.com/pandas-dev/pandas/issues/49818).
    To avoid this, there are various options:
    1. Convert all UInt32 into standard int before dividing by a float. But, if there are nans, int dtype is not valid.
    2. Convert all floats into Float64 before dividing.
    3. Convert all Float64 into float, after dividing.

    We adopt option 3.

    """
    df = df.astype({column: float for column in df[df.columns[df.dtypes == "Float64"]]})

    return df


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Harmonize country names."""
    df = df.copy()

    # Harmonize country names.
    df = geo.harmonize_countries(
        df=df, countries_file=N.country_mapping_path, warn_on_missing_countries=True, warn_on_unused_countries=True
    )

    # Add Azores Islands to Portugal (so that we can attach a population to it).
    df = df.replace({"Azores Islands": "Portugal"})
    # Add Canary Islands to Spain (so that we can attach a population to it).
    df = df.replace({"Canary Islands": "Spain"})

    return df


def calculate_start_and_end_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate start and end dates of disasters.

    The original data had year, month and day of start and end, and some of those fields were missing. This function
    deals with those missing fields and creates datetime columns for start and end of events.

    """
    df = df.copy()

    # When start month is not given, assume the beginning of the year.
    df["start_month"] = df["start_month"].fillna(1)
    # When start day is not given, assume the beginning of the month.
    df["start_day"] = df["start_day"].fillna(1)

    # When end month is not given, assume the end of the year.
    df["end_month"] = df["end_month"].fillna(12)

    # When end day is not given, assume the last day of the month.
    last_day_of_month = pd.Series(
        [get_last_day_of_month(year=row["end_year"], month=row["end_month"]) for i, row in df.iterrows()]
    )
    df["end_day"] = df["end_day"].fillna(last_day_of_month)

    # Create columns for start and end dates.
    df["start_date"] = (
        df["start_year"].astype(str)
        + "-"
        + df["start_month"].astype(str).str.zfill(2)
        + "-"
        + df["start_day"].astype(str).str.zfill(2)
    )
    df["end_date"] = (
        df["end_year"].astype(str)
        + "-"
        + df["end_month"].astype(str).str.zfill(2)
        + "-"
        + df["end_day"].astype(str).str.zfill(2)
    )

    # Convert dates into datetime objects.
    # Note: This may fail if one of the dates is wrong, e.g. September 31 (if so, check error message for row index).
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    error = "Events can't have an end_date prior to start_date."
    assert ((df["end_date"] - df["start_date"]).dt.days >= 0).all(), error

    # Drop unnecessary columns.
    df = df.drop(columns=["start_year", "start_month", "start_day", "end_year", "end_month", "end_day"])

    return df


def calculate_yearly_impacts(df: pd.DataFrame) -> pd.DataFrame:
    """Equally distribute the impact of disasters lasting longer than one year among the individual years, as separate
    events.

    Many disasters last more than one year. Therefore, we need to spread their impact among the different years.
    Otherwise, if we assign the impact of a disaster to, say, the first year, we may overestimate the impacts on a
    particular country-year.
    Hence, for events that started and ended in different years, we distribute their impact equally across the
    time spanned by the disaster.

    """
    df = df.copy()

    # There are many rows that have no data on impacts of disasters.
    # I suppose those are known disasters for which we don't know the impact.
    # Given that we want to count overall impact, fill them with zeros (to count them as disasters that had no victims).
    df[IMPACT_COLUMNS] = df[IMPACT_COLUMNS].fillna(0)

    # Select rows of disasters that last more than one year.
    multi_year_rows_mask = df["start_date"].dt.year != df["end_date"].dt.year
    multi_year_rows = df[multi_year_rows_mask].reset_index(drop=True)

    # Go row by row, and create a new disaster event with the impact normalized by the fraction of days it happened
    # in a specific year.
    added_events = pd.DataFrame()
    for i, row in multi_year_rows.iterrows():
        # Start dataframe for new event.
        new_event = pd.DataFrame(row).transpose()
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
            added_events = pd.concat([added_events, new_event], ignore_index=True)

    # Remove multi-year rows from main dataframe, and add those rows after separating events year by year.
    yearly_df = pd.concat([df[~(multi_year_rows_mask)], added_events], ignore_index=True)  # type: ignore

    # Sort conveniently.
    yearly_df = yearly_df.sort_values(["country", "year", "type"]).reset_index(drop=True)

    return yearly_df


def get_total_count_of_yearly_impacts(df: pd.DataFrame) -> pd.DataFrame:
    """Get the total count of impacts in the year, ignoring the individual events.

    We are not interested in each individual event, but the number of events of each kind and their impacts.
    This function will produce the total count of impacts per country, year and type of disaster.

    """
    counts = (
        df.reset_index()
        .groupby(["country", "year", "type"], observed=True)
        .agg({"index": "count"})
        .reset_index()
        .rename(columns={"index": "n_events"})
    )
    df = df.groupby(["country", "year", "type"], observed=True).sum(numeric_only=True, min_count=1).reset_index()
    df = pd.merge(df, counts, on=["country", "year", "type"], how="left")

    return df


def create_a_new_type_for_all_disasters_combined(df: pd.DataFrame) -> pd.DataFrame:
    """Add a new disaster type that has the impact of all other disasters combined."""
    all_disasters = (
        df.groupby(["country", "year"], observed=True)
        .sum(numeric_only=True, min_count=1)
        .assign(**{"type": ALL_DISASTERS_TYPE})
        .reset_index()
    )
    df = (
        pd.concat([df, all_disasters], ignore_index=True)
        .sort_values(["country", "year", "type"])
        .reset_index(drop=True)
    )

    return df


def add_population_including_historical_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Add population to the main dataframe, including the population of historical regions.

    For historical regions for which we do not have population data, we construct their population by adding the
    population of their successor countries. This is done for countries in BUILD_POPULATION_FOR_HISTORICAL_COUNTRIES,
    using the definition of regions in HISTORIC_TO_CURRENT_REGION.
    For certain countries we have population data only for certain years (e.g. 1900, 1910, but not the years in
    between). In those cases we interpolate population data.

    """
    df = df.copy()

    # Historical regions whose population we want to include.
    historical_regions = {
        region: HISTORIC_TO_CURRENT_REGION[region]
        for region in HISTORIC_TO_CURRENT_REGION
        if region in BUILD_POPULATION_FOR_HISTORICAL_COUNTRIES
    }
    # All regions whose population we want to include (i.e. continents and historical countries).
    regions = dict(**REGIONS, **historical_regions)

    # Add population to main dataframe.
    df = add_population(
        df=df,
        interpolate_missing_population=True,
        warn_on_missing_countries=False,
        regions=regions,
        expected_countries_without_population=EXPECTED_COUNTRIES_WITHOUT_POPULATION,
    )

    return df


def create_additional_variables(df: pd.DataFrame, df_gdp: pd.DataFrame) -> pd.DataFrame:
    """Create additional variables, namely damages per GDP, and impacts per 100,000 people."""
    # Combine natural disasters with GDP data.
    df = pd.merge(df, df_gdp.rename(columns={"ny_gdp_mktp_cd": "gdp"}), on=["country", "year"], how="left")
    # Prepare cost variables.
    for variable in COST_VARIABLES:
        # Convert costs (given in '000 US$, aka thousand current US$) into current US$.
        df[variable] *= 1000
        # Create variables of costs (in current US$) as a share of GDP (in current US$).
        df[f"{variable}_per_gdp"] = df[variable] / df["gdp"] * 100

    # Add rates per 100,000 people.
    for column in VARIABLES_PER_100K_PEOPLE:
        df[f"{column}_per_100k_people"] = df[column] * 1e5 / df["population"]

    # Fix issue with faulty dtypes (see more details in the function's documentation).
    df = fix_faulty_dtypes(df=df)

    return df


def create_decade_data(df: pd.DataFrame) -> pd.DataFrame:
    """Create data of average impacts over periods of 10 years.

    For example (as explained in the footer of the natural disasters explorer), the value for 1900 of any column should
    represent the average of that column between 1900 and 1909.

    """
    decade_df = df.copy()

    # Ensure each country has data for all years (and fill empty rows with zeros).
    # Otherwise, the average would only be performed only across years for which we have data.
    # For example, if we have data only for 1931 (and no other year in the 1930s) we want that data point to be averaged
    # over all years in the decade (assuming they are all zero).
    # Note that, for the current decade, since it's not complete, we want to average over the number of current years
    # (not the entire decade).

    # List all countries, years and types in the data.
    countries = sorted(set(decade_df["country"]))
    years = np.arange(decade_df["year"].min(), decade_df["year"].max() + 1).tolist()
    types = sorted(set(decade_df["type"]))

    # Create a new index covering all combinations of countries, years and types.
    new_indexes = pd.MultiIndex.from_product([countries, years, types], names=["country", "year", "type"])

    # Reindex data so that all countries and types have data for each year (filling with zeros when there's no data).
    decade_df = decade_df.set_index(["country", "year", "type"]).reindex(new_indexes, fill_value=0).reset_index()

    # For each year, calculate the corresponding decade (e.g. 1951 -> 1950, 1929 -> 1920).
    decade_df["decade"] = (decade_df["year"] // 10) * 10

    # Group by that country-decade-type and get the mean for each column.
    decade_df = (
        decade_df.drop(columns=["year"])
        .groupby(["country", "decade", "type"], observed=True)
        .mean(numeric_only=True)
        .reset_index()
        .rename(columns={"decade": "year"})
    )

    return decade_df


def sanity_checks_on_outputs(df: pd.DataFrame, is_decade: bool) -> None:
    """Run sanity checks on output (yearly or decadal) data.

    Parameters
    ----------
    df : pd.DataFrame
        Output (yearly or decadal) data.
    is_decade : bool
        True if df is decadal data; False if it is yearly data.

    """
    # Common sanity checks for yearly and decadal data.
    error = "All values should be positive."
    assert (df.select_dtypes("number").fillna(0) >= 0).all().all(), error

    error = (
        "List of expected disaster types has changed. "
        "Consider updating EXPECTED_DISASTER_TYPES (or renaming ALL_DISASTERS_TYPE)."
    )
    expected_disaster_types = [ALL_DISASTERS_TYPE] + [
        catalog.utils.underscore(EXPECTED_DISASTER_TYPES[disaster])
        for disaster in EXPECTED_DISASTER_TYPES
        if not pd.isna(EXPECTED_DISASTER_TYPES[disaster])
    ]
    assert set(df["type"]) == set(expected_disaster_types), error

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
    assert df[columns_that_should_not_have_nans].notnull().all(axis=1).all(), error

    # Sanity checks only for yearly data.
    if not is_decade:
        all_countries = sorted(set(df["country"]) - set(REGIONS) - set(HISTORIC_TO_CURRENT_REGION))

        # Check that the aggregate of all countries and disasters leads to the same numbers we have for the world.
        # This check would not pass when adding historical regions (since we know there are some overlaps between data
        # from historical and successor countries). So check for a specific year.
        year_to_check = 2022
        all_disasters_for_world = df[
            (df["country"] == "World") & (df["year"] == year_to_check) & (df["type"] == ALL_DISASTERS_TYPE)
        ].reset_index(drop=True)
        all_disasters_check = (
            df[(df["country"].isin(all_countries)) & (df["year"] == year_to_check) & (df["type"] != ALL_DISASTERS_TYPE)]
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
            df["total_affected"].fillna(0) >= df[["injured", "affected", "homeless"]].sum(axis=1).fillna(0)
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
            informed_rows = df[column].notnull() & df["population"].notnull()
            assert (df[informed_rows][column] <= df[informed_rows]["population"]).all(), error


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load natural disasters dataset from meadow.
    ds_meadow = catalog.Dataset(DATA_DIR / f"meadow/emdat/{MEADOW_VERSION}/natural_disasters")
    # Get table from dataset.
    tb_meadow = ds_meadow["natural_disasters"]
    # Create a dataframe from the table.
    df = pd.DataFrame(tb_meadow)

    # Load GDP from WorldBank WDI dataset.
    ds_gdp = catalog.Dataset(WDI_DATASET_PATH)
    # Load main table from WDI dataset, and select variable corresponding to GDP (in current US$).
    tb_gdp = ds_gdp["wdi"][["ny_gdp_mktp_cd"]]
    # Create a dataframe with GDP.
    df_gdp = pd.DataFrame(tb_gdp).reset_index()

    #
    # Process data.
    #
    # Prepare input data (and fix some known issues).
    df = prepare_input_data(df=df)

    # Sanity checks.
    sanity_checks_on_inputs(df=df)

    # Harmonize country names.
    df = harmonize_countries(df=df)

    # Calculate start and end dates of disasters.
    df = calculate_start_and_end_dates(df=df)

    # Distribute the impacts of disasters lasting longer than a year among separate yearly events.
    df = calculate_yearly_impacts(df=df)

    # Get total count of impacts per year (regardless of the specific individual events during the year).
    df = get_total_count_of_yearly_impacts(df=df)

    # Add a new category (or "type") corresponding to the total of all natural disasters.
    df = create_a_new_type_for_all_disasters_combined(df=df)

    # Add region aggregates.
    df = add_region_aggregates(data=df, index_columns=["country", "year", "type"])

    # Add population including historical regions.
    df = add_population_including_historical_regions(df=df)

    # Add damages per GDP, and rates per 100,000 people.
    df = create_additional_variables(df=df, df_gdp=df_gdp)

    # Change disaster types to snake, lower case.
    df["type"] = [catalog.utils.underscore(value) for value in df["type"]]

    # Create data aggregated (using a simple mean) in intervals of 10 years.
    decade_df = create_decade_data(df=df)

    # Run sanity checks on output yearly data.
    sanity_checks_on_outputs(df=df, is_decade=False)

    # Run sanity checks on output decadal data.
    sanity_checks_on_outputs(df=decade_df, is_decade=True)

    # Set an appropriate index to yearly data and sort conveniently.
    df = df.set_index(["country", "year", "type"], verify_integrity=True).sort_index().sort_index()

    # Set an appropriate index to decadal data and sort conveniently.
    decade_df = decade_df.set_index(["country", "year", "type"], verify_integrity=True).sort_index().sort_index()

    #
    # Save outputs.
    #
    # Create new Garden dataset.
    ds_garden = catalog.Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Ensure all column names are snake, lower case.
    tb_garden = catalog.Table(df, short_name="natural_disasters_yearly", underscore=True)
    decade_tb_garden = catalog.Table(decade_df, short_name="natural_disasters_decadal", underscore=True)

    # Add tables to dataset
    ds_garden.add(tb_garden)
    ds_garden.add(decade_tb_garden)

    # Add metadata from yaml file.
    ds_garden.update_metadata(N.metadata_path)

    # Save dataset
    ds_garden.save()
