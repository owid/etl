"""Process and harmonize EM-DAT natural disasters dataset.

"""

import datetime
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, cast

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table, Variable, utils
from owid.datautils.dataframes import groupby_agg, map_series

from etl.data_helpers import geo

# Temporary imports (while add_regions_to_table is not directly imported from geo).
from etl.data_helpers.geo import (
    TableOrDataFrame,
    Union,
    create_table_of_regions_and_subregions,
    detect_overlapping_regions,
    list_countries_in_region,
    list_countries_in_region_that_must_have_data,
    list_members_of_region,
    log,
)
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

# Aggregate regions to add, following OWID definitions.
REGIONS = {
    # Default continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "European Union (27)": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    "World": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
}

# Overlaps found between historical regions and successor countries, that we accept in the data.
# We accept them either because they happened close to the transition, or to avoid needing to introduce new
# countries for which we do not have data (like the Russian Empire).
ACCEPTED_OVERLAPS = [{1911: {"USSR", "Kazakhstan"}}, {1991: {"Georgia", "USSR"}}, {1991: {"West Germany", "Germany"}}]

# List issues found in the data:
# Each element is a tuple with a dictionary that fully identifies the wrong row,
# and another dictionary that specifies the changes.
# Note: Countries here should appear as in the raw data (i.e. not harmonized).
DATA_CORRECTIONS = []


def correct_data_points(tb: Table, corrections: List[Tuple[Dict[Any, Any], Dict[Any, Any]]]) -> Table:
    """Make individual corrections to data points in a table.

    Parameters
    ----------
    tb : Table
        Data to be corrected.
    corrections : List[Tuple[Dict[Any, Any], Dict[Any, Any]]]
        Corrections.

    Returns
    -------
    tb_corrected : Table
        Corrected data.

    """
    tb_corrected = tb.copy()

    for correction in corrections:
        wrong_row, corrected_row = correction

        # Select the row in the table where the wrong data point is.
        # The 'fillna(False)' is added because otherwise rows that do not fulfil the selection will create ambiguity.
        selection = tb_corrected.loc[(tb_corrected[list(wrong_row)] == Variable(wrong_row)).fillna(False).all(axis=1)]
        # Sanity check.
        error = "Either raw data has been corrected, or dictionary selecting wrong row is ambiguous."
        assert len(selection) == 1, error

        # Replace wrong fields by the corrected ones.
        # Note: Changes to categorical fields will not work.
        tb_corrected.loc[selection.index, list(corrected_row)] = list(corrected_row.values())

    return tb_corrected


def get_last_day_of_month(year: int, month: int):
    """Get the number of days in a specific month of a specific year.

    Parameters
    ----------
    year : int
        Year.
    month : int
        Month.

    Returns
    -------
    last_day
        Number of days in month.

    """
    if month == 12:
        last_day = 31
    else:
        last_day = (datetime.datetime.strptime(f"{year:04}-{month + 1:02}", "%Y-%m") + datetime.timedelta(days=-1)).day

    return last_day


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
    tb["type"] = map_series(
        series=tb["type"], mapping=EXPECTED_DISASTER_TYPES, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )

    # Drop rows for disaster types that are not relevant.
    tb = tb.dropna(subset="type").reset_index(drop=True)

    return tb


def sanity_checks_on_inputs(tb: Table) -> None:
    """Run sanity checks on input data."""
    error = "All values should be positive."
    assert (tb.select_dtypes("number").fillna(0) >= 0).all().all(), error

    error = "Column 'total_affected' should be the sum of columns 'injured', 'affected', and 'homeless'."
    assert (
        tb["total_affected"].fillna(0) == tb[["injured", "affected", "homeless"]].sum(axis=1).fillna(0)
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
    assert (tb["end_date"] >= tb["start_date"]).all(), error

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
    for _, row in multi_year_rows.iterrows():
        # Start table for new event.
        new_event = Table(row).transpose().reset_index(drop=True).copy_metadata(tb)
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
                impacts = pd.DataFrame(row[IMPACT_COLUMNS] * days_fraction).transpose().astype(int)
                # Ensure "total_affected" is the sum of "injured", "affected" and "homeless".
                # Note that the previous line may have introduced rounding errors.
                impacts["total_affected"] = impacts["injured"] + impacts["affected"] + impacts["homeless"]
                # Start a series that counts the impacts accumulated over the years.
                cumulative_impacts = impacts
                # Normalize data by the number of days affected in this year.
                new_event.loc[:, IMPACT_COLUMNS] = impacts.values
                # Correct year and dates.
                new_event["year"] = year
                new_event["end_date"] = pd.Timestamp(year=year, month=12, day=31)
            elif years[0] < year < years[-1]:
                # The entire year was affected by the disaster.
                # Note: Ignore leap years.
                days_fraction = 365 / days_total
                # Impacts this year.
                impacts = pd.DataFrame(row[IMPACT_COLUMNS] * days_fraction).transpose().astype(int)
                # Ensure "total_affected" is the sum of "injured", "affected" and "homeless".
                # Note that the previous line may have introduced rounding errors.
                impacts["total_affected"] = impacts["injured"] + impacts["affected"] + impacts["homeless"]
                # Add impacts to the cumulative impacts series.
                cumulative_impacts += impacts  # type: ignore
                # Normalize data by the number of days affected in this year.
                new_event.loc[:, IMPACT_COLUMNS] = impacts.values
                # Correct year and dates.
                new_event["year"] = year
                new_event["start_date"] = pd.Timestamp(year=year, month=1, day=1)
                new_event["end_date"] = pd.Timestamp(year=year, month=12, day=31)
            else:
                # Assign all remaining impacts to the last year.
                impacts = (pd.Series(row[IMPACT_COLUMNS]) - cumulative_impacts).astype(int)  # type: ignore
                new_event.loc[:, IMPACT_COLUMNS] = impacts.values
                # Correct year and dates.
                new_event["year"] = year
                new_event["start_date"] = pd.Timestamp(year=year, month=1, day=1)
                new_event["end_date"] = row["end_date"]
            added_events = pr.concat([added_events, new_event], ignore_index=True).copy()

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
    # Ensure columns have the right type.
    tb = tb.astype(
        {column: int for column in tb.columns if column not in ["country", "year", "type", "start_date", "end_date"]}
    )
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
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population)

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
    # Otherwise, the average would be performed only across years for which we have data.
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


def sanity_checks_on_outputs(tb: Table, is_decade: bool, ds_regions: Dataset) -> None:
    """Run sanity checks on output (yearly or decadal) data.

    Parameters
    ----------
    tb : Table
        Output (yearly or decadal) data.
    is_decade : bool
        True if tb is decadal data; False if it is yearly data.
    ds_regions : Dataset
        Regions dataset.

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

    # Get names of historical regions in the data.
    regions = ds_regions["regions"].reset_index()
    historical_regions_in_data = set(regions[regions["is_historical"]]["name"]) & set(tb["country"])

    # Sanity checks only for yearly data.
    if not is_decade:
        all_countries = sorted(set(tb["country"]) - set(REGIONS) - historical_regions_in_data)

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


########################################################################################################################
# TODO: Remove this code and import it from data_helpers.geo once the code is properly tested.
def add_region_aggregates(
    df: TableOrDataFrame,
    region: str,
    countries_in_region: Optional[List[str]] = None,
    index_columns: Optional[List[str]] = None,
    countries_that_must_have_data: Optional[Union[List[str], Literal["auto"]]] = None,
    num_allowed_nans_per_year: Optional[int] = None,
    frac_allowed_nans_per_year: Optional[float] = None,
    min_num_values_per_year: Optional[int] = None,
    country_col: str = "country",
    year_col: str = "year",
    aggregations: Optional[Dict[str, Any]] = None,
    keep_original_region_with_suffix: Optional[str] = None,
    population: Optional[pd.DataFrame] = None,
) -> TableOrDataFrame:
    """Add aggregate data for a specific region (e.g. a continent or an income group) to a table.

    ####################################################################################################################
    WARNING: Consider using add_regions_to_table instead.
    This function is not deprecated, as it is used by add_regions_to_table, but it should not be used directly.
    ####################################################################################################################

    If data for a region already exists:
    * If keep_original_region_with_suffix is None, the original data for the region will be replaced by a new aggregate.
    * If keep_original_region_with_suffix is not None, the original data for the region will be kept, and the value of
      keep_original_region_with_suffix will be appended to the name of the region.

    When adding up the contribution from different countries (e.g. Spain, France, etc.) of a region (e.g. Europe), we
    want to avoid two problems:
    * Generating a series of nan, because one small country (with a negligible contribution) has nans.
    * Generating a series that underestimates the real one, because of treating missing values as zeros.

    To avoid these problems, we first define a list of "big countries" that must be present in the data, in order to
    safely do the aggregation. If any of these countries is not present for a particular variable and year, the
    aggregation will be nan for that variable and year. Otherwise, if all big countries are present, any other missing
    country will be assumed to have zero contribution to the variable.
    For example, when aggregating the electricity demand of North America, United States and Mexico cannot be missing,
    because otherwise the aggregation would significantly underestimate the true electricity demand of North America.

    Additionally, the aggregation of a particular variable for a particular year cannot have too many nans. If the
    number of nans exceeds num_allowed_nans_per_year, or if the fraction of nans exceeds frac_allowed_nans_per_year, the
    aggregation for that variable and year will be nan.

    Parameters
    ----------
    df : TableOrDataFrame
        Original table, which may already contain data for the region.
    region : str
        Region to add.
    countries_in_region : list or None
        List of countries that are members of this region. None to load them from countries-regions dataset.
    index_columns : Optional[List[str]], default: None
        Names of index columns (usually ["country", "year"]) to group by.
    countries_that_must_have_data : list or None or str
        * If a list of countries is passed, those countries must have data for a particular variable and year. If any of
          those countries is not informed on a particular variable and year, the region will have nan for that particular
          variable and year.
        * If "auto", a list of countries that must have data is automatically generated, based on population. When
          choosing this option, explicitly pass population as an argument (otherwise it will be silently loaded).
          See function list_countries_in_region_that_must_have_data for more details.
        * If None, nothing happens: An aggregate is constructed even if important countries are missing.
    num_allowed_nans_per_year : int or None
        * If a number is passed, this is the maximum number of nans that can be present in a particular variable and
          year. If that number of nans is exceeded, the aggregate will be nan.
        * If None, nothing happens: An aggregate is constructed regardless of the number of nans.
    frac_allowed_nans_per_year : float or None
        * If a number is passed, this is the maximum fraction of nans that can be present in a particular variable and
          year. If that fraction of nans is exceeded, the aggregate will be nan.
        * If None, nothing happens: An aggregate is constructed regardless of the fraction of nans.
    min_num_values_per_year : int or None
        * If a number is passed, this is the minimum number of non-nan values that must be present in a particular
          variable and year. If that number of values is not reached, the aggregate will be nan.
        * If None, nothing happens: An aggregate is constructed regardless of the number of non-nan values.
    country_col : str
        Name of country column.
    year_col : str
        Name of year column.
    aggregations : dict or None
        Aggregations to execute for each variable. If None, the contribution to each variable from each country in the
        region will be summed. Otherwise, only the variables indicated in the dictionary will be affected. All remaining
        variables will be nan.
    keep_original_region_with_suffix : str or None
        * If not None, the original data for a region will be kept, with the same name, but having suffix
          keep_original_region_with_suffix appended to its name.
        * If None, the original data for a region will be replaced by aggregate data constructed by this function.
    population : pd.DataFrame or None
        Only relevant if countries_that_must_have_data is "auto", otherwise ignored.
        * If not None, it should be the main population table from the population dataset.
        * If None, the population table will be silently loaded.

    Returns
    -------
    df_updated : pd.DataFrame
        Original dataset after adding (or replacing) data for selected region.

    """
    if countries_in_region is None:
        # List countries in the region.
        countries_in_region = list_countries_in_region(
            region=region,
        )

    if countries_that_must_have_data is None:
        countries_that_must_have_data = []
    elif countries_that_must_have_data == "auto":
        # List countries that should present in the data (since they are expected to contribute the most).
        countries_that_must_have_data = list_countries_in_region_that_must_have_data(
            region=region,
            population=population,
        )

    if index_columns is None:
        index_columns = [country_col, year_col]

    # If aggregations are not defined for each variable, assume 'sum'.
    if aggregations is None:
        aggregations = {variable: "sum" for variable in df.columns if variable not in index_columns}
    variables = list(aggregations)

    # Initialise dataframe of added regions, and add variables one by one to it.
    # df_region = Table({country_col: [], year_col: []}).astype(dtype={country_col: "object", year_col: "int"})
    # Select data for countries in the region.
    df_countries = df[df[country_col].isin(countries_in_region)]

    df_region = groupby_agg(
        df=df_countries,
        groupby_columns=[column for column in index_columns if column != country_col],
        aggregations=dict(
            **aggregations,
            **{country_col: lambda x: set(countries_that_must_have_data).issubset(set(list(x)))},  # type: ignore
        ),
        num_allowed_nans=num_allowed_nans_per_year,
        frac_allowed_nans=frac_allowed_nans_per_year,
        min_num_values=min_num_values_per_year,
    ).reset_index()

    # Create filter that detects rows where the most contributing countries are not present.
    if df_region[country_col].dtypes == "category":
        # Doing df_region[country_col].any() fails if the country column is categorical.
        mask_countries_present = ~(df_region[country_col].astype(str))
    else:
        mask_countries_present = ~df_region[country_col]
    if mask_countries_present.any():
        # Make nan all aggregates if the most contributing countries were not present.
        df_region.loc[mask_countries_present, variables] = np.nan
    # Replace the column that was used to check if most contributing countries were present by the region's name.
    df_region[country_col] = region

    if isinstance(keep_original_region_with_suffix, str):
        # Keep rows in the original dataframe containing rows for region (adding a suffix to the region name), and then
        # append new rows for region.
        rows_original_region = df[country_col] == region
        df_original_region = df[rows_original_region].reset_index(drop=True)
        # Append suffix at the end of the name of the original region.
        df_original_region[country_col] = region + cast(str, keep_original_region_with_suffix)
        df_updated = pd.concat(
            [df[~rows_original_region], df_original_region, df_region],
            ignore_index=True,
        )
    else:
        # Remove rows in the original table containing rows for region, and append new rows for region.
        df_updated = pd.concat([df[~(df[country_col] == region)], df_region], ignore_index=True)
        # WARNING: When an aggregate is added (e.g. "Europe") just for one of the columns (and no aggregation is
        # specified for the rest of columns) and there was already data for that region, the data for the rest of
        # columns is deleted for that particular region (in the following line).
        # This is an unusual scenario, because you would normally want to replace all data for a certain region, not
        # just certain columns. However, the expected behavior would be to just replace the region data for the
        # specified column.
        # For now, simply warn that the original data for the region for those columns was deleted.
        columns_without_aggregate = set(df.drop(columns=index_columns).columns) - set(aggregations)
        if (len(columns_without_aggregate) > 0) and (len(df[df[country_col] == region]) > 0):
            log.warning(
                f"Region {region} already has data for columns that do not have a defined aggregation method: "
                f"({columns_without_aggregate}). That data will become nan."
            )

    # Sort conveniently.
    df_updated = df_updated.sort_values([country_col, year_col]).reset_index(drop=True)

    # If the original was Table, copy metadata
    if isinstance(df, Table):
        return Table(df_updated).copy_metadata(df)
    else:
        return df_updated  # type: ignore


def add_regions_to_table(
    tb: TableOrDataFrame,
    ds_regions: Dataset,
    ds_income_groups: Optional[Dataset] = None,
    regions: Optional[Union[List[str], Dict[str, Any]]] = None,
    aggregations: Optional[Dict[str, str]] = None,
    index_columns: Optional[List[str]] = None,
    num_allowed_nans_per_year: Optional[int] = None,
    frac_allowed_nans_per_year: Optional[float] = None,
    min_num_values_per_year: Optional[int] = None,
    country_col: str = "country",
    year_col: str = "year",
    keep_original_region_with_suffix: Optional[str] = None,
    check_for_region_overlaps: bool = True,
    accepted_overlaps: Optional[List[Dict[int, Set[str]]]] = None,
    ignore_overlaps_of_zeros: bool = False,
    subregion_type: str = "successors",
    countries_that_must_have_data: Optional[Dict[str, List[str]]] = None,
) -> Table:
    """Add one or more region aggregates to a table (or dataframe).

    This should be the default function to use when adding data for regions to a table (or dataframe).
    This function respects the metadata of the incoming data.

    If the original data for a region already exists:
    * If keep_original_region_with_suffix is None, the original data for the region will be replaced by a new aggregate.
    * If keep_original_region_with_suffix is not None, the original data for the region will be kept, and the value of
      keep_original_region_with_suffix will be appended to the name of the region.

    Parameters
    ----------
    tb : TableOrDataFrame
        Original data, which may or may not contain data for regions.
    ds_regions : Dataset
        Regions dataset.
    ds_income_groups : Optional[Dataset], default: None
        World Bank income groups dataset.
        * If given, aggregates for income groups may be added to the data.
        * If None, no aggregates for income groups will be added.
    regions : Optional[Union[List[str], Dict[str, Any]]], default: None
        Regions to be added.
        * If it is a list, it must contain region names of default regions or income groups.
          Example: ["Africa", "Europe", "High-income countries"]
        * If it is a dictionary, each key must be the name of a default, or custom region, and the value is another
          dictionary, that can contain any of the following keys:
          * "additional_regions": Additional regions whose members should be included in the region.
          * "excluded_regions": Regions whose members should be excluded from the region.
          * "additional_members": Additional individual members (countries) to include in the region.
          * "excluded_members": Individual members to exclude from the region.
          Example: {
            "Asia": {},  # No need to define anything, since it is a default region.
            "Asia excluding China": {  # Custom region that must be defined based on other known regions and countries.
                "additional_regions": ["Asia"],
                "excluded_members": ["China"],
                },
            }
        * If None, the default regions will be added (defined as REGIONS in etl.data_helpers.geo).
    aggregations : Optional[Dict[str, str]], default: None
        Aggregation to implement for each variable.
        * If a dictionary is given, the keys must be columns of the input data, and the values must be valid operations.
          Only the variables indicated in the dictionary will be affected. All remaining variables will have an
          aggregate value for the new regions of nan.
          Example: {"column_1": "sum", "column_2": "mean", "column_3": lambda x: some_function(x)}
          If there is a "column_4" in the data, for which no aggregation is defined, then the e.g. "Europe" will have
          only nans for "column_4".
        * If None, "sum" will be assumed to all variables.
    index_columns : Optional[List[str]], default: None
        Names of index columns (usually ["country", "year"]) to group by.
    num_allowed_nans_per_year : Optional[int], default: None
        * If a number is passed, this is the maximum number of nans that can be present in a particular variable and
          year. If that number of nans is exceeded, the aggregate will be nan.
        * If None, an aggregate is constructed regardless of the number of nans.
    frac_allowed_nans_per_year : Optional[float], default: None
        * If a number is passed, this is the maximum fraction of nans that can be present in a particular variable and
          year. If that fraction of nans is exceeded, the aggregate will be nan.
        * If None, an aggregate is constructed regardless of the fraction of nans.
    min_num_values_per_year : Optional[int], default: None
        * If a number is passed, this is the minimum number of valid (not-nan) values that must be present in a
          particular variable and year grouped. If that number of values is not reached, the aggregate will be nan.
          However, if all values in the group are valid, the aggregate will also be valid, even if the number of values
          in the group is smaller than min_num_values_per_year.
        * If None, an aggregate is constructed regardless of the number of non-nan values.
    country_col : Optional[str], default: "country"
        Name of country column.
    year_col : Optional[str], default: "year"
        Name of year column.
    keep_original_region_with_suffix : Optional[str], default: None
        * If not None, the original data for a region will be kept, with the same name, but having suffix
          keep_original_region_with_suffix appended to its name.
          Example: If keep_original_region_with_suffix is " (WB)", then there will be rows for, e.g. "Europe (WB)", with
          the original data, and rows for "Europe", with the new aggregate data.
        * If None, the original data for a region will be replaced by new aggregate data constructed by this function.
    check_for_region_overlaps : bool, default: True
        * If True, a warning is raised if a historical region has data on the same year as any of its successors.
          TODO: For now, this function simply warns about overlaps, but does nothing else about them.
            Consider adding the option to remove the data for the historical region, or the data for the successor, at
            the moment the aggregate is created.
        * If False, any possible overlap is ignored.
    accepted_overlaps : Optional[List[Dict[int, Set[str]]]], default: None
        Only relevant if check_for_region_overlaps is True.
        * If a dictionary is passed, it must contain years as keys, and sets of overlapping countries as values.
          This is used to avoid warnings when there are known overlaps in the data that are accepted.
          Note that, if the overlaps passed here are not present in the data, a warning is also raised.
          Example: [{1991: {"Georgia", "USSR"}}, {2000: {"Some region", "Some overlapping region"}}]
        * If None, any possible overlap in the data will raise a warning.
    ignore_overlaps_of_zeros : bool, default: False
        Only relevant if check_for_region_overlaps is True.
        * If True, overlaps of values of zero are ignored. In other words, if a region and one of its successors have
          both data on the same year, and that data is zero for both, no warning is raised.
        * If False, overlaps of values of zero are not ignored.
    subregion_type : str, default: "successors"
        Only relevant if check_for_region_overlaps is True.
        * If "successors", the function will look for overlaps between historical regions and their successors.
        * If "related", the function will look for overlaps between regions and their possibly related members (e.g.
          overseas territories).
    countries_that_must_have_data : Optional[Dict[str, List[str]]], default: None
        * If a dictionary is passed, each key must be a valid region, and the value should be a list of countries that
          must have data for that region. If any of those countries is not informed on a particular variable and year,
          that region will have nan for that particular variable and year.
        * If None, an aggregate is constructed regardless of the countries missing.

    Returns
    -------
    TableOrDataFrame
        Original table (or dataframe) after adding (or replacing) aggregate data for regions.

    """
    df_with_regions = pd.DataFrame(tb).copy()

    if index_columns is None:
        index_columns = [country_col, year_col]

    if check_for_region_overlaps:
        # Find overlaps between regions and its members.

        if accepted_overlaps is None:
            accepted_overlaps = []

        # Create a dictionary of regions and its members.
        df_regions_and_members = create_table_of_regions_and_subregions(
            ds_regions=ds_regions, subregion_type=subregion_type
        )
        regions_and_members = df_regions_and_members[subregion_type].to_dict()

        # Assume incoming table has a dummy index (the whole function may not work otherwise).
        # Example of region_and_members:
        # {"Czechoslovakia": ["Czechia", "Slovakia"]}
        all_overlaps = detect_overlapping_regions(
            df=df_with_regions,
            regions_and_members=regions_and_members,
            country_col=country_col,
            year_col=year_col,
            index_columns=index_columns,
            ignore_overlaps_of_zeros=ignore_overlaps_of_zeros,
        )
        # Example of accepted_overlaps:
        # [{1991: {"Georgia", "USSR"}}, {2000: {"Some region", "Some overlapping region"}}]
        # Check whether all accepted overlaps are found in the data, and that there are no new unknown overlaps.
        all_overlaps_sorted = sorted(all_overlaps, key=lambda d: str(d))
        accepted_overlaps_sorted = sorted(accepted_overlaps, key=lambda d: str(d))
        if all_overlaps_sorted != accepted_overlaps_sorted:
            log.warning(
                "Either the list of accepted overlaps is not found in the data or there are unknown overlaps. "
                f"Accepted overlaps: {accepted_overlaps_sorted}.\nFound overlaps: {all_overlaps_sorted}."
            )

    if aggregations is None:
        # Create region aggregates for all columns (with a simple sum) except for index columns.
        aggregations = {column: "sum" for column in df_with_regions.columns if column not in index_columns}

    if regions is None:
        regions = REGIONS
    elif isinstance(regions, list):
        # Assume they are known regions and they have no modifications.
        regions = {region: {} for region in regions}

    if countries_that_must_have_data:
        # If countries_that_must_have_data is neither None or [], it must be a dictionary with regions as keys.
        # Check that the dictionary has the right format.
        error = "Argument countries_that_must_have_data must be a dictionary with regions as keys."
        assert set(countries_that_must_have_data) <= set(regions), error
        # Fill missing regions with an empty list.
        countries_that_must_have_data = {
            region: countries_that_must_have_data.get(region, []) for region in list(regions)
        }
    else:
        countries_that_must_have_data = {region: [] for region in list(regions)}

    # Add region aggregates.
    for region in regions:
        # Check that the content of the region dictionary is as expected.
        expected_items = {"additional_regions", "excluded_regions", "additional_members", "excluded_members"}
        unknown_items = set(regions[region]) - expected_items
        if len(unknown_items) > 0:
            log.warning(
                f"Unknown items in dictionary of regions {region}: {unknown_items}. Expected: {expected_items}."
            )

        # List members of the region.
        members = list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            additional_regions=regions[region].get("additional_regions"),
            excluded_regions=regions[region].get("excluded_regions"),
            additional_members=regions[region].get("additional_members"),
            excluded_members=regions[region].get("excluded_members"),
            # By default, include historical regions in income groups.
            include_historical_regions_in_income_groups=True,
        )
        # TODO: Here we could optionally define _df_with_regions, which is passed to add_region_aggregates, and is
        #   identical to df_with_regions, but overlaps in accepted_overlaps are solved (e.g. the data for the historical
        #   or parent region is made nan).

        # Add aggregate data for current region.
        df_with_regions = add_region_aggregates(
            df=df_with_regions,
            region=region,
            aggregations=aggregations,
            index_columns=index_columns,
            countries_in_region=members,
            countries_that_must_have_data=countries_that_must_have_data[region],
            num_allowed_nans_per_year=num_allowed_nans_per_year,
            frac_allowed_nans_per_year=frac_allowed_nans_per_year,
            min_num_values_per_year=min_num_values_per_year,
            country_col=country_col,
            year_col=year_col,
            keep_original_region_with_suffix=keep_original_region_with_suffix,
        )

    # If the original object was a Table, copy metadata
    if isinstance(tb, Table):
        # TODO: Add entry to processing log.
        return Table(df_with_regions).copy_metadata(tb)
    else:
        return df_with_regions  # type: ignore


########################################################################################################################


def run(dest_dir: str) -> None:
    #
    # Load inputs.
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
    ####################################################################################################################
    # TODO: Replace by geo.add_regions_to_table once merged.
    tb = add_regions_to_table(
        tb=tb,
        regions=REGIONS,
        index_columns=["country", "year", "type"],
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        accepted_overlaps=ACCEPTED_OVERLAPS,
    )
    ####################################################################################################################

    # Add damages per GDP, and rates per 100,000 people.
    tb = create_additional_variables(tb=tb, ds_population=ds_population, tb_gdp=tb_gdp)

    # Change disaster types to snake, lower case.
    tb["type"] = tb["type"].replace({value: utils.underscore(value) for value in tb["type"].unique()})

    # Create data aggregated (using a simple mean) in intervals of 10 years.
    tb_decadal = create_decade_data(tb=tb)

    # Run sanity checks on output yearly data.
    sanity_checks_on_outputs(tb=tb, is_decade=False, ds_regions=ds_regions)

    # Run sanity checks on output decadal data.
    sanity_checks_on_outputs(tb=tb_decadal, is_decade=True, ds_regions=ds_regions)

    # Set an appropriate index to yearly data and sort conveniently.
    tb = tb.format(keys=["country", "year", "type"], sort_columns=True, short_name="natural_disasters_yearly")

    # Set an appropriate index to decadal data and sort conveniently.
    tb_decadal = tb_decadal.format(
        keys=["country", "year", "type"], sort_columns=True, short_name="natural_disasters_decadal"
    )

    #
    # Save outputs.
    #
    # Create new garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_decadal], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()
