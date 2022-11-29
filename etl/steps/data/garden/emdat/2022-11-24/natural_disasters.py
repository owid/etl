"""Process and harmonize EM-DAT natural disasters dataset.

"""

from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from shared import CURRENT_DIR, HISTORIC_TO_CURRENT_REGION, REGIONS, add_region_aggregates, add_population

from etl.helpers import Names
from etl.paths import DATA_DIR

# Define inputs.
MEADOW_VERSION = "2022-11-24"
# Define outputs.
VERSION = MEADOW_VERSION

# List of expected disaster types in the raw data.
# If new ones are included, simply add them here.
EXPECTED_DISASTER_TYPES = [
    'Animal accident',
    'Drought',
    'Earthquake',
    'Epidemic',
    'Extreme temperature',
    'Flood',
    'Fog',
    'Glacial lake outburst',
    'Impact',
    'Insect infestation',
    'Landslide',
    'Mass movement (dry)',
    'Storm',
    'Volcanic activity',
    'Wildfire',
]

# List of columns to select from raw data, and how to rename them.
COLUMNS = {
    'country': "country",
    'year': "year",
    # Column "group" is used only for sanity checks.
    'group': "group",
    'type': "type",
    'total_dead': "total_dead",
    'injured': "injured",
    'affected': "affected",
    'homeless': "homeless",
    'total_affected': "total_affected",
    'reconstructed_costs_adjusted': 'reconstructed_costs_adjusted',
    'insured_damages_adjusted': 'insured_damages_adjusted',
}

# Get naming conventions.
N = Names(str(CURRENT_DIR / "natural_disasters"))


def sanity_checks_on_inputs(df: pd.DataFrame) -> None:
    error = "Expected only 'Natural' in 'group' column."
    assert df["group"].unique().tolist() == ["Natural"], error

    error = "All values should be positive."
    assert (df.select_dtypes('number').fillna(0) >= 0).all().all(), error

    error = "List of expected disaster types has changed. Consider updating EXPECTED_DISASTER_TYPES."
    assert set(df["type"]) == set(EXPECTED_DISASTER_TYPES), error

    error = "Column 'total_affected' should be the sum of columns 'injured', 'affected', and 'homeless'."
    assert (df["total_affected"].fillna(0) >= df[["injured", "affected", "homeless"]].sum(axis=1).fillna(0)).all(), error


def fix_faulty_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    # Dividing a UInt32 by float64 results in a faulty Float64 that does not handle nans properly
    # (which may be a bug: https://github.com/pandas-dev/pandas/issues/49818)
    # To avoid this, convert all UInt32 into standard int.
    int_columns = ['total_dead', 'injured', 'affected', 'homeless', 'total_affected', 'reconstructed_costs_adjusted',
                   'insured_damages_adjusted']
    df = df.astype({column: int for column in int_columns})

    return df


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=N.country_mapping_path, warn_on_missing_countries=True,
                            warn_on_unused_countries=True)

    # Add Azores Islands to Portugal (so that we can attach a population to it).
    new_portugal_df = pd.concat([df[df["country"] == "Portugal"], df[df["country"] == "Azores Islands"]]).\
        groupby(["year", "type"]).\
        sum(numeric_only=True).reset_index().assign(**{"country": "Portugal"})
    df = pd.concat([df[~df["country"].isin(["Azores Islands", "Portugal"])], new_portugal_df]).reset_index(drop=True)

    # Idem for Canary Islands.
    new_spain_df = pd.concat([df[df["country"] == "Spain"], df[df["country"] == "Canary Islands"]]).\
        groupby(["year", "type"]).\
        sum(numeric_only=True).reset_index().assign(**{"country": "Spain"})
    df = pd.concat([df[~df["country"].isin(["Canary Islands", "Spain"])], new_spain_df]).reset_index(drop=True)

    return df


def create_decade_data(df: pd.DataFrame) -> pd.DataFrame:
    decade_df = df.copy()
    # Convert "year" column into a datetime.
    decade_df["year"] = pd.to_datetime(decade_df["year"], format="%Y")
    # Group tens of years and sum.
    decade_df = decade_df.set_index("year").groupby(['country','type']).resample('10AS').mean(numeric_only=True).\
        reset_index()
    # Make "year" column years instead of dates.
    decade_df["year"] = decade_df["year"].dt.year

    return decade_df


def sanity_checks_on_outputs(df: pd.DataFrame, decade_df: pd.DataFrame) -> None:
    all_countries = sorted(set(df["country"]) - set(REGIONS) - set(HISTORIC_TO_CURRENT_REGION))

    # Check that the aggregate of all countries and disasters leads to the same numbers we have for the world.
    # This check would not pass when adding historical regions (since we know there are some overlaps between data from
    # historical and successor countries). So check for a specific year.
    year_to_check = 2022
    all_disasters_for_world = df[(df["country"] == "World") & (df["year"] == year_to_check) &
                                 (df["type"] == "All disasters")].\
        reset_index(drop=True)
    all_disasters_check = df[(df["country"].isin(all_countries)) & (df["year"] == year_to_check) &
                             (df["type"] != "All disasters")].\
        groupby("year").sum(numeric_only=True).reset_index()

    cols_to_check = ["total_dead", "injured", "affected", "homeless", "total_affected",
                     "reconstructed_costs_adjusted", "insured_damages_adjusted"]
    error = f"Aggregate for the World in {year_to_check} does not coincide with the sum of all countries."
    assert all_disasters_for_world[cols_to_check].equals(all_disasters_check[cols_to_check]), error

    error = "All values should be positive."
    assert (df.select_dtypes('number').fillna(0) >= 0).all().all(), error

    error = "List of expected disaster types has changed. Consider updating EXPECTED_DISASTER_TYPES (or renaming 'All disasters')."
    assert set(df["type"]) == set(EXPECTED_DISASTER_TYPES + ["All disasters"]), error

    error = "Column 'total_affected' should be the sum of columns 'injured', 'affected', and 'homeless'."
    assert (df["total_affected"].fillna(0) >= df[["injured", "affected", "homeless"]].sum(axis=1).fillna(0)).all(), error

    error = "There are unexpected nans in data."
    assert df.notnull().all(axis=1).all(), error

    # List of columns whose value should not be larger than population.
    columns_to_inspect = [
        'total_dead',
        'injured',
        'affected',
        'homeless',
        'total_affected',
        'population',
        'total_dead_per_100k_people',
        'injured_per_100k_people',
        'affected_per_100k_people',
        'homeless_per_100k_people',
        'total_affected_per_100k_people',
    ]
    # TODO: Uncomment once disaster duration is taken into account.
    #error = "One disaster should not be able to affect more than the entire population of a country in one year."
    #for column in columns_to_inspect:
    #    assert (df[column] <= df["population"]).all(), error
    # This may be happening because the disaster lasts for several years.
    # It can also be because of inaccuracies in the estimates of affected or population, or due to
    # temporary population.


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow.
    ds_meadow = Dataset(DATA_DIR / f"meadow/emdat/{MEADOW_VERSION}/natural_disasters")
    # Get table from dataset.
    tb_meadow = ds_meadow["natural_disasters"]
    # Create a dataframe from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Select and rename columns.
    df = df[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Remove spurious spaces in entities.
    df["type"] = df["type"].str.strip()

    # Sanity checks.
    sanity_checks_on_inputs(df=df)

    # We are not interested in each individual event, but the number of events of each kind and damages.
    df = df.groupby(["country", "year", "type"]).sum(numeric_only=True).reset_index()

    # Fix issue with faulty dtypes (see more details in the function's documentation).
    df = fix_faulty_dtypes(df=df)

    # Harmonize country names and solve some issues with regions.
    df = harmonize_countries(df=df)

    # Add a new category (or "type") corresponding to the total of all natural disasters.
    all_disasters = df.groupby(["country", "year"]).sum(numeric_only=True).assign(**{"type": "All disasters"}).\
        reset_index()
    df = pd.concat([df, all_disasters], ignore_index=True).sort_values(["country", "year", "type"]).\
        reset_index(drop=True)

    # Add region aggregates.
    df = add_region_aggregates(data=df, index_columns=["country", "year", "type"])

    # Add population including historical countries.
    # For certain countries we have population data only for certain years (e.g. 1900, 1910, but not the years in
    # between). In those cases we interpolate population data.
    df = add_population(df=df, interpolate_missing_population=True, warn_on_missing_countries=True)

    # Add rates per 100,000 people.
    for column in df.drop(columns=["country", "year", "type", "population"]).columns:
        df[f"{column}_per_100k_people"] = df[column] * 1e5 / df["population"]

    # Create data aggregated (using a simple mean) in intervales of 10 years.
    # For example (as explained in the footer of the natural disasters explorer), the value for 1900 of any column
    # should represent the average of that column between 1900 and 1909.
    decade_df = create_decade_data(df=df)

    # Run sanity checks on outputs.
    sanity_checks_on_outputs(df=df, decade_df=decade_df)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year", "type"]).sort_index()
    decade_df = decade_df.set_index(["country", "year", "type"]).sort_index()

    #
    # Save outputs.
    #
    # Create new Garden dataset.
    ds_garden = Dataset.create_empty(dest_dir)
    # Ensure all column names are snake, lower case.
    tb_garden = underscore_table(Table(df))
    decade_tb_garden = underscore_table(Table(decade_df))
    # Get dataset metadata from yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    # Get tables metadata from yaml file.
    tb_garden.update_metadata_from_yaml(N.metadata_path, "natural_disasters_yearly")
    decade_tb_garden.update_metadata_from_yaml(N.metadata_path, "natural_disasters_decadal")
    # Add tables to dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.add(decade_tb_garden)
    ds_garden.save()
