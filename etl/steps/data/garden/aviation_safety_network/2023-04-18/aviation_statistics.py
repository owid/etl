"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Select columns.
COLUMNS = {
    # Columns that could not be found in spreadsheet from dataframe extracted from their website.
    "country": "country",
    "year": "year",
    "hijackings": "hijacking_incidents",
    "hijacking_fatalities": "hijacking_fatalities",
    # Unused columns.
    # 'airliner_accidents',
    # 'airliner_fatalities',
    # 'cargo',
    # 'charter',
    # 'corp__jet_accidents',
    # 'corp__jet_fatalities',
    # 'domestic_scheduled_passenger',
    # 'ferry__postioning',
    # 'intl_scheduled_passenger',
    # 'training',
    # Columns from the spreadsheet.
    "accidents_with_passenger_and_cargo_flights_including_hijacking_etc": "accidents_with_passenger_and_cargo_flights_including_hijacking_etc",
    "fatalities_with_passenger_and_cargo_flights_including_hijacking_etc": "fatalities_with_passenger_and_cargo_flights_including_hijacking_etc",
    # Unused columns:
    # 'accidents_excluding_hijacking_etc',
    # 'accidents_including_hijacking_etc',
    # 'accidents_with_corporate_jets',
    # 'accidents_with_passenger_flights_including_hijacking_etc',
    # 'fatalities_excluding_hijacking_etc',
    # 'fatalities_including_hijacking_etc',
    # 'fatalities_with_corporate_jets',
    # 'fatalities_with_passenger_flights_including_hijacking_etc',
}

# Select columns from WDI table, and how to rename them.
WDI_COLUMNS = {
    "country": "country",
    "year": "year",
    "is_air_dprt": "departures_worldwide",
    "is_air_psgr": "passengers_carried",
}


def run(dest_dir: str) -> None:
    log.info("aviation_statistics.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("aviation_statistics")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["aviation_statistics"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    # Load WDI dataset.
    ds_wdi: Dataset = paths.load_dependency("wdi")

    # Read table from WDI dataset.
    tb_wdi = ds_wdi["wdi"]

    # Create a dataframe with data from the table.
    df_wdi = pd.DataFrame(tb_wdi).reset_index()

    #
    # Process data.
    #
    # Select required columns, and rename them.
    df = df[list(COLUMNS)].rename(columns=COLUMNS)

    # Select required columns from WDI data, and rename them.
    df_wdi = df_wdi[list(WDI_COLUMNS)].rename(columns=WDI_COLUMNS)

    # Select only global data.
    df_wdi = df_wdi[df_wdi["country"] == "World"].reset_index(drop=True)

    # To avoid issues when merging with ASN data (which has integer columns with nans) convert to new pandas dtypes.
    df_wdi = df_wdi.astype({"departures_worldwide": "Float64", "passengers_carried": "Int64"})

    # Combine ASN with WDI data.
    df_combined = pd.merge(df, df_wdi, how="left", on=["country", "year"])

    # Add new variables.
    df_combined["million_passengers_per_fatality"] = (
        df_combined["passengers_carried"]
        * 1e-6
        / df_combined["fatalities_with_passenger_and_cargo_flights_including_hijacking_etc"]
    )
    df_combined["fatalities_per_million_flights"] = (
        df_combined["fatalities_with_passenger_and_cargo_flights_including_hijacking_etc"]
        / df_combined["departures_worldwide"]
        * 1e6
    )
    df_combined["fatalities_per_million_passengers"] = (
        df_combined["fatalities_with_passenger_and_cargo_flights_including_hijacking_etc"]
        / df_combined["passengers_carried"]
        * 1e6
    )
    df_combined["accidents_per_million_flights"] = (
        df_combined["accidents_with_passenger_and_cargo_flights_including_hijacking_etc"]
        / df_combined["departures_worldwide"]
        * 1e6
    )
    df_combined["million_flights_per_accident"] = (
        df_combined["departures_worldwide"]
        * 1e-6
        / df_combined["accidents_with_passenger_and_cargo_flights_including_hijacking_etc"]
    )

    # Set an appropriate index and sort conveniently.
    df_combined = df_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create a new table with the processed data.
    tb_garden = Table(df_combined, short_name="aviation_statistics")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("aviation_statistics.end")
