"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

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
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("aviation_statistics")
    tb_meadow = ds_meadow["aviation_statistics"].reset_index()

    # Load WDI dataset and read its main table.
    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi["wdi"].reset_index()

    #
    # Process data.
    #
    # Select required columns, and rename them.
    tb = tb_meadow[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Select required columns from WDI data, and rename them.
    tb_wdi = tb_wdi[list(WDI_COLUMNS)].rename(columns=WDI_COLUMNS, errors="raise")

    # Select only global data.
    tb_wdi = tb_wdi[tb_wdi["country"] == "World"].reset_index(drop=True)

    # To avoid issues when merging with ASN data (which has integer columns with nans) convert to new pandas dtypes.
    tb_wdi = tb_wdi.astype({"departures_worldwide": "Float64", "passengers_carried": "Int64"})

    # Combine ASN with WDI data.
    tb_combined = tb.merge(tb_wdi, how="left", on=["country", "year"])

    # Add new variables.
    tb_combined["million_passengers_per_fatality"] = (
        tb_combined["passengers_carried"]
        * 1e-6
        / tb_combined["fatalities_with_passenger_and_cargo_flights_including_hijacking_etc"]
    )
    tb_combined["fatalities_per_million_flights"] = (
        tb_combined["fatalities_with_passenger_and_cargo_flights_including_hijacking_etc"]
        / tb_combined["departures_worldwide"]
        * 1e6
    )
    tb_combined["fatalities_per_million_passengers"] = (
        tb_combined["fatalities_with_passenger_and_cargo_flights_including_hijacking_etc"]
        / tb_combined["passengers_carried"]
        * 1e6
    )
    tb_combined["accidents_per_million_flights"] = (
        tb_combined["accidents_with_passenger_and_cargo_flights_including_hijacking_etc"]
        / tb_combined["departures_worldwide"]
        * 1e6
    )
    tb_combined["million_flights_per_accident"] = (
        tb_combined["departures_worldwide"]
        * 1e-6
        / tb_combined["accidents_with_passenger_and_cargo_flights_including_hijacking_etc"]
    )

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined], check_variables_metadata=True)
    ds_garden.save()
