import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from shared import CURRENT_DIR

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

# Details of input datasets.
# Even though the author of the dataset pointed out the existence of a Google spreadsheet,
# it seems that its data is incomplete, so we will use both data from the spreadsheet and from their website.
MEADOW_WEB_DATASET_PATH = DATA_DIR / "meadow/aviation_safety_network/2022-10-12/aviation_statistics"
MEADOW_SHEET_DATASET_PATH = DATA_DIR / "meadow/aviation_safety_network/2022-10-14/aviation_statistics"
# Details of output dataset.
GARDEN_DATASET_NAME = "aviation_statistics"
# Get naming conventions.
N = PathFinder(str(CURRENT_DIR / "aviation_statistics"))
WDI_DATASET_PATH = DATA_DIR / "garden/worldbank_wdi/2022-05-26/wdi"

# Select columns from WDI table, and how to rename them.
WDI_COLUMNS = {
    "country": "country",
    "year": "year",
    "is_air_dprt": "departures_worldwide",
    "is_air_psgr": "passengers_carried",
}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load Meadow dataset from the web of the Aviation Safety Network (ASN).
    ds_web_meadow = Dataset(MEADOW_WEB_DATASET_PATH)
    # Load Meadow dataset from the spreadsheet.
    ds_sheet_meadow = Dataset(MEADOW_SHEET_DATASET_PATH)
    # Load tables from datasets.
    tb_web_meadow = ds_web_meadow[ds_web_meadow.table_names[0]]
    tb_sheet_meadow = ds_sheet_meadow[ds_sheet_meadow.table_names[0]]
    # Create dataframes out of the tables.
    df_web = pd.DataFrame(tb_web_meadow).reset_index()
    df_sheet = pd.DataFrame(tb_sheet_meadow).reset_index()
    # Load WDI dataset, and the only table it contains.
    wdi_ds = Dataset(WDI_DATASET_PATH)
    # Load the only table it contains.
    wdi_tb = wdi_ds[wdi_ds.table_names[0]]
    # Create a dataframe out of that table.
    wdi_df = pd.DataFrame(wdi_tb).reset_index()

    #
    # Process data.
    #
    # Select required columns from WDI data, and rename them.
    wdi_df = wdi_df[list(WDI_COLUMNS)].rename(columns=WDI_COLUMNS)

    # Select only global data.
    wdi_df = wdi_df[wdi_df["country"] == "World"].reset_index(drop=True)

    # Select columns that could not be found in spreadsheet from dataframe extracted from their website.
    web_columns = {
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
    }
    df_web = df_web[list(web_columns)].rename(columns=web_columns)

    # Select columns from the spreadsheet.
    sheet_columns = {
        "country": "country",
        "year": "year",
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
    df_sheet = df_sheet[list(sheet_columns)].rename(columns=sheet_columns)

    # Combine both dataframes.
    df_combined = (
        pd.merge(df_sheet, df_web, how="outer", on=["country", "year"]).sort_values("year").reset_index(drop=True)
    )

    # Combine ASN with WDI data.
    df_combined = pd.merge(df_combined, wdi_df, how="left", on=["country", "year"])

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

    #
    # Save outputs.
    #
    # Create a new empty garden dataset.
    ds_garden = Dataset.create_empty(dest_dir)
    # Ensure all columns are snake, lower case.
    tb_garden = underscore_table(Table(df_combined))
    # Get metadata from yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    tb_garden.update_metadata_from_yaml(N.metadata_path, "aviation_statistics")
    # Add table to dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()
