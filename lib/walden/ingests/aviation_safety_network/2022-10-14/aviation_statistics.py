"""Get data from the Aviation Safety Network (extracted from a Google Sheet).

"""

import click
import pandas as pd
from shared import CURRENT_DIR

from owid.walden import Dataset
from owid.walden.ingest import add_to_catalog

# Define URL to Google Sheet containing data.
SHEET_ID = "1SDp7p1y6m7N5xD5_fpOkYOrJvd68V7iy6etXy2cetb8"
SHEET_NAME = "Accidents+and+fatalities+per+year"
GOOGLE_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"
# Path to metadata file.
METADATA_PATH = (CURRENT_DIR / "aviation_statistics").with_suffix(".meta.yml")

# Define columns to select from sheet and how to rename them.
COLUMNS = {
    "Accidents \n(excl. suicide, sabotage, hijackings etc.) Year": "year",
    "Accidents ": "accidents_excluding_hijacking_etc",
    "Fatalities": "fatalities_excluding_hijacking_etc",
    "Accidents \n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_including_hijacking_etc",
    "Fatalities.1": "fatalities_including_hijacking_etc",
    "Accidents with passenger flights \n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_with_passenger_flights_including_hijacking_etc",
    "Fatalities.2": "fatalities_with_passenger_flights_including_hijacking_etc",
    "Accidents with passenger + cargo flights\n(incl. suicide, sabotage, hijackings etc.) Accidents ": "accidents_with_passenger_and_cargo_flights_including_hijacking_etc",
    "Fatalities.3": "fatalities_with_passenger_and_cargo_flights_including_hijacking_etc",
    # 'World air traffic (departures)': '',
    # '1 accident \nper x flights': '',
    # 'fatal accidents \nper mln flights': '',
    # '5-year \nmoving avg': '',
    "Corporate jets (civilian) Accidents ": "accidents_with_corporate_jets",
    "Fatalities.4": "fatalities_with_corporate_jets",
    # 'moving 5 year average # of accidents': '',
}


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    # Load raw data from a specific sheet.
    raw = pd.read_csv(GOOGLE_SHEET_URL)

    # Select necessary columns and rename them appropriately.
    df = raw[list(COLUMNS)].rename(columns=COLUMNS)

    # Drop last column (which should be the only one without a year), which gives a grand total.
    df = df.dropna(subset="year").reset_index(drop=True).astype({"year": int})

    # Get walden metadata.
    dataset = Dataset.from_yaml(METADATA_PATH)

    # Add data to Walden catalog and metadata to Walden index.
    add_to_catalog(metadata=dataset, dataframe=df, upload=upload)

    # Update Walden datasets.
    dataset.save()


if __name__ == "__main__":
    main()
