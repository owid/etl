"""Script to create a snapshot of USDA's monthly series of laying hens and egg-type chicks in the United States.

The data is fetched from the Quick Stats API of the National Agricultural Statistics Service (NASS). The API needs a key,
read from the USDA_NASS_API_TOKEN environment variable (defined in the .env file). You can request a key at
https://quickstats.nass.usda.gov/api/
"""

import io
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Quick Stats API endpoint and key.
API_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
load_dotenv()
API_KEY = os.getenv("USDA_NASS_API_TOKEN")

# Data items to fetch (national totals, for all years and periods available).
DATA_ITEMS = [
    # Layers sold for slaughter during the month.
    "CHICKENS, LAYERS - SALES FOR SLAUGHTER, MEASURED IN HEAD",
    # Layers rendered, died, destroyed, composted, or disappeared for any reason (other than sold) during the month.
    "CHICKENS, LAYERS - LOSS, DEATH & RENDERED, MEASURED IN HEAD",
    # Egg-type chicks hatched during the month.
    "CHICKENS, CHICKS, EGG TYPE - HATCHED, MEASURED IN HEAD",
    # Average number of layers on hand during the month, and during the year (December through November).
    "CHICKENS, LAYERS - INVENTORY, AVG, MEASURED IN HEAD",
    # Average number of layers on hand during the month, by type: table egg layers, and hatching egg layers of broiler
    # type (broiler breeders) and of egg type (layer breeders).
    "CHICKENS, LAYERS, TABLE - INVENTORY, AVG, MEASURED IN HEAD",
    "CHICKENS, LAYERS, HATCHING, BROILER TYPE - INVENTORY, AVG, MEASURED IN HEAD",
    "CHICKENS, LAYERS, HATCHING, EGG TYPE - INVENTORY, AVG, MEASURED IN HEAD",
    # Chickens slaughtered in federally inspected plants: young chickens (broilers), mature chickens (spent laying and
    # breeding hens, split into light type, i.e. spent egg-type hens, and heavy type, i.e. spent broiler breeders), and
    # all chickens.
    "CHICKENS, YOUNG, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD",
    "CHICKENS, MATURE, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD",
    "CHICKENS, MATURE, LIGHT, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD",
    "CHICKENS, MATURE, HEAVY, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD",
    "CHICKENS, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD",
]


def run(upload: bool = True) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"usda_nass/{SNAPSHOT_VERSION}/us_laying_hens_and_chicks.csv")

    error = "Define USDA_NASS_API_TOKEN in the .env file (request a key at https://quickstats.nass.usda.gov/api/)."
    assert API_KEY, error

    # Fetch each data item separately (the API takes one data item per request).
    tables = []
    for data_item in DATA_ITEMS:
        response = requests.get(
            API_URL,
            params={
                "key": API_KEY,
                "source_desc": "SURVEY",
                "commodity_desc": "CHICKENS",
                "short_desc": data_item,
                "agg_level_desc": "NATIONAL",
                "domain_desc": "TOTAL",
                "format": "CSV",
            },
            timeout=300,
        )
        response.raise_for_status()
        table = pd.read_csv(io.BytesIO(response.content), low_memory=False)
        assert len(table) > 0, f"No data returned for {data_item}."
        tables.append(table)
    df = pd.concat(tables, ignore_index=True)

    # Use the date of the latest data load among the series fetched as the publication date.
    snap.metadata.origin.date_published = pd.to_datetime(df["load_time"]).max().strftime("%Y-%m-%d")  # type: ignore
    snap.metadata.save()

    # Save snapshot.
    snap.create_snapshot(data=df, upload=upload)
