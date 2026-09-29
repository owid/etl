"""Load the DeDuCE physical trade model file and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

COLUMNS = {
    "Producer country group": "producer_region",
    "Producer country ISO": "producer_iso_code",
    "Producer country": "producer_country",
    "Year": "year",
    "Commodity group": "commodity_group",
    "Commodity": "commodity",
    "Consumer country group": "consumer_region",
    "Consumer country ISO": "consumer_iso_code",
    "Consumer country": "consumer_country",
    "Deforestation risk, amortized (ha)": "deforestation_risk",
    "Deforestation emissions incl. peat drainage, amortized (MtCO2)": "deforestation_emissions",
    "Consumption": "consumption",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("deforestation_embedded_in_trade.csv")
    tb = snap.read_csv()

    #
    # Process data.
    #
    tb = tb.rename(columns=COLUMNS, errors="raise")

    tb = tb.format(["producer_country", "consumer_country", "commodity", "year"])

    #
    # Save outputs.
    #
    # Keep the source's float64 precision: repacking would store the values as float32.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata, repack=False)
    ds_meadow.save()
