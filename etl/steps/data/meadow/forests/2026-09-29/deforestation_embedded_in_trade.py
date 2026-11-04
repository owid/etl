"""Load the DeDuCE physical trade model file and its commodity lookup, and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

COLUMNS = {
    "Producer country": "producer_country",
    "Year": "year",
    "Commodity group": "commodity_group",
    "Commodity": "commodity",
    "Consumer country": "consumer_country",
    "Deforestation risk, amortized (ha)": "deforestation_risk",
    "Deforestation emissions incl. peat drainage, amortized (MtCO2)": "deforestation_emissions",
    "Consumption": "consumption",
}

LOOKUP_COLUMNS = {
    "Item name": "commodity",
    "FAO Item code": "fao_item_code",
    "Group": "commodity_group",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("deforestation_embedded_in_trade.csv")
    tb = snap.read_csv()

    snap_lookup = paths.load_snapshot("deforestation_commodity_lookup.xlsx")
    tb_lookup = snap_lookup.read_excel(sheet_name="Lookup-FAO commodity")

    #
    # Process data.
    #
    tb = tb.rename(columns=COLUMNS, errors="raise")[list(COLUMNS.values())]

    tb = tb.format(["producer_country", "consumer_country", "commodity", "year"])

    # The lookup gives each commodity its FAOSTAT item code and commodity group. Item codes are whole numbers; the
    # only blank is the row for forest plantations, which have no FAOSTAT item.
    tb_lookup = tb_lookup.rename(columns=LOOKUP_COLUMNS, errors="raise")[list(LOOKUP_COLUMNS.values())]
    tb_lookup["fao_item_code"] = tb_lookup["fao_item_code"].astype("Int64")

    tb_lookup = tb_lookup.format(["commodity"], short_name="deforestation_commodity_lookup")

    #
    # Save outputs.
    #
    # Keep the source's float64 precision: repacking would store the values as float32.
    ds_meadow = paths.create_dataset(tables=[tb, tb_lookup], default_metadata=snap.metadata, repack=False)
    ds_meadow.save()
