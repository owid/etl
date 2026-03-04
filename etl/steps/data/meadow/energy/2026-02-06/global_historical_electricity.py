"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

TABLE_NAMES = {
    "Table B1. World shares of electricity production per energy source (%)": "electricity_production_share_by_source",
    "Table B2. World shares of electricity consumption by sector and subsector (%)": "electricity_consumption_share_by_sector",
    "Table B3. World shares of electricity consumption allocated per end-use (%)": "electricity_consumption_share_by_end_use",
    "Table B4. World primary to useful, primary to final\nand final to useful exergy efficiencies (%)": "exergy_efficiency_overall",
    "Table B5. Final to useful exergy efficiencies for each end use,  for the period between 1900 to 2017 (%)": "exergy_efficiency_end_use",
    "Table B6. Final-to-useful exergy efficiency\nfor the different sectors (%)": "exergy_efficiency_sector",
    "Table B7. Carbon intensity (kgCO2/kWh)": "carbon_intensity",
    "Table B8. Carbon dioxide emissions\nfrom electricity generation  (Mt CO2eq)": "co2_emissions",
    "Table B9.  Final-to-useful exergy efficiencies for LTH end-uses(%)": "exergy_efficiency_lth_end_use",
    "Table B10. World electricity production and consumption\n (TWh)": "electricity_production_and_consumption",
}


def read_tables(archive):
    columns = archive.read("1-s2.0-S036054422300169X-mmc2.xlsx", skiprows=12).iloc[0]
    tb = archive.read("1-s2.0-S036054422300169X-mmc2.xlsx", skiprows=13)

    table_starts = [idx for idx, name in enumerate(columns.index) if isinstance(name, str) and name.startswith("Table")]
    table_ends = table_starts[1:] + [len(columns)]

    tables = []
    for start, end in zip(table_starts, table_ends):
        header = columns.iloc[start:end]
        valid = header.notna()
        sub = tb.iloc[:, start:end].loc[:, valid.values].copy()
        header = header[valid].astype(str).str.replace("\n", " ").str.strip()
        sub.columns = header

        assert "Year" in sub.columns, f"Missing Year column in table: {columns.index[start]}"
        sub = sub.dropna(subset=["Year"]).rename(columns={"Year": "year"})
        sub = sub[["year"] + [c for c in sub.columns if c != "year"]]

        # Ensure all tables have rows from 1900 until 2017.
        assert len(sub) == 118

        table_key = str(columns.index[start])
        assert table_key in TABLE_NAMES, f"Missing table short_name for header: {table_key}"
        short_name = TABLE_NAMES[table_key]
        tables.append(sub.format(["year"], short_name=short_name))

    return tables


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("global_historical_electricity.zip")

    # Load data from snapshot.
    with snap.extracted() as archive:
        tables = read_tables(archive)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
