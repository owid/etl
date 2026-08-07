"""Load snapshot of Ember's Monthly Electricity Data and create a raw data table.

Unlike the yearly data (long format: one row per area/year/variable/unit), the monthly file is
semi-wide: one row per (area, month, electricity source) with the metrics as columns. We keep it in
that long-by-source shape here and pivot it into per-source columns in the garden step.
"""

from etl.helpers import PathFinder

# Get naming conventions.
paths = PathFinder(__file__)

# Columns to keep from the raw monthly file, and how to rename them.
COLUMNS = {
    "Area": "country",
    "Date": "date",
    "Electricity source": "electricity_source",
    "Is aggregated source": "is_aggregated_source",
    "Generation (TWh)": "generation__twh",
    "Share of generation (%)": "share_of_generation__pct",
    "Emissions (MtCO2e)": "emissions__mtco2e",
    "Emissions intensity (gCO2e/kWh)": "emissions_intensity__gco2e_kwh",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("monthly_electricity.csv")
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")
    # Parse the monthly date (first day of each month).
    tb["date"] = tb["date"].astype("datetime64[ns]")
    # Low-cardinality string columns as categoricals (small feather, fast reads).
    for column in ["country", "electricity_source"]:
        tb[column] = tb[column].astype("category")
    tb = tb.format(keys=["country", "date", "electricity_source"], sort_columns=True)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
