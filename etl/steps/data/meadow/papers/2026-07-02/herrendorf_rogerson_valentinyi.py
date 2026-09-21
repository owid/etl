"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Data file inside the snapshot archive.
DATA_FILE = "Handbook_data/HistoricTimeSeries.xls"

# Sheets with long-run series by country. The remaining sheets ("PCE USA", "Graph Data")
# contain consumption expenditure data and chart inputs not used here.
COUNTRY_SHEETS = ["BEL", "ESP", "FIN", "FRA", "JPN", "KOR", "NLD", "SWE", "UK", "USA"]

# Column layout of each country sheet: year in column 0, value added at current prices
# (agriculture, manufacturing, services, total) in columns 4-7, and employment
# (agriculture, manufacturing, services, total) in columns 9-12. The "manufacturing"
# labels correspond to the broad industry sector; utilities are included in services.
COLUMNS = {
    0: "year",
    4: "va_agriculture",
    5: "va_industry",
    6: "va_services",
    7: "va_total",
    9: "emp_agriculture",
    10: "emp_industry",
    11: "emp_services",
    12: "emp_total",
}

# Header rows above the data in each sheet.
N_HEADER_ROWS = 5


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("herrendorf_rogerson_valentinyi.zip")

    # Load the long-run series of each country from the historic time series workbook.
    tables = []
    with snap.extracted() as archive:
        for sheet in COUNTRY_SHEETS:
            tb_sheet = archive.read(DATA_FILE, sheet_name=sheet, header=None)
            tb_sheet = tb_sheet.iloc[N_HEADER_ROWS:, list(COLUMNS.keys())].rename(columns=COLUMNS)
            tb_sheet["country"] = sheet
            tables.append(tb_sheet)

    tb = pr.concat(tables, ignore_index=True)

    #
    # Process data.
    #
    # Ensure numeric values and drop rows without a year or without any data.
    data_columns = [column for column in tb.columns if column not in ("country", "year")]
    for column in data_columns:
        tb[column] = pr.to_numeric(tb[column], errors="coerce")
    tb["year"] = pr.to_numeric(tb["year"], errors="coerce")
    tb = tb.dropna(subset=["year"])
    tb["year"] = tb["year"].astype(int)
    tb = tb.dropna(subset=data_columns, how="all")

    # Improve tables format.
    tables = [tb.format(["country", "year"], short_name=paths.short_name)]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
