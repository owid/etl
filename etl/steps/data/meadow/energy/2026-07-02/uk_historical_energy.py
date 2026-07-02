"""Load a snapshot and create a meadow dataset.

The NIC historical energy file has many sheets; for now we only extract UK coal production from the coal long-run sheet
("1.3.1 CoalLR"). The sheet has a multi-row header, so we read it without a header and select the relevant columns by
position after locating the header row.

"""

import warnings

from etl.helpers import PathFinder

# Ignore unnecessary warnings when loading the file.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Name of the coal long-run sheet in the NIC file.
COAL_SHEET = "1.3.1 CoalLR"


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("uk_historical_energy.xlsx")

    # Load the coal long-run sheet without interpreting any row as a header (the header spans several rows).
    tb = snap.read_excel(sheet_name=COAL_SHEET, header=None)

    #
    # Process data.
    #
    # Sanity checks on the sheet structure (fail loudly if the source layout changed).
    error = "File structure of the coal long-run sheet has changed."
    assert tb.iloc[6, 0] == "Year", error
    assert tb.iloc[6, 1] == "Supply2", error

    # Keep the year column and "Supply2" (total coal production, in million tonnes), and rename them.
    # NOTE: We rename by assigning to ".columns" (rather than ".rename") because the integer column labels created by
    # reading with header=None would otherwise cause the column origins to be dropped.
    tb = tb.iloc[:, [0, 1]].copy()
    tb.columns = ["year", "coal_production_mt"]

    # Keep only rows whose year is a 4-digit integer (drops title, header and footnote rows).
    tb = tb[tb["year"].astype(str).str.match(r"^\d{4}(\.0)?$", na=False)].reset_index(drop=True)
    tb["year"] = tb["year"].astype(float).astype(int)

    # Ensure production is numeric.
    tb["coal_production_mt"] = tb["coal_production_mt"].astype(float)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index()
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
