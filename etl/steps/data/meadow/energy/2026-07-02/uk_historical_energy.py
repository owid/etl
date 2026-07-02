"""Load a snapshot and create a meadow dataset.

The NIC historical energy file has many sheets; for now we only extract UK coal production from the coal long-run sheet
("1.3.1 CoalLR"). That sheet has a multi-row header, so we skip down to the row holding the column names ("Year" and
"Supply2") and select those two columns.

"""

import warnings

from etl.helpers import PathFinder

# Ignore unnecessary warnings when loading the file.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("uk_historical_energy.xlsx")

    # Load the coal long-run sheet, using its header row (which holds "Year" and "Supply2") as column names.
    tb = snap.read_excel(sheet_name="1.3.1 CoalLR", skiprows=6)

    #
    # Process data.
    #
    # Sanity check on the sheet structure (fail loudly if the source layout changed).
    error = "File structure of the coal long-run sheet has changed."
    assert {"Year", "Supply2"} <= set(tb.columns), error

    # Keep the year column and "Supply2" (total coal production, in million tonnes), and rename them.
    tb = tb[["Year", "Supply2"]].rename(columns={"Year": "year", "Supply2": "coal_production_mt"}, errors="raise")

    # Keep only rows whose year is a 4-digit integer (drops the leftover header and footnote rows).
    tb = tb[tb["year"].astype(str).str.match(r"^\d{4}(\.0)?$", na=False)].reset_index(drop=True)
    tb["year"] = tb["year"].astype(float).astype(int)

    # Ensure production is numeric.
    tb["coal_production_mt"] = tb["coal_production_mt"].astype(float)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
