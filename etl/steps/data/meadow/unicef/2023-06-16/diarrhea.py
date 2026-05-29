"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("diarrhea.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("diarrhea.xlsx")

    sheets = ["DIARCARE", "ORS", "ORTCF", "ORSZINC", "ZINC"]
    # Load data from snapshot, one Table per sheet, then concatenate.
    tables = []
    for sheet in sheets:
        tb_sheet = snap.read_excel(sheet_name=sheet)
        tb_sheet["indicator"] = sheet
        tables.append(tb_sheet)
    tb = pr.concat(tables, ignore_index=True)

    tb = tb[["Countries and areas", "Year", "National", "indicator"]]
    tb = tb.rename(columns={"Countries and areas": "country", "Year": "year", "National": "value"})

    # Process data.
    #
    # Ensure all columns are snake-case and set short_name.
    tb = tb.underscore()
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("diarrhea.end")
