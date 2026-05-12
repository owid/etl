"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("guinea_worm.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("guinea_worm.csv")

    # Load data from snapshot.
    tb = snap.read_csv(skiprows=2)
    tb = clean_certification_table(tb).reset_index(drop=True)
    #
    # Process data.
    #
    # Ensure all columns are snake-case.
    tb = tb.underscore()
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("guinea_worm.end")


def clean_certification_table(tb: Table) -> Table:
    tb = tb.rename(columns={tb.columns[0]: "country", tb.columns[24]: "year_certified"})
    tb["year_certified"] = tb["year_certified"].str.replace(r"Countries certified in", "", regex=True)

    tb = tb.replace(
        {
            "year_certified": {
                "Countries at precertification stage": "Pre-certification",
                "Countries currently endemic for dracunculiasis": "Endemic",
                "Countries not known to have dracunculiasis but yet to be certified": "Pending surveillance",
            }
        }
    )

    return tb
