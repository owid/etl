"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its main table.
    snap = paths.load_snapshot("dietary_choices_uk.xlsx")
    data = snap.ExcelFile()

    #
    # Process data.
    #
    # Combine all sheets into a single table.
    tb = pr.concat(
        [data.parse(sheet_name=sheet_name).assign(**{"group": sheet_name}) for sheet_name in data.sheet_names]
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["which_of_these_best_describes_your_diet", "group"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
