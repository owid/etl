"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("atkinson_2008_finland.xls")

    # Load data from snapshot
    tb = snap.read(safe_types=False, sheet_name="Table F.3 (E and J", usecols="C,E", skiprows=2)

    #
    # Process data.
    #
    tb = rename_columns_and_add_country(tb=tb, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def rename_columns_and_add_country(tb: Table, short_name: str) -> Table:
    """
    - Rename the unnamed column to year and the P90 one to p90_p50_ratio
    - Add a country column with the value Canada
    - Remove missing rows, create indices and add short_name
    """

    tb.columns = ["year", "p90_p50_ratio"]
    tb["country"] = "Finland"
    tb = tb.dropna()

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"], short_name=short_name)

    return tb
