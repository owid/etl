"""Load snapshot of Ember's Yearly Electricity Data and create a raw data table.

"""
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_dependency("yearly_electricity.csv")
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    # Create a table with metadata and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["area", "year", "variable", "unit"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
