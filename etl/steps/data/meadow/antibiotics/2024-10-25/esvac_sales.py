"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("esvac_sales.zip")

    tables = Table()
    for year in range(2010, 2023):
        # Load data from snapshot.
        tb = snap.read_in_archive(filename=f"esvac/esvac_{year}.xlsx", sheet_name="Overall sales", skiprows=5)
        tb["year"] = year
        assert tb.columns[2] == str(year), f"Year {year} not found in the table"
        cols = [
            "country",
            "tablets_tonnes",
            "tablets_share_of_sales",
            "all_other_forms_tonnes",
            "all_other_forms_share_of_sales",
            "total_tonnes",
            "total_share_of_sales",
            "year",
        ]
        assert len(tb.columns) == len(cols)
        tb.columns = cols
        tb = tb.dropna(
            subset=[
                "tablets_tonnes",
                "tablets_share_of_sales",
                "all_other_forms_tonnes",
                "all_other_forms_share_of_sales",
                "total_tonnes",
                "total_share_of_sales",
            ]
        )
        tables = pr.concat([tables, tb])
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tables.format(["country", "year"], short_name="esvac_sales")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
