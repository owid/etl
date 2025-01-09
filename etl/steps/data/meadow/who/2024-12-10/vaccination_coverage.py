"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("vaccination_coverage.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Data")

    #
    # Process data.
    # The last row is not needed so we'll drop that, also there is a
    # mysterious region with the code 'WB_LONG_NA' and country name 'NA'
    # this will also drop that
    tb = tb.dropna(subset=["NAME"])
    tb = tb.rename(columns={"NAME": "country", "YEAR": "year"})
    # There are duplicates in the data for country codes with the prefix WB_LONG and WB_SHORT, let's drop those out
    msk = tb["CODE"].str.startswith(("WB_LONG", "WB_SHORT"), na=False)
    tb_filtered = tb[msk]
    tb_filtered = tb_filtered.drop_duplicates(
        subset=["country", "year", "ANTIGEN_DESCRIPTION", "COVERAGE_CATEGORY", "COVERAGE"]
    )
    # Keep the rows that don't have the prefix
    tb = tb[~msk]
    # Concatenate the two tables
    tb = pr.concat([tb, tb_filtered], ignore_index=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "antigen_description", "coverage_category"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
