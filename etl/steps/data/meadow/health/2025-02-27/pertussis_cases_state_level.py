"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("pertussis_cases_state_level.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive(filename="US.27836007.csv", low_memory=False)

    #
    # Process data.
    tb = tb[tb["PartOfCumulativeCountSeries"] == 0]
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # Some states report for individual counties, so we need to aggregate the data.
    tb_group = (
        tb.groupby(["CountryName", "Admin1Name", "PeriodStartDate", "PeriodEndDate"])["CountValue"].sum().reset_index()
    )
    tables = [
        tb_group.format(["countryname", "admin1name", "periodstartdate", "periodenddate"], short_name=paths.short_name)
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
