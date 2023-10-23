"""Load snapshot and create a meadow dataset."""

import zipfile

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Path within the ECP compressed folder to the economy data file.
DATA_PATH_ECONOMY = "ECP-master/_dataset/ecp/ecp_economy/ecp_CO2.csv"

# Path within the ECP compressed folder to the coverage data file.
DATA_PATH_COVERAGE = "ECP-master/_dataset/coverage/tot_coverage_jurisdiction_CO2.csv"


def run_sanity_checks(tb):
    error = "There should be one row per jurisdiction-year."
    assert tb[tb.duplicated(subset=["jurisdiction", "year"])].empty, error
    error = "There should not be any row that only has nan data."
    assert tb[tb.drop(columns=["jurisdiction", "year"]).isnull().all(axis=1)].empty, error


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_snapshot("emissions_weighted_carbon_price.zip")

    # Read necessary data from compressed folder.
    with zipfile.ZipFile(snap.path) as z:
        # Extract metadata and origin from snapshot.
        metadata = snap.to_table_metadata()
        origin = snap.metadata.origin

        # Read economy data.
        tb_economy = pr.read_csv(z.open(DATA_PATH_ECONOMY), metadata=metadata, origin=origin)
        tb_economy.metadata.short_name = "emissions_weighted_carbon_price_economy"

        # Read coverage data.
        tb_coverage = pr.read_csv(z.open(DATA_PATH_COVERAGE), metadata=metadata, origin=origin)
        tb_coverage.metadata.short_name = "emissions_weighted_carbon_price_coverage"

    #
    # Process data.
    #
    # Sanity checks.
    run_sanity_checks(tb=tb_economy)
    run_sanity_checks(tb=tb_coverage)

    # Set an appropriate index and sort conveniently.
    index_columns = ["jurisdiction", "year"]
    tb_economy = tb_economy.set_index(index_columns, verify_integrity=True).sort_index().sort_index(axis=1)
    tb_coverage = tb_coverage.set_index(index_columns, verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create new dataset with metadata from walden.
    ds_meadow = create_dataset(
        dest_dir=dest_dir,
        tables=[tb_economy, tb_coverage],
        default_metadata=snap.metadata,
        check_variables_metadata=True,
    )
    ds_meadow.save()
