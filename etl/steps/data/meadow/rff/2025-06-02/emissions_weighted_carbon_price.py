"""Load snapshot and create a meadow dataset."""

import zipfile

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Path within the ECP compressed folder to the economy data file.
DATA_PATH_ECONOMY = "ECP-master/_dataset/ecp/ipcc/ecp_economy/ecp_CO2.csv"

# Path within the ECP compressed folder to the coverage data file.
DATA_PATH_COVERAGE = "ECP-master/_dataset/coverage/tot_coverage_jurisdiction_CO2.csv"


def sanity_check_outputs(tb):
    error = "There should be one row per jurisdiction-year."
    assert tb[tb.duplicated(subset=["jurisdiction", "year"])].empty, error
    error = "There should not be any row that only has nan data."
    assert tb[tb.drop(columns=["jurisdiction", "year"]).isnull().all(axis=1)].empty, error


def run() -> None:
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
    sanity_check_outputs(tb=tb_economy)
    sanity_check_outputs(tb=tb_coverage)

    # Improve table formats.
    tb_economy = tb_economy.format(keys=["jurisdiction", "year"], sort_columns=True)
    tb_coverage = tb_coverage.format(keys=["jurisdiction", "year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb_economy, tb_coverage], default_metadata=snap.metadata)
    ds_meadow.save()
