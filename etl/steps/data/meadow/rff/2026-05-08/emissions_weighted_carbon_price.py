"""Load snapshot and create a meadow dataset."""

import fnmatch
import zipfile

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Path glob within the ECP compressed folder to the economy data file (time-varying-weights variant).
# v2026.1 splits the economy file into ecp_fw (fixed weights), ecp_vw (time-varying weights), and
# ecp_intro (jurisdictional, no supra columns). Empirically, ecp_vw is the successor to the old
# single `ecp_CO2.csv` from v2025.0 — values match for France, Germany, UK, Sweden, Japan, Canada,
# China, Mexico (within ~$1). The fixed-weights variant drops countries with newly-introduced taxes
# covering sectors that had small emission shares in the base year (e.g. Uruguay, Argentina, South
# Africa, plus much smaller values for Canada and China). The filename carries a build-date suffix
# that changes per release, so we glob the directory rather than hard-code the date.
DATA_PATH_ECONOMY_GLOB = "ECP-main/_output/_dataset/v2026.1/ecp/ipcc/ecp_economy/ecp_vw/ecp_tv_CO2_*.csv"

# Path glob for the coverage data file. The filename also carries a build-date suffix.
DATA_PATH_COVERAGE_GLOB = "ECP-main/_output/_dataset/v2026.1/coverage/jurisdictions/tot_coverage_jurisdiction_CO2_*.csv"


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

        names = z.namelist()
        economy_matches = fnmatch.filter(names, DATA_PATH_ECONOMY_GLOB)
        assert len(economy_matches) == 1, f"Expected exactly one economy CSV, found: {economy_matches}"
        coverage_matches = fnmatch.filter(names, DATA_PATH_COVERAGE_GLOB)
        assert len(coverage_matches) == 1, f"Expected exactly one coverage CSV, found: {coverage_matches}"

        # Read economy data.
        tb_economy = pr.read_csv(z.open(economy_matches[0]), metadata=metadata, origin=origin)
        tb_economy.metadata.short_name = "emissions_weighted_carbon_price_economy"

        # Read coverage data.
        tb_coverage = pr.read_csv(z.open(coverage_matches[0]), metadata=metadata, origin=origin)
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
