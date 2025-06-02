"""Load snapshot and create a meadow dataset.

The IPCC codes file used to be given in the World Carbon Pricing (WCP) repos, but that is no longer the case.
For that reason, we load those codes from the Emissions-Weighted Carbon Price (ECP) repos.

"""

import zipfile

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common path within the WCP compressed folder to the national/subnational data files.
DATA_PATH = "WorldCarbonPricingDatabase-main/_dataset/data/CO2/"

# Path within the ECP compressed folder to the IPCC codes file.
IPCC_CODES_PATH = "ECP-master/_raw/_aux_files/ipcc2006_iea_category_codes.csv"

# Columns to select and rename in ipcc column names.
IPCC_COLUMNS = {
    "ipcc_code": "ipcc_code",
    "FULLNAME": "sector_name",
}


def run() -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_snapshot("world_carbon_pricing.zip")

    # Just to extract the IPCC codes, load the ECP snapshot.
    snap_ecp = paths.load_snapshot("emissions_weighted_carbon_price.zip")

    # Read necessary data from compressed folder.
    with zipfile.ZipFile(snap.path) as z:
        # Extract metadata and origin from snapshot.
        metadata = snap.to_table_metadata()
        origin = snap.metadata.origin

        # Read data files corresponding to national data (within the zipped folder), and create a table with them.
        data_national = [
            pr.read_csv(z.open(path), metadata=metadata, origin=origin, underscore=True)
            for path in sorted(z.namelist())
            if path.startswith(f"{DATA_PATH}national/") and path.endswith(".csv")
        ]
        tb_national = pr.concat(data_national, ignore_index=True, short_name="world_carbon_pricing_national_level")

        # Read data files corresponding to sub-national data (within the zipped folder) and create a table with them.
        data_subnational = [
            pr.read_csv(z.open(path), metadata=metadata, origin=origin, underscore=True)
            for path in sorted(z.namelist())
            if path.startswith(f"{DATA_PATH}subnational/") and path.endswith(".csv")
        ]
        tb_subnational = pr.concat(
            data_subnational, ignore_index=True, short_name="world_carbon_pricing_subnational_level"
        )

    # Read IPCC codes from the ECP snapshot (since, for some reason, it is no longer available in the WCP repos).
    # NOTE: To avoid mixing metadata and origins from ECP and WCP, use the origins from WCP.
    with zipfile.ZipFile(snap_ecp.path) as z:
        tb_ipcc_codes = pr.read_csv(z.open(IPCC_CODES_PATH), metadata=metadata, origin=origin, dtype=object)
        tb_ipcc_codes.metadata.short_name = "ipcc_codes"

    #
    # Process data.
    #
    # Prepare IPCC codes.
    tb_ipcc_codes = tb_ipcc_codes[list(IPCC_COLUMNS)].rename(columns=IPCC_COLUMNS, errors="raise")

    # Sanity check.
    error = "IPCC codes found in data that are missing in IPCC codes file."
    assert set(tb_national["ipcc_code"]) == set(tb_ipcc_codes["ipcc_code"]), error
    assert set(tb_subnational["ipcc_code"]) == set(tb_ipcc_codes["ipcc_code"]), error

    # Add sector names to data, mapping IPCC codes.
    tb_national = tb_national.merge(tb_ipcc_codes, on="ipcc_code", how="left")
    tb_subnational = tb_subnational.merge(tb_ipcc_codes, on="ipcc_code", how="left")

    # Improve table formats.
    tb_national = tb_national.format(keys=["jurisdiction", "year", "ipcc_code", "product"], sort_columns=True)
    tb_subnational = tb_subnational.format(keys=["jurisdiction", "year", "ipcc_code", "product"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb_national, tb_subnational], default_metadata=snap.metadata)
    ds_meadow.save()
