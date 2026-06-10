"""Load snapshot and create a meadow dataset.

The IPCC codes lookup is loaded from a dedicated snapshot (`ipcc_codes.csv`) pinned to an old
commit of the ECP repo, since the file was removed from the upstream `main` branch during the
2026 restructure.

"""

import zipfile

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Since v2026.2, the data is distributed as a ZIP from the producer's website
# (https://worldcarbonpricing.org/developers), with data grouped under a versioned directory.
DATA_PATH = "WCPD_v2026.2/CO2/"

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

    # Read IPCC codes from the dedicated snapshot (pinned to a commit where the file still exists).
    snap_codes = paths.load_snapshot("ipcc_codes.csv")
    tb_ipcc_codes = snap_codes.read_csv(dtype=object)
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
