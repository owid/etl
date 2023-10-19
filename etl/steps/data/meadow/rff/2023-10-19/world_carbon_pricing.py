"""Load snapshot and create a meadow dataset"""

import zipfile

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select and rename in ipcc column names.
IPCC_COLUMNS = {
    "IPCC_CODE": "ipcc_code",
    "FULLNAME": "sector_name",
}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_snapshot("world_carbon_pricing.zip")

    # Read necessary data from compressed folder.
    with zipfile.ZipFile(snap.path) as z:
        # Read data files corresponding to national data (within the zipped folder).
        national_files = sorted(
            [
                path
                for path in z.namelist()
                if path.startswith("WorldCarbonPricingDatabase-master/_dataset/data/CO2/national/")
                if path.endswith(".csv")
            ]
        )
        tb_national = pr.concat(
            [
                pr.read_csv(
                    z.open(file_name), metadata=snap.to_table_metadata(), origin=snap.metadata.origin, underscore=True
                )
                for file_name in national_files
            ],
            ignore_index=True,
            short_name="carbon_pricing_at_national_level",
        )

        # Read data files corresponding to sub-national data (within the zipped folder).
        subnational_files = sorted(
            [
                path
                for path in z.namelist()
                if path.startswith("WorldCarbonPricingDatabase-master/_dataset/data/CO2/subnational/")
                if path.endswith(".csv")
            ]
        )
        tb_subnational = pr.concat(
            [
                pr.read_csv(
                    z.open(file_name), metadata=snap.to_table_metadata(), origin=snap.metadata.origin, underscore=True
                )
                for file_name in subnational_files
            ],
            ignore_index=True,
            short_name="carbon_pricing_at_subnational_level",
        )

        # TODO: It seems that the IPCC file is no longer where it used to.
        #  Instead, I think we can take it from the ECP snapshot:
        #  https://github.com/g-dolphin/ECP/blob/master/_raw/_aux_files/ipcc2006_iea_category_codes.csv
        # Therefore, consider if this step should load both snapshots and create all tables in the same step.

        # # Read IPCC codes from file.
        # ipcc_codes_file = [path for path in z.namelist() if path == "WorldCarbonPricingDatabase-master/_raw/_aux_files/misc/ipcc2006_category_codes_links.csv"][0]
        # tb_ipcc_codes = pr.read_csv(z.open(ipcc_codes_file), metadata=snap.to_table_metadata(), origin=snap.metadata.origin, dtype=object)
        # tb_ipcc_codes.metadata.short_name="ipcc_codes"

    #
    # Process data.
    #
    # Prepare IPCC codes dataframe.
    # tb_ipcc_codes = tb_ipcc_codes[list(IPCC_COLUMNS)].rename(columns=IPCC_COLUMNS, errors="raise")
    # # Sanity check.
    # error = "IPCC codes found in data that are missing in IPCC codes file."
    # assert set(df["ipcc_code"]) <= set(ipcc_codes["ipcc_code"]), error
    # # Add sector names to data, mapping IPCC codes.
    # tb_national = tb_national.merge(ipcc_codes, on="ipcc_code", how="left")
    # tb_subnational = tb_subnational.merge(ipcc_codes, on="ipcc_code", how="left")

    # Set an appropriate index and sort conveniently.
    tb_national = (
        tb_national.set_index(["jurisdiction", "year", "ipcc_code", "product"], verify_integrity=True)
        .sort_index()
        .sort_index(axis=1)
    )
    tb_subnational = (
        tb_subnational.set_index(["jurisdiction", "year", "ipcc_code", "product"], verify_integrity=True)
        .sort_index()
        .sort_index(axis=1)
    )

    #
    # Save outputs.
    #
    # Create new dataset with metadata from walden.
    ds_meadow = create_dataset(dest_dir=dest_dir, tables=[tb_national, tb_subnational], check_variables_metadata=True)
    ds_meadow.save()
