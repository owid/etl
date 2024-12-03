"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define files directory
FILES_DIRECTORY = "FR_WLD_2024_198/Reproducibility package/Chapter 1/1-data/raw/forecasts"

# Define table parameters
TABLE_PARAMETERS = {
    "country": {"file": "FGTcountry_1990_2050_3pr24.dta", "index": ["country", "year", "povertyline", "scenario"]},
    "region": {"file": "FGTregion_1990_2050_3pr24.dta", "index": ["region_pip", "year", "povertyline", "scenario"]},
    "global": {"file": "FGTglobal_1990_2050_3pr24.dta", "index": ["year", "povertyline", "scenario"]},
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("reproducibility_package_poverty_prosperity_planet.zip")

    # Define empty list to store tables.
    tables = []
    for table, table_config in TABLE_PARAMETERS.items():
        # Load data from snapshot.
        tb = snap.read_in_archive(f"{FILES_DIRECTORY}/{table_config['file']}")

        #
        # Process data.
        #
        # Remove duplicates in the data
        tb = tb.drop_duplicates(subset=table_config["index"])

        # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
        tb = tb.format(keys=table_config["index"], short_name=table)

        # Append table to list.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
