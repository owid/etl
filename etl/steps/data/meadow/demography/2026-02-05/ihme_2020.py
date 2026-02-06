"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Define list with snapshots and their corresponding relevant files
SNAPSHOT_FILES = {
    "ihme_2020_fertility.zip": "IHME_POP_2017_2100_TFR_Y2020M05D01.CSV",
    "ihme_2020_migration.zip": "IHME_POP_2017_2100_MIGRATION_Y2020M05D01.CSV",
    "ihme_2020_population.zip": "IHME_POP_2017_2100_POP_BOTH_SEX_ALL_AGE_Y2020M05D01.CSV",
    "ihme_2020_life_expectancy.zip": "IHME_POP_2017_2100_LIFE_EXPECTANCY_Y2020M05D01.CSV",
    "ihme_2020_population_retro.zip": "IHME_POP_2017_2100_POP_PAST_Y2020M05D01.CSV",
}


def run() -> None:
    #
    # Load inputs.
    #
    tables = []
    for snapshot_name, fname in SNAPSHOT_FILES.items():
        paths.log.info(f"Loading snapshot '{snapshot_name}' and file '{fname}'")
        # Retrieve snapshot.
        snap = paths.load_snapshot(snapshot_name)

        # Load data from snapshot.
        with snap.extracted() as archive:
            tb = archive.read(fname, force_extension="csv")

        #
        # Process data.
        #
        # Change table name
        short_name = tb.m.short_name
        assert isinstance(short_name, str), f"Expected short name to be a string, got {type(short_name)}"
        assert (
            paths.short_name in short_name
        ), f"Snapshot name {snapshot_name} does not contain expected short name {paths.short_name}"
        tb.m.short_name = short_name.replace(f"{paths.short_name}_", "")

        # Replace unknown scenario with "reference" scenario.
        if "scenario_name" in tb.columns:
            tb["scenario_name"] = tb["scenario_name"].fillna("estimates_past")

        # Drop duplicates
        tb = tb.drop(columns=["location_id"])
        tb = tb.drop_duplicates()
        # break
        # Format
        columns_index = tb.columns.intersection(
            ["location_name", "year_id", "measure_name", "age_group_name", "sex", "scenario_name"]
        )
        tb = tb.format(columns_index)

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()
