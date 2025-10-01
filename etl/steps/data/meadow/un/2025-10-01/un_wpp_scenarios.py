"""NOTE: This meadow step is relatively complete. Why? Because the snapshot steps are quite big, and we want to extract the esential data for next steps. Otherwise, we would be making Garden steps quite slow.

What do we do here?

- Read the XLSX files
- Keep relevant columns
- Format the tables to have them in long format
- Set indices and verify integrity
"""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__=__file__)


# CSV
COLUMNS_RENAME_CSV = {
    "Location": "country",
    "Time": "year",
    "Variant": "variant",
    "AgeGrp": "age",
    "TPopulation1July": "population_july",
    "TPopulationMale1July": "population_male_july",
    "TPopulationFemale1July": "population_female_july",
    "TFR": "total_fertility_rate",
    "LEx": "life_expectancy",
    "CDR": "crude_death_rate",
    "NetMigrations": "net_migration",
}
COLUMNS_INDEX_CSV = list(COLUMNS_RENAME_CSV.values())
# FINAL FORMAT
COLUMNS_INDEX_FORMAT = [
    "country",
    "year",
    "variant",
    "sex",
    "age",
]
SCENARIOS = ["Medium", "Low", "High", "Constant fertility", "Estimates", "Constant mortality", "No change"]
LOCATION_TYPES_CSV = [
    "Country/Area",
    "Geographic region",
    "Income group",
    "Development group",
    "World",
]

COLS_TO_KEEP = [
    "Location",
    "Time",
    "Variant",
    "TPopulation1July",
    "TPopulationMale1July",
    "TPopulationFemale1July",
    "TFR",
    "LEx",
    "CDR",
    "NetMigrations",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    paths.log.info("reading snapshots...")

    # Main file: Demography indicators
    paths.log.info("reading main file: demography indicators...")
    tb_main = read_from_csv("un_wpp_demographic_indicators_scenarios.csv")
    tb_main = tb_main.loc[tb_main["Variant"].isin(SCENARIOS)]
    assert tb_main["Variant"].isin(SCENARIOS).all()
    tb_main = tb_main[tb_main["LocTypeName"].isin(LOCATION_TYPES_CSV)]
    assert tb_main["LocTypeName"].isin(LOCATION_TYPES_CSV).all()
    # Process data.
    tb_main = tb_main[COLS_TO_KEEP]
    tb_main = tb_main.rename(columns=COLUMNS_RENAME_CSV)

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb_main], check_variables_metadata=True, repack=False)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_from_csv(short_name: str) -> Table:
    paths.log.info(f"reading {short_name}...")
    # Read snap
    tb = paths.read_snap_table(short_name, compression="gzip")
    # Drop unused columns
    tb = tb.drop(columns=["Notes"])
    # Filter relevant variants
    tb = tb.loc[tb["Variant"].isin(SCENARIOS)]
    # Optimize memory
    return tb
