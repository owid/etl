"""NOTE: This meadow step is relatively complete. Why? Because the snapshot steps are quite big, and we want to extract the esential data for next steps. Otherwise, we would be making Garden steps quite slow.

What do we do here?

- Read the XLSX files
- Keep relevant columns
- Format the tables to have them in long format
- Set indices and verify integrity
"""
from typing import Dict, List, Optional, Tuple, cast

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.tables import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__=__file__)

# XLSX
COLUMNS_RENAME_XLSX = {
    "Region, subregion, country or area *": "country",
    "Year": "year",
    "Variant": "variant",
}
COLUMNS_INDEX_XLSX = list(COLUMNS_RENAME_XLSX.values())
# CSV
COLUMNS_RENAME_CSV = {
    "Location": "country",
    "Time": "year",
    "Variant": "variant",
    "AgeGrp": "age",
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
SCENARIOS = ["Medium", "Low", "High", "Constant fertility", "Estimates"]
LOCATION_TYPES = [
    "Country/Area",
    "Region",
    "Income Group",
    "Development Group",
    "World",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    paths.log.info("reading snapshots...")

    # Main file: Demography indicators
    paths.log.info("reading main file: demography indicators...")
    tb_main = read_from_xlsx("un_wpp_demographic_indicators.xlsx")

    tb_population_density = clean_table_standard_xlsx(
        tb_main, "Population Density, as of 1 July (persons per square km)", "population_density"
    )
    tb_growth_rate = clean_table_standard_xlsx(tb_main, "Population Growth Rate (percentage)", "growth_rate")
    tb_nat_change = clean_table_standard_xlsx(
        tb_main, "Rate of Natural Change (per 1,000 population)", "natural_change_rate"
    )
    tb_migration = clean_table_standard_xlsx(tb_main, "Net Number of Migrants (thousands)", "net_migration")
    tb_migration_rate = clean_table_standard_xlsx(
        tb_main, "Net Migration Rate (per 1,000 population)", "net_migration_rate"
    )
    tb_death_rate = clean_table_standard_xlsx(tb_main, "Crude Death Rate (deaths per 1,000 population)", "death_rate")
    tb_birth_rate = clean_table_standard_xlsx(tb_main, "Crude Birth Rate (births per 1,000 population)", "birth_rate")
    tb_median_age = clean_table_standard_xlsx(tb_main, "Median Age, as of 1 July (years)", "median_age")
    tb_mortality = make_tb_mortality(tb_main)
    tb_le = make_tb_life_expectancy(tb_main)

    # # Population
    paths.log.info("reading population...")
    tb_population = make_tb_population()

    # # Fertility rate
    tb_fertility, tb_births = make_tb_fertility_births(tb_main)

    # Deaths
    tb_deaths = make_tb_deaths()

    #
    # Save outputs.
    #
    tables = [
        tb_population,
        tb_population_density,
        tb_growth_rate,
        tb_nat_change,
        tb_fertility,
        tb_migration,
        tb_migration_rate,
        tb_deaths,
        tb_death_rate,
        tb_births,
        tb_birth_rate,
        tb_median_age,
        tb_le,
        tb_mortality,
        # tb_childbearing_age,
        # tb_population_doubling,
    ]
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def make_tb_population() -> Table:
    """Make population table."""
    tb_population = read_from_csv("un_wpp_population_estimates.csv")
    tb_population_l = read_from_csv("un_wpp_population_low.csv")
    tb_population_m = read_from_csv("un_wpp_population_medium.csv")
    tb_population_h = read_from_csv("un_wpp_population_high.csv")
    tb_population_c = read_from_csv("un_wpp_population_constant_fertility.csv")
    tb_population = combine_population(
        [
            tb_population,
            tb_population_l,
            tb_population_m,
            tb_population_h,
            tb_population_c,
        ]
    )
    del tb_population_l, tb_population_m, tb_population_h, tb_population_c
    tb_population = tb_population.format(COLUMNS_INDEX_FORMAT, short_name="population")
    return tb_population


def make_tb_mortality(tb_main: Table) -> Table:
    """Make mortality table: includes child and infant mortality rates."""
    paths.log.info("reading mortality data...")
    tb_child_mort = clean_table_standard_xlsx(
        tb_main,
        "Under-Five Mortality (deaths under age 5 per 1,000 live births)",
        "mortality_rate",
        age="0-4",
        format_table=False,
        log=False,
    )
    tb_infant_mort = clean_table_standard_xlsx(
        tb_main,
        "Infant Mortality Rate (infant deaths per 1,000 live births)",
        "mortality_rate",
        age="0",
        format_table=False,
        log=False,
    )
    # Combine tables
    tb = pr.concat([tb_child_mort, tb_infant_mort])
    # Reduce size
    tb = tb.astype({"age": "string"})
    # Format
    tb = tb.format(COLUMNS_INDEX_FORMAT, short_name="mortality_rate")

    return tb


def make_tb_life_expectancy(tb_main: Table) -> Table:
    """Make mortality table: includes child and infant mortality rates."""
    indicator_dimensions = {
        "Life Expectancy at Birth, both sexes (years)": {
            "age": 0,
            "sex": "all",
        },
        "Male Life Expectancy at Birth (years)": {
            "age": 0,
            "sex": "male",
        },
        "Female Life Expectancy at Birth (years)": {
            "age": 0,
            "sex": "female",
        },
        "Life Expectancy at Age 15, both sexes (years)": {
            "age": 15,
            "sex": "all",
        },
        "Male Life Expectancy at Age 15 (years)": {
            "age": 15,
            "sex": "male",
        },
        "Female Life Expectancy at Age 15 (years)": {
            "age": 15,
            "sex": "female",
        },
        "Life Expectancy at Age 65, both sexes (years)": {
            "age": 65,
            "sex": "all",
        },
        "Male Life Expectancy at Age 65 (years)": {
            "age": 65,
            "sex": "male",
        },
        "Female Life Expectancy at Age 65 (years)": {
            "age": 65,
            "sex": "female",
        },
        "Life Expectancy at Age 80, both sexes (years)": {
            "age": 80,
            "sex": "all",
        },
        "Male Life Expectancy at Age 80 (years)": {
            "age": 80,
            "sex": "male",
        },
        "Female Life Expectancy at Age 80 (years)": {
            "age": 80,
            "sex": "female",
        },
    }
    columns_indicators = list(indicator_dimensions.keys())
    tb = tb_main.loc[:, COLUMNS_INDEX_XLSX + columns_indicators]

    # Unpivot
    tb = tb.melt(
        id_vars=COLUMNS_INDEX_XLSX,
        value_name="life_expectancy",
    )

    # Remove NaNs
    tb["life_expectancy"] = tb["life_expectancy"].replace("...", np.nan)
    tb = tb.dropna(subset="life_expectancy")

    # Sex
    tb["sex"] = tb["variable"].map(lambda x: indicator_dimensions[x]["sex"])
    # Age
    tb["age"] = tb["variable"].map(lambda x: indicator_dimensions[x]["age"])

    # Drop variable column
    tb = tb.drop(columns="variable")

    # Set year to int
    tb["year"] = tb["year"].astype(int)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(COLUMNS_INDEX_FORMAT, short_name="life_expectancy")

    return tb


def make_tb_fertility_births(tb_main: Table) -> Tuple[Table, Table]:
    """Make Births and Fertility tables.

    This is done together because the data is in the same file.

    For total births, and total fertility rate, the main file is needed.
    """
    paths.log.info("reading fertility rate...")
    tb_fertility_tot = clean_table_standard_xlsx(
        tb_main,
        "Total Fertility Rate (live births per woman)",
        "fertility_rate",
        format_table=False,
    )
    tb_fertility_age = read_from_csv("un_wpp_fertility.csv")
    tb_fertility_age = clean_table_standard_csv(
        tb_fertility_age, metrics_rename={"ASFR": "fertility_rate", "Births": "births"}
    )
    tb_fertility = (
        concat(
            [
                tb_fertility_age.drop(columns=["births"]),
                tb_fertility_tot,
            ],
            ignore_index=True,
        )
        .assign(sex="all")
        .format(COLUMNS_INDEX_FORMAT, short_name="fertility_rate")
    )
    del tb_fertility_tot

    # Births
    paths.log.info("reading births...")
    tb_births_tot = clean_table_standard_xlsx(
        tb_main,
        "Births (thousands)",
        "births",
        format_table=False,
    )
    tb_births = (
        concat(
            [
                tb_fertility_age.drop(columns=["fertility_rate"]),
                tb_births_tot,
            ],
            ignore_index=True,
        )
        .assign(sex="all")
        .format(COLUMNS_INDEX_FORMAT, short_name="births")
    )
    del tb_births_tot, tb_fertility_age

    return tb_fertility, tb_births


def make_tb_deaths() -> Table:
    """Make table with deaths.

    NOTE: no data available for scenarios other than Medium.
    """
    tb_deaths = read_from_csv("un_wpp_deaths_estimates.csv")
    tb_deaths_m = read_from_csv("un_wpp_deaths_medium.csv")
    tb_deaths = [
        clean_table_standard_csv(tb_deaths, ["DeathTotal", "DeathFemale", "DeathMale"]),
        clean_table_standard_csv(tb_deaths_m, ["DeathTotal", "DeathFemale", "DeathMale"]),
    ]
    tbs_ = []
    for tb in tb_deaths:
        # Unpivot
        tb = tb.melt(id_vars=COLUMNS_INDEX_CSV, var_name="sex", value_name="deaths")
        # Ensure correct format of column `sex`
        tb["sex"] = (
            tb["sex"]
            .map(
                {
                    "DeathTotal": "all",
                    "DeathFemale": "female",
                    "DeathMale": "male",
                }
            )
            .astype("category")
        )
        tbs_.append(tb)
        del tb
    tb_deaths = concat(tbs_, ignore_index=True).format(COLUMNS_INDEX_FORMAT, short_name="deaths")
    return tb_deaths


def read_from_xlsx(short_name: str) -> Table:
    """Read from XLSX. Clean and format table."""
    paths.log.info(f"reading {short_name}...")
    # Read snap
    snap = paths.load_snapshot(short_name)
    # Read tables
    tb_estimates = snap.read(sheet_name="Estimates", skiprows=16)
    tb_projections_medium = snap.read(sheet_name="Medium variant", skiprows=16)
    tb_projections_low = snap.read(sheet_name="Low variant", skiprows=16)
    tb_projections_high = snap.read(sheet_name="High variant", skiprows=16)
    # Merge tables
    tb = concat(
        [
            tb_estimates,
            tb_projections_medium,
            tb_projections_low,
            tb_projections_high,
        ],
        ignore_index=True,
    )
    # Drop spurious rows
    tb = tb.dropna(subset=["Year"])
    # Rename columns
    tb = tb.rename(columns=COLUMNS_RENAME_XLSX)
    # Keep relevant rows, drop location_type column
    tb = tb.loc[tb["Type"].isin(LOCATION_TYPES)]

    return tb


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


def clean_table_standard_csv(
    tb: Table, metrics: Optional[List[str]] = None, metrics_rename: Optional[Dict[str, str]] = None
) -> Table:
    if metrics_rename:
        tb = tb.rename(columns=metrics_rename)
        metrics = list(metrics_rename.values())
    if metrics is None:
        raise ValueError("No metrics to keep! Please specify argument `metrics` or `metrics_rename`")
    # Rename columns
    tb = tb.rename(columns=COLUMNS_RENAME_CSV)
    # Remove NaNs in location type
    tb = tb.dropna(subset=["LocTypeName"])
    tb = tb.loc[tb["LocTypeName"].isin(LOCATION_TYPES)]
    # Keep relevant columns
    tb = tb.loc[:, COLUMNS_INDEX_CSV + metrics]
    return tb


def combine_population(tbs: List[Table]) -> Table:
    tbs_ = []
    for tb in tbs:
        # Clean table
        tb = clean_table_standard_csv(tb, ["PopTotal", "PopMale", "PopFemale"])
        # Unpivot
        tb = tb.melt(id_vars=COLUMNS_INDEX_CSV, var_name="sex", value_name="population")
        # Ensure correct format of column `sex`
        tb["sex"] = (
            tb["sex"]
            .map(
                {
                    "PopTotal": "all",
                    "PopFemale": "female",
                    "PopMale": "male",
                }
            )
            .astype("category")
        )
        # Append table
        tbs_.append(tb)
        del tb
    tb = concat(tbs_, ignore_index=True)
    return tb


def clean_table_standard_xlsx(
    tb: Table,
    colname: str,
    new_name: str,
    sex: str = "all",
    age: str = "all",
    format_table: bool = True,
    log: bool = True,
) -> Table:
    """Process growth rate data.

    From snapshot table to ETL-ready-cleaned table.
    """
    if log:
        paths.log.info(f"processing {new_name} data...")

    # Rename columns
    tb = tb.rename(columns={colname: new_name})

    # Keep relevant columns
    tb = tb.loc[:, COLUMNS_INDEX_XLSX + [new_name]]

    # Set missing dimensions
    tb["sex"] = sex
    tb["age"] = age

    # Set year to int
    tb["year"] = tb["year"].astype(int)

    # Reduce size
    # tb = cast(Table, repack_frame(tb))

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    if format_table:
        tb = tb.format(COLUMNS_INDEX_FORMAT, short_name=new_name)

    return tb
