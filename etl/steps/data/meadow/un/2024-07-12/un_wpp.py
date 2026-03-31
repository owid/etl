"""NOTE: This meadow step is relatively complete. Why? Because the snapshot steps are quite big, and we want to extract the esential data for next steps. Otherwise, we would be making Garden steps quite slow.

What do we do here?

- Read the XLSX files
- Keep relevant columns
- Format the tables to have them in long format
- Set indices and verify integrity
"""

from typing import Dict, List, Optional, Tuple

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.tables import concat

from etl.helpers import PathFinder

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
COLUMNS_INDEX_FORMAT_MONTH = [
    "country",
    "year",
    "variant",
    "sex",
    "age",
    "month",
]
SCENARIOS = ["Medium", "Low", "High", "Constant fertility", "Estimates"]
LOCATION_TYPES_CSV = [
    "Country/Area",
    "Geographic region",
    "Income group",
    "Development group",
    "World",
]
LOCATION_TYPES_XLSX = [
    "Country/Area",
    "Region",
    "Income Group",
    "Development Group",
    "World",
]

# Interim update: mapping from snapshot short_name to the CSV filename inside the interim ZIP.
# Only files that are actually used in the pipeline are listed here.
INTERIM_CSV_MAP = {
    "un_wpp_population_estimates.csv": "WPP2024_PopulationBySingleAgeSex_Medium_Update.csv",
    "un_wpp_population_medium.csv": "WPP2024_PopulationBySingleAgeSex_Medium_Update.csv",
    "un_wpp_population_jan_estimates.csv": "WPP2024_Population1JanuaryBySingleAgeSex_Medium_Update.csv",
    "un_wpp_population_jan_medium.csv": "WPP2024_Population1JanuaryBySingleAgeSex_Medium_Update.csv",
    "un_wpp_fertility.csv": "WPP2024_Fertility_by_Age5_Medium_Update.csv",
    "un_wpp_fertility_single_age.csv": "WPP2024_Fertility_by_Age1_Medium_Update.csv",
    "un_wpp_deaths_estimates.csv": "WPP2024_DeathsBySingleAgeSex_Medium_Update.csv",
    "un_wpp_deaths_medium.csv": "WPP2024_DeathsBySingleAgeSex_Medium_Update.csv",
}

# Mapping from interim demographic indicators CSV columns to the long XLSX column names.
INTERIM_DEMO_INDICATORS_RENAME = {
    "PopDensity": "Population Density, as of 1 July (persons per square km)",
    "PopGrowthRate": "Population Growth Rate (percentage)",
    "NatChangeRT": "Rate of Natural Change (per 1,000 population)",
    "NetMigrations": "Net Number of Migrants (thousands)",
    "CNMR": "Net Migration Rate (per 1,000 population)",
    "CDR": "Crude Death Rate (deaths per 1,000 population)",
    "CBR": "Crude Birth Rate (births per 1,000 population)",
    "MedianAgePop": "Median Age, as of 1 July (years)",
    "MAC": "Mean Age Childbearing (years)",
    "TFR": "Total Fertility Rate (live births per woman)",
    "Births": "Births (thousands)",
    "Q5": "Under-Five Mortality (deaths under age 5 per 1,000 live births)",
    "IMR": "Infant Mortality Rate (infant deaths per 1,000 live births)",
    "LEx": "Life Expectancy at Birth, both sexes (years)",
    "LExMale": "Male Life Expectancy at Birth (years)",
    "LExFemale": "Female Life Expectancy at Birth (years)",
    "LE15": "Life Expectancy at Age 15, both sexes (years)",
    "LE15Male": "Male Life Expectancy at Age 15 (years)",
    "LE15Female": "Female Life Expectancy at Age 15 (years)",
    "LE65": "Life Expectancy at Age 65, both sexes (years)",
    "LE65Male": "Male Life Expectancy at Age 65 (years)",
    "LE65Female": "Female Life Expectancy at Age 65 (years)",
    "LE80": "Life Expectancy at Age 80, both sexes (years)",
    "LE80Male": "Male Life Expectancy at Age 80 (years)",
    "LE80Female": "Female Life Expectancy at Age 80 (years)",
}

# Interim snapshot short names (add more here for future interim updates).
INTERIM_SNAPSHOTS = [
    "un_wpp_interim_20260119_togo.zip",
]

YEAR_ESTIMATES_END = 2023  # Year up to which the estimates go (inclusive). Projections start in 2024.


def _load_interim_csvs() -> Dict[str, Table]:
    """Load all interim update ZIPs and return a dict mapping CSV filename to Table.

    Verifies that all loaded CSVs only contain the 'Medium' variant.
    """
    result: Dict[str, Table] = {}
    for snap_name in INTERIM_SNAPSHOTS:
        snap = paths.load_snapshot(snap_name)
        with snap.extracted() as archive:
            for name in archive.glob("*.csv"):
                tb = archive.read(name)
                # Sanity check: interim data should only contain the Medium variant
                if "Variant" in tb.columns:
                    variants = set(tb["Variant"].unique())
                    assert variants == {"Medium"}, (
                        f"Interim CSV '{name}' from '{snap_name}' contains unexpected variants: "
                        f"{variants - {'Medium'}}. Expected only 'Medium'."
                    )
                result[name] = tb
    return result


def _apply_interim_csv(tb: Table, interim_csvs: Dict[str, Table], short_name: str) -> Table:
    """Replace rows for interim-updated countries in a CSV-based table.

    Drops rows from `tb` that match interim country+variant, then appends the interim rows.
    Only Togo / Medium variant interim data is expected; year range is clipped to match `tb`.
    """
    interim_filename = INTERIM_CSV_MAP.get(short_name)
    if interim_filename is None or interim_filename not in interim_csvs:
        return tb

    tb_interim = interim_csvs[interim_filename].copy()

    # Keep only Togo + Medium variant from interim data
    tb_interim = tb_interim.loc[(tb_interim["Location"] == "Togo") & (tb_interim["Variant"] == "Medium")]
    if tb_interim.empty:
        return tb

    # Clip interim years to match the year range present in the original table
    year_min = int(tb["Time"].min())
    year_max = int(tb["Time"].max())
    tb_interim = tb_interim.loc[(tb_interim["Time"] >= year_min) & (tb_interim["Time"] <= year_max)]
    if tb_interim.empty:
        return tb

    # Drop matching rows from the original table (both Medium and Estimates for Togo)
    mask = (tb["Location"] == "Togo") & (tb["Variant"].isin({"Medium", "Estimates"}))
    tb = tb.loc[~mask]

    # Append interim data
    tb = concat([tb, tb_interim], ignore_index=True)
    return tb


def _apply_interim_xlsx(tb: Table, interim_csvs: Dict[str, Table]) -> Table:
    """Replace rows for interim-updated countries in the XLSX-based demographic indicators table.

    The interim CSV uses short column names (TFR, LEx, etc.) while the XLSX uses long names.
    We rename the interim columns, drop matching rows from the original, and append.
    Only Togo / Medium variant interim data is expected; year range is clipped to match `tb`.
    """
    interim_filename = "WPP2024_Demographic_Indicators_Medium_Update.csv"
    if interim_filename not in interim_csvs:
        return tb

    tb_interim = interim_csvs[interim_filename].copy()

    # Keep only Togo + Medium variant from interim data
    tb_interim = tb_interim.loc[(tb_interim["Location"] == "Togo") & (tb_interim["Variant"] == "Medium")]
    if tb_interim.empty:
        return tb

    # Get interim countries (use the already-renamed "country" column if present, else "Location")
    country_col = "country" if "country" in tb.columns else "Region, subregion, country or area *"
    variant_col = "variant" if "variant" in tb.columns else "Variant"
    year_col = "year" if "year" in tb.columns else "Year"

    # Clip interim years to match the year range present in the original table
    year_min = int(tb[year_col].min())
    year_max = int(tb[year_col].max())
    tb_interim = tb_interim.loc[(tb_interim["Time"] >= year_min) & (tb_interim["Time"] <= year_max)]
    if tb_interim.empty:
        return tb

    # Rename interim columns to match XLSX column names
    rename_map = {
        "Location": country_col,
        "Time": year_col,
        "Variant": variant_col,
        "LocTypeName": "Type" if "Type" in tb.columns else "LocTypeName",
    }
    rename_map.update(INTERIM_DEMO_INDICATORS_RENAME)
    tb_interim = tb_interim.rename(columns=rename_map)

    # Keep only columns that exist in the original table
    common_cols = [c for c in tb_interim.columns if c in tb.columns]
    tb_interim = tb_interim[common_cols]

    # Ensure year types match
    tb_interim[year_col] = tb_interim[year_col].astype(tb[year_col].dtype)

    # Drop matching rows from the original table (both Medium and Estimates for Togo)
    mask = (tb[country_col] == "Togo") & (tb[variant_col].isin({"Medium", "Estimates"}))
    tb = tb.loc[~mask]

    tb = concat([tb, tb_interim], ignore_index=True)
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    paths.log.info("reading snapshots...")

    # Load interim update CSVs
    paths.log.info("loading interim update data...")
    interim_csvs = _load_interim_csvs()

    # Main file: Demography indicators
    paths.log.info("reading main file: demography indicators...")
    tb_main = read_from_xlsx("un_wpp_demographic_indicators.xlsx", interim_csvs=interim_csvs)
    # Process data.
    # Population density add month column
    tb_population_density = clean_table_standard_xlsx(
        tb_main, "Population Density, as of 1 July (persons per square km)", "population_density"
    )
    tb_population_density = tb_population_density.reset_index()
    tb_population_density["month"] = "July"
    tb_population_density = tb_population_density.format(COLUMNS_INDEX_FORMAT_MONTH)

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
    tb_macb = clean_table_standard_xlsx(tb_main, "Mean Age Childbearing (years)", "mean_age_childbearing")
    tb_mortality = make_tb_mortality(tb_main)
    tb_le = make_tb_life_expectancy(tb_main)

    # # Population
    paths.log.info("reading population...")
    tb_population = make_tb_population(interim_csvs)

    # # Fertility rate
    tb_fertility, tb_births = make_tb_fertility_births(tb_main, interim_csvs)
    tb_fertility_births_single = make_tb_fertility_births_single_age(interim_csvs)

    # Deaths
    tb_deaths = make_tb_deaths(interim_csvs)

    #
    # Save outputs.
    #
    tables = [
        tb_population,
        tb_population_density,
        tb_growth_rate,
        tb_nat_change,
        tb_fertility,
        tb_fertility_births_single,
        tb_migration,
        tb_migration_rate,
        tb_deaths,
        tb_death_rate,
        tb_births,
        tb_birth_rate,
        tb_median_age,
        tb_le,
        tb_mortality,
        tb_macb,
        # tb_population_doubling,
    ]
    # Convert low-cardinality string columns to categoricals for all tables.
    # This drastically reduces feather file size and read time (e.g. population: 406 MB -> 153 MB on disk).
    tables = [_categorize_index(tb) for tb in tables]

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, repack=False)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def make_tb_population(interim_csvs: Dict[str, Table]) -> Table:
    """Make population table."""
    # Get estimates (1950-2023) data
    tb_population = read_from_csv("un_wpp_population_estimates.csv", interim_csvs)
    tb_population["month"] = "July"
    tb_population = tb_population.loc[tb_population["Time"] <= YEAR_ESTIMATES_END]
    tb_population_jan = read_from_csv("un_wpp_population_jan_estimates.csv", interim_csvs)
    tb_population_jan["month"] = "January"
    tb_population_jan = tb_population_jan.loc[tb_population_jan["Time"] <= YEAR_ESTIMATES_END]

    # Get projections (2024-2100) data
    tb_population_jan_medium = read_from_csv("un_wpp_population_jan_medium.csv", interim_csvs)
    tb_population_jan_medium["month"] = "January"
    tb_population_jan_medium = tb_population_jan_medium.loc[tb_population_jan_medium["Time"] > YEAR_ESTIMATES_END]
    tb_population_l = read_from_csv("un_wpp_population_low.csv")
    tb_population_l["month"] = "July"
    tb_population_m = read_from_csv("un_wpp_population_medium.csv", interim_csvs)
    tb_population_m["month"] = "July"
    tb_population_m = tb_population_m.loc[tb_population_m["Time"] > YEAR_ESTIMATES_END]
    tb_population_h = read_from_csv("un_wpp_population_high.csv")
    tb_population_h["month"] = "July"
    tb_population_c = read_from_csv("un_wpp_population_constant_fertility.csv")
    tb_population_c["month"] = "July"
    tb_population = combine_population(
        [
            tb_population,
            tb_population_jan,
            tb_population_jan_medium,
            tb_population_l,
            tb_population_m,
            tb_population_h,
            tb_population_c,
        ]
    )
    del tb_population_l, tb_population_m, tb_population_h, tb_population_c, tb_population_jan, tb_population_jan_medium
    tb_population = tb_population.format(COLUMNS_INDEX_FORMAT_MONTH, short_name="population")
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


def make_tb_fertility_births_single_age(interim_csvs: Dict[str, Table]) -> Table:
    # Read
    tb = read_from_csv("un_wpp_fertility_single_age.csv", interim_csvs)
    # Clean
    tb = clean_table_standard_csv(tb, metrics_rename={"ASFR": "fertility_rate", "Births": "births"})
    # Add missing dimension
    tb = tb.assign(sex="all")
    # Format
    tb = tb.format(COLUMNS_INDEX_FORMAT, short_name="fertility_births_single")
    return tb


def make_tb_fertility_births(tb_main: Table, interim_csvs: Dict[str, Table]) -> Tuple[Table, Table]:
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
    tb_fertility_age = read_from_csv("un_wpp_fertility.csv", interim_csvs)
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


def make_tb_deaths(interim_csvs: Dict[str, Table]) -> Table:
    """Make table with deaths.

    NOTE: no data available for scenarios other than Medium.
    """
    tb_deaths = read_from_csv("un_wpp_deaths_estimates.csv", interim_csvs)
    tb_deaths_m = read_from_csv("un_wpp_deaths_medium.csv", interim_csvs)
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


def read_from_xlsx(short_name: str, interim_csvs: Optional[Dict[str, Table]] = None) -> Table:
    """Read from XLSX. Clean and format table."""
    paths.log.info(f"reading {short_name}...")
    # Read snap
    snap = paths.load_snapshot(short_name)
    # Read tables
    tb_estimates = snap.read(safe_types=False, sheet_name="Estimates", skiprows=16)
    tb_projections_medium = snap.read(safe_types=False, sheet_name="Medium variant", skiprows=16)
    tb_projections_low = snap.read(safe_types=False, sheet_name="Low variant", skiprows=16)
    tb_projections_high = snap.read(safe_types=False, sheet_name="High variant", skiprows=16)
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
    tb = tb.loc[tb["Type"].isin(LOCATION_TYPES_XLSX)]

    # Apply interim update for demographic indicators
    if interim_csvs:
        tb = _apply_interim_xlsx(tb, interim_csvs)

    return tb


def read_from_csv(short_name: str, interim_csvs: Optional[Dict[str, Table]] = None) -> Table:
    paths.log.info(f"reading {short_name}...")
    # Read snap
    tb = paths.read_snap_table(short_name, compression="gzip")
    # Drop unused columns
    tb = tb.drop(columns=["Notes"])
    # Filter relevant variants
    tb = tb.loc[tb["Variant"].isin(SCENARIOS)]
    # Apply interim update (replace data for updated countries)
    if interim_csvs:
        tb = _apply_interim_csv(tb, interim_csvs, short_name)
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
    tb = tb.loc[tb["LocTypeName"].isin(LOCATION_TYPES_CSV)]
    # Keep relevant columns
    if "month" in tb.columns:
        tb = tb.loc[:, COLUMNS_INDEX_CSV + ["month"] + metrics]
    else:
        tb = tb.loc[:, COLUMNS_INDEX_CSV + metrics]
    return tb


def combine_population(tbs: List[Table]) -> Table:
    tbs_ = []
    for tb in tbs:
        # Clean table
        tb = clean_table_standard_csv(tb, ["PopTotal", "PopMale", "PopFemale"])
        # Unpivot
        tb = tb.melt(id_vars=COLUMNS_INDEX_CSV + ["month"], var_name="sex", value_name="population")
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


def _categorize_index(tb: Table) -> Table:
    """Convert string index levels to categoricals to reduce feather file size and read time."""
    _CATEGORICAL_COLS = {"country", "variant", "sex", "age", "month"}
    idx_names = list(tb.index.names)
    cols_to_convert = [n for n in idx_names if n in _CATEGORICAL_COLS]
    if not cols_to_convert:
        return tb
    tb = tb.reset_index()
    for col in cols_to_convert:
        tb[col] = tb[col].astype("category")
    tb = tb.format(idx_names, short_name=tb.metadata.short_name)
    return tb
