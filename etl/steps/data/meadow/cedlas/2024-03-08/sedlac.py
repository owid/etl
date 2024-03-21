"""
Load a snapshot and create a meadow dataset.

Because the format of the snapshots are very particular, I prefer to to run several adjustments that I would normally do in garden.

"""

from typing import Dict, List

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

INCOMES_SHEETS = {"deciles_pci": {"header": [6, 7], "short_name": "deciles_pci"}}
POVERTY_SHEETS = {
    "poverty USD2.15": {"header": [6, 7, 8], "short_name": "poverty_215"},
    "poverty USD3.65": {"header": [6, 7, 8], "short_name": "poverty_365"},
    "poverty USD6.85": {"header": [6, 7, 8], "short_name": "poverty_685"},
    "poverty median": {"header": [6, 7, 8], "short_name": "poverty_median"},
}
INEQUALITY_DECILES_INDICES_SHEETS = {
    "deciles pci": {
        "header": 7,
        "short_name": "ineq_deciles_pci",
    },  # decile shares and income ratios - per capita income
    "indices pci": {"header": 5, "short_name": "ineq_indices_pci"},  # inequality indices - per capita income
    "deciles ei": {
        "header": 7,
        "short_name": "ineq_deciles_ei",
    },  # decile shares and income ratios - equivalized income
    "indices ei": {"header": 5, "short_name": "ineq_indices_ei"},  # inequality indices - equivalized income
    # "deciles lmi": {
    #     "header": 7,
    #     "short_name": "ineq_deciles_lmi",
    # },  # decile shares and income ratios - equivalized labor monetary income
    # "indices lmi": {
    #     "header": 5,
    #     "short_name": "ineq_indices_lmi",
    # },  # inequality indices - equivalized labor monetary income
    # "deciles ni": {
    #     "header": 7,
    #     "short_name": "ineq_deciles_ni",
    # },  # decile shares and income ratios - income or consumption variable for computing poverty with national lines
    # "indices ni": {
    #     "header": 5,
    #     "short_name": "ineq_indices_ni",
    # },  # inequality indices - income or consumption variable for computing poverty with national lines
}

INEQUALITY_GINI_SHEETS = {
    "gini1": {"header": [5, 6, 7], "short_name": "gini1"},  # Gini for different household income variables
    "gini2": {"header": [5, 6, 7, 8], "short_name": "gini2"},  # page 2
    # "gini3": {"header": [5, 6], "short_name": "gini3"},  # Gini including and excluding zero income
    # "polarization": {"header": [4, 5, 6]},  # Indices of bipolarization (EGR and Wolfson)
}

DECILES_INDICES_COLUMNS = {
    "1": "decile1_share",
    "2": "decile2_share",
    "3": "decile3_share",
    "4": "decile4_share",
    "5": "decile5_share",
    "6": "decile6_share",
    "7": "decile7_share",
    "8": "decile8_share",
    "9": "decile9_share",
    "10": "decile10_share",
    "10/1": "10_1_ratio",
    "90/10": "90_10_ratio",
    "95/5": "95_5_ratio",
    "95/50": "95_50_ratio",
    "50/5": "50_5_ratio",
    "95/80": "95_80_ratio",
    "Gini": "gini",
    "Theil": "theil",
    "CV": "cv",
    "A(.5)": "a_05",
    "A(1)": "a_1",
    "A(2)": "a_2",
    "E(0)": "e_0",
}

GINI_COLUMNS = {
    "Per capita_income_Unnamed: 1_level_2": "per_capita_income",
    "Equivalized_income_A": "equivalized_income_a",
    "Equivalized_income_B": "equivalized_income_b",
    "Equivalized_income_C": "equivalized_income_c",
    "Equivalized_income_D": "equivalized_income_d",
    "Equivalized_income_E": "equivalized_income_e",
    "Total_household _income": "total_household_income",
    "Equivalized_income A_Age 0-10": "equivalized_income_a_age_0_10",
    "Equivalized_income A_Age 20-30": "equivalized_income_a_age_20_30",
    "Equivalized_income A_Age 40-50": "equivalized_income_a_age_40_50",
    "Equivalized_income A_Age 60-70": "equivalized_income_a_age_60_70",
    "Per capita_income_Only urban_Unnamed: 1_level_3": "per_capita_income_only_urban",
    "Per capita_income_Only rural_Unnamed: 2_level_3": "per_capita_income_only_rural",
    "Equivalized_income_Only urban_Unnamed: 3_level_3": "equivalized_income_only_urban",
    "Equivalized_income_Only rural_Unnamed: 4_level_3": "equivalized_income_only_rural",
    "Per capita_income_Only labor _Unnamed: 5_level_3": "per_capita_income_only_labor",
    "Per capita_income_Only monetary _Unnamed: 6_level_3": "per_capita_income_only_monetary",
    "Per capita_income_Only labor _monetary": "per_capita_income_only_labor_monetary",
    "Per capita_income_Urban labor _monetary": "per_capita_income_urban_labor_monetary",
    "Per capita income_Without zeros": "per_capita_income_without_zeros",
    "Per capita income_With zeros": "per_capita_income_with_zeros",
    "Equivalized income_Without zeros": "equivalized_income_without_zeros",
    "Equivalized income_With zeros": "equivalized_income_with_zeros",
    '"National" income variable_Without zeros': "national_income_variable_without_zeros",
    '"National" income variable_With zeros': "national_income_variable_with_zeros",
}

POVERTY_COLUMNS = {
    "National_Headcount_FGT(0)": "national_headcount_ratio",
    "National_Poverty gap_FGT(1)": "national_poverty_gap_index",
    "National_Poverty gap_FGT(2)": "national_fgt2",
    "Urban_Headcount_FGT(0)": "urban_headcount_ratio",
    "Urban_Poverty gap_FGT(1)": "urban_poverty_gap_index",
    "Urban_Poverty gap_FGT(2)": "urban_fgt2",
    "Rural_Headcount_FGT(0)": "rural_headcount_ratio",
    "Rural_Poverty gap_FGT(1)": "rural_poverty_gap_index",
    "Rural_Poverty gap_FGT(2)": "rural_fgt2",
}

# In the 2024 version there is a column (AV) that has data, but it seems to be a mistake
POVERTY_COLUMNS_TO_DROP = ["B. Urban_Unnamed: 47_level_1_Unnamed: 47_level_2"]

COUNTRIES = [
    "Argentina",
    "Bolivia",
    "Brazil",
    "Chile",
    "Colombia",
    "Costa Rica",
    "Dominican Rep.",
    "Ecuador",
    "El Salvador",
    "Guatemala",
    "Honduras",
    "Mexico",
    "Nicaragua",
    "Panama",
    "Paraguay",
    "Peru",
    "Uruguay",
    "Venezuela",
]

AGGREGATION_LEVELS = ["NATIONAL", "URBAN", "RURAL"]

SOURCE_TEXT = "Source: SEDLAC (CEDLAS and The World Bank) based on microdata from household surveys. "


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_inequality = paths.load_snapshot("sedlac_inequality.xlsx")
    snap_poverty = paths.load_snapshot("sedlac_poverty.xlsx")

    # Load all the sheets from the snapshots.
    inequality_deciles_indices_tables = load_tables_from_snapshot(snap_inequality, INEQUALITY_DECILES_INDICES_SHEETS)
    inequality_gini_tables = load_tables_from_snapshot(snap_inequality, INEQUALITY_GINI_SHEETS)
    poverty_tables = load_tables_from_snapshot(snap_poverty, POVERTY_SHEETS)

    #
    # Process data.
    inequality_deciles_indices_tables = format_long_tables(
        inequality_deciles_indices_tables, COUNTRIES, DECILES_INDICES_COLUMNS
    )
    inequality_gini_tables = format_long_tables(inequality_gini_tables, COUNTRIES, GINI_COLUMNS)

    poverty_tables = format_long_tables(poverty_tables, COUNTRIES, POVERTY_COLUMNS, POVERTY_COLUMNS_TO_DROP)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=inequality_deciles_indices_tables + inequality_gini_tables + poverty_tables,
        check_variables_metadata=True,
        default_metadata=snap_poverty.metadata,
    )

    # Save changes in the
    # new meadow dataset.
    ds_meadow.save()


def load_tables_from_snapshot(snap: Snapshot, sheets: Dict) -> List[Table]:
    """Load all the sheets from the snapshot."""
    tables = []
    for sheet in sheets:
        tb = snap.read(sheet_name=sheet, header=sheets[sheet]["header"])
        tb.metadata.short_name = sheets[sheet]["short_name"]

        tables.append(tb)
    return tables


def format_long_tables(
    tb: List[Table], countries: List[str], columns: Dict, cols_to_drop: List[str] = []
) -> List[Table]:
    """Format inequality and poverty tables, which share a similar, long format."""

    tables = []
    for t in tb:
        # Flatten column names
        t.columns = ["_".join(map(str, x)) if isinstance(x, tuple) else str(x) for x in t.columns.values]

        # Rename first columm to "index", to facilitate the next steps
        t = t.rename(columns={t.columns[0]: "index"})

        # Strip spaces from first column
        t["index"] = t["index"].astype(str)
        t["index"] = t["index"].str.strip()

        # Create column country by selecting the rows in countries list
        t["country"] = t.loc[t["index"].isin(countries), "index"]
        t["country"] = t["country"].fillna(method="ffill")

        # Do the same for year
        t["year"] = t.loc[t["index"].astype(str).str[:4].str.isnumeric(), "index"]

        # Assert if null values in index are for for certain countries
        if t.m.short_name in ["ineq_deciles_ni", "ineq_indices_ni"]:
            countries_with_null = ["Argentina", "Chile", "Mexico"]
        elif t.m.short_name == "gini3":
            countries_with_null = ["Argentina", "Brazil", "Chile"]
        else:
            countries_with_null = ["Argentina", "Chile"]

        assert (
            t[t["index"] == "nan"]["country"].unique().tolist() == countries_with_null
        ), f"Null values in index are not only for {countries_with_null}."

        # Assert that the null values are only two
        assert len(t[t["index"] == "nan"]) == len(
            countries_with_null
        ), f"There are more than {len(countries_with_null)} null values in index."

        # Replace empty index values for country = Chile and Argentina and delete for Brazil and Mexico
        # This is done to identify survey spells that are blank in the original table.
        # In the case of Brazil and Mexico, it is just that there is an additional jump not representing a different spell.
        t.loc[(t["country"] == "Argentina") & (t["index"] == "nan"), "index"] = "EPH with changes"
        t.loc[(t["country"] == "Chile") & (t["index"] == "nan"), "index"] = "New adjustments and imputations"
        t = t[~((t["country"] == "Brazil") & (t["index"] == "nan"))]
        t = t[~((t["country"] == "Mexico") & (t["index"] == "nan"))]

        # And for survey
        t["survey"] = t.loc[~(t["index"].isin(countries)) & ~(t["index"].astype(str).str[:4].str.isnumeric()), "index"]
        t["survey"] = t["survey"].fillna(method="ffill")

        # Drop index column
        t = t.drop(columns="index")

        # Drop empty columns
        t = t.dropna(axis=1, how="all")

        # For poverty median, there is a column with two observations, so it is necessary to drop it
        for col in cols_to_drop:
            if col in t.columns:
                t = t.drop(columns=col)

        # Strip spaces from column names
        t.columns = t.columns.str.strip()

        # Assert if columns in table are contained in columns dict + country + year + survey
        assert set(
            t.columns
        ).issubset(
            set(list(columns.keys()) + ["country", "year", "survey"])
        ), f"Columns not found in the table: {set(t.columns) - set(list(columns.keys()) + ['country', 'year', 'survey'])}."

        # Rename columns with columns dict
        t = t.rename(columns=columns)

        # Drop rows with year = null
        t = t.dropna(subset=["year"]).reset_index(drop=True)

        # Factorize survey, but restarting the count for each country

        for country in list(t["country"].unique()):
            t.loc[t["country"] == country, "survey_number"] = (
                t.loc[t["country"] == country, "survey"].factorize()[0] + 1
            )

        # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
        t = t.underscore().set_index(["country", "year", "survey_number", "survey"], verify_integrity=True).sort_index()

        tables.append(t)

    return tables
