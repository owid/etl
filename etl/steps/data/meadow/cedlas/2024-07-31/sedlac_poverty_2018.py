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

POVERTY_SHEETS = {
    "poverty median": {"header": [6, 7, 8], "short_name": "poverty_median"},
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
POVERTY_COLUMNS_TO_DROP = ["B. Urban_Unnamed: 48_level_1_Unnamed: 48_level_2"]

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
    "Bahamas",
    "Belice",
    "Guyana",
    "Haiti",
    "Jamaica",
    "Suriname",
]

COUNTRY_TO_KEEP = ["Brazil"]

AGGREGATION_LEVELS = ["NATIONAL", "URBAN", "RURAL"]

SOURCE_TEXT = "Source: SEDLAC (CEDLAS and The World Bank) based on microdata from household surveys. "


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_poverty = paths.load_snapshot("sedlac_poverty_2018.xls")

    # Load all the sheets from the snapshots.
    poverty_tables = load_tables_from_snapshot(snap_poverty, POVERTY_SHEETS)

    #
    # Process data.
    poverty_tables = format_long_tables(poverty_tables, COUNTRIES, POVERTY_COLUMNS, POVERTY_COLUMNS_TO_DROP)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=poverty_tables,
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
        tb = snap.read(safe_types=False, sheet_name=sheet, header=sheets[sheet]["header"])
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
        t["country"] = t["country"].ffill()

        # Select only the countries I want to keep
        t = t[t["country"].isin(COUNTRY_TO_KEEP)].reset_index(drop=True)

        # Do the same for year
        t["year"] = t.loc[t["index"].astype(str).str[:4].str.isnumeric(), "index"]

        # Assert if null values in index are for for certain countries
        countries_with_null = []

        actual_countries_with_null = t[t["index"] == "nan"]["country"].unique().tolist()

        assert (
            actual_countries_with_null == countries_with_null
        ), f"Null values in index are not only for {countries_with_null}. In this case, we have {actual_countries_with_null}."

        # Assert that the null values are only two
        assert (
            len(t[t["index"] == "nan"]) == len(countries_with_null)
        ), f"There are more than {len(countries_with_null)} null values in index. There are {len(t[t['index'] == 'nan'])}."

        # Replace empty index values for country = Chile and Argentina and delete for Brazil and Mexico
        # t.loc[(t["country"] == "El Salvador") & (t["index"] == "nan"), "index"] = "Single series"

        # And for survey
        t["survey"] = t.loc[~(t["index"].isin(countries)) & ~(t["index"].astype(str).str[:4].str.isnumeric()), "index"]
        t["survey"] = t["survey"].ffill()

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
