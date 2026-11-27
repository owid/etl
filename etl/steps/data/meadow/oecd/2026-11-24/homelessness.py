"""Load a snapshot and create a meadow dataset.

The snapshot is the Excel file of indicator HC3.1 "People experiencing homelessness and housing exclusion" of the OECD
Affordable Housing Database. Its sheets hold the data behind each figure, laid out for charting, so this step reads each
block by its cell range and checks the block's header before using it.
"""

from openpyxl.utils import column_index_from_string
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Blocks to read: sheet, first and last column, row of the header (1-based), expected header (None = empty cell), and new column names.
BLOCKS = {
    # Figure HC3.1.1, panel A: people experiencing homelessness per 10,000 people, point-in-time counts.
    "point_in_time": {
        "sheet": "HC3.1.1",
        "columns": ("L", "P"),
        "header_row": 2,
        "header": [
            None,
            "Living rough (ETHOS 1)",
            "Staying in temporary accommodation / shelter for homeless (ETHOS 2 and 3)",
            "No disaggregation (ETHOS 1,2,3)",
            "Total",
        ],
        "names": ["country", "point_in_time_1", "point_in_time_2_3", "point_in_time_1_2_3", "point_in_time_total"],
    },
    # Figure HC3.1.1, panel B: same, flow counts.
    "flow": {
        "sheet": "HC3.1.1",
        "columns": ("S", "W"),
        "header_row": 2,
        "header": [
            None,
            "Living rough (ETHOS 1)",
            "Staying in temporary accommodation / shelter for homeless (ETHOS 2 and 3)",
            "No disaggregation (ETHOS 1,2,3)",
            "Total",
        ],
        "names": ["country", "flow_1", "flow_2_3", "flow_1_2_3", "flow_total"],
    },
    # Figure HC3.1.2: share of women among people experiencing homelessness (as a fraction).
    "share_of_women": {
        "sheet": "HC3.1.2",
        "columns": ("R", "S"),
        "header_row": 2,
        "header": [None, "Share of Women"],
        "names": ["country", "share_of_women"],
    },
    # Table HC3.1.A1: headline estimate of each country and its breakdown by ETHOS Light category.
    "estimates": {
        "sheet": "Table_HC3.1.A1",
        "columns": ("A", "L"),
        "header_row": 3,
        "header": [
            "Country",
            "% of total population",
            "Headline estimate",
            "Breakdown of headline estimate",
            None,
            None,
            None,
            None,
            None,
            "Year",
            "PIT or FLOW",
            "Are children included?",
        ],
        "names": [
            "country",
            "share_of_population",
            "headline_estimate",
            "ethos_light_1",
            "ethos_light_2",
            "ethos_light_3",
            "ethos_light_4",
            "ethos_light_5",
            "ethos_light_6",
            "year",
            "count_type",
            "children_included",
        ],
    },
    # Table HC3.1.A2: reported number of people experiencing homelessness around 2018, around 2023, and in 2026 or latest year.
    "number": {
        "sheet": "Table_HC3.1.A2",
        "columns": ("A", "D"),
        "header_row": 4,
        "header": ["Country", "Around 2018", "Around 2023", "2026 or latest year"],
        "names": ["country", "around_2018", "around_2023", "latest"],
    },
}

# Strategies to address homelessness, transcribed from the PDF of indicator HC3.2 (key findings and Tables HC3.2.1
# and HC3.2.2), last updated 10 September 2026.
# NOTE: The PDF has no data file, so update these lists by hand.
# Countries with an active national homelessness strategy. The key findings list 23 countries and say there are 24;
# the 24th is the United States, whose national plan (All In) is listed in Table HC3.2.1.
NATIONAL_STRATEGY = [
    "Australia",
    "Belgium",
    "Canada",
    "Chile",
    "Colombia",
    "Costa Rica",
    "Denmark",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Ireland",
    "Israel",
    "Italy",
    "Japan",
    "Korea",
    "Netherlands",
    "Poland",
    "Portugal",
    "Romania",
    "Slovak Republic",
    "Spain",
    "Sweden",
    "United States",
]
# Countries without an active national homelessness strategy (regional or local strategies only, homelessness
# addressed in other national strategies, or no strategy at all).
NO_NATIONAL_STRATEGY = [
    "Austria",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czechia",
    "Estonia",
    "Hungary",
    "Iceland",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Mexico",
    "New Zealand",
    "Norway",
    "Slovenia",
    "South Africa",
    "Switzerland",
    "Türkiye",
    "United Kingdom",
]
# Countries with a Housing First or housing-led approach at the national level (Table HC3.2.2, first column).
HOUSING_FIRST_NATIONAL = [
    "Austria",
    "Belgium",
    "Canada",
    "Chile",
    "Czechia",
    "Denmark",
    "Finland",
    "France",
    "Greece",
    "Ireland",
    "Italy",
    "Korea",
    "Luxembourg",
    "Netherlands",
    "New Zealand",
    "Norway",
    "Poland",
    "Portugal",
    "Slovak Republic",
    "Spain",
    "Sweden",
]

# Row where the notes start in Table HC3.1.A2 (data rows end right before it).
NUMBER_END_MARKER = ".. Not available, | break in series"


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("affordable_housing_database_homelessness.xlsx")
    snap_strategies = paths.load_snapshot("affordable_housing_database_strategies.pdf")

    # Read every sheet once, without headers, to address cells by position.
    sheets = {
        sheet: snap.read_excel(sheet_name=sheet, header=None) for sheet in {block["sheet"] for block in BLOCKS.values()}
    }

    #
    # Process data.
    #
    tables = []
    for short_name, block in BLOCKS.items():
        tb = read_block(sheets[block["sheet"]], block=block)

        if short_name == "estimates":
            tb = clean_estimates(tb)
        elif short_name == "number":
            tb = clean_number(tb)

        value_columns = [col for col in tb.columns if col != "country"]
        if short_name in ["estimates", "number"]:
            # These tables mix numbers with notes ("Not included", "10,141 households"), so keep them as text; garden parses them.
            for col in value_columns:
                tb[col] = tb[col].astype("string")
        else:
            # The figure data must be fully numeric.
            for col in value_columns:
                tb[col] = pr.to_numeric(tb[col], errors="raise")

        tables.append(tb.format(["country"], short_name=short_name))

    # Strategies to address homelessness, from the HC3.2 document.
    tables.append(read_strategies(snap_strategies))

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_block(tb_sheet: Table, block: dict) -> Table:
    """Select a block of cells from a sheet, check its header, and keep the rows that have a country name."""
    first, last = (column_index_from_string(col) - 1 for col in block["columns"])
    header_index = block["header_row"] - 1

    # Check the header, so that a change in the layout of the sheet fails loudly.
    header = [
        None if (isinstance(v, float) or v is None) else str(v).strip()
        for v in tb_sheet.iloc[header_index, first : last + 1]
    ]
    assert header == block["header"], f"Unexpected header in sheet {block['sheet']}: {header}"

    # Select the data rows and rename columns.
    tb = tb_sheet.iloc[header_index + 1 :, first : last + 1].copy()
    tb.columns = block["names"]

    # Keep rows with a country name.
    tb = tb.dropna(subset=["country"]).reset_index(drop=True)
    tb["country"] = tb["country"].astype(str).str.strip()

    # Missing values are shown as "..".
    tb = tb.replace("..", None)

    return tb


def clean_estimates(tb: Table) -> Table:
    """Keep the first row of each country (each country spans three rows) and drop the notes under the table."""
    # Country rows are the ones with a year; the two rows below each country only list the types of accommodation.
    tb = tb.dropna(subset=["year"]).reset_index(drop=True)
    assert tb["country"].is_unique, "Duplicate countries in Table HC3.1.A1."

    return tb


def clean_number(tb: Table) -> Table:
    """Drop the notes under Table HC3.1.A2."""
    end = tb.index[tb["country"] == NUMBER_END_MARKER]
    assert len(end) == 1, f"Could not find the end of Table HC3.1.A2 ('{NUMBER_END_MARKER}')."
    tb = tb.loc[: end[0] - 1].reset_index(drop=True)
    assert tb["country"].is_unique, "Duplicate countries in Table HC3.1.A2."

    return tb


def read_strategies(snap: Snapshot) -> Table:
    """Build the table of strategies from the lists transcribed from the HC3.2 document."""
    assert not set(NATIONAL_STRATEGY) & set(NO_NATIONAL_STRATEGY), (
        "Countries listed with and without a national strategy."
    )
    assert set(HOUSING_FIRST_NATIONAL) <= set(NATIONAL_STRATEGY) | set(NO_NATIONAL_STRATEGY), (
        "Housing First countries missing from the national strategy lists."
    )
    countries = sorted(set(NATIONAL_STRATEGY) | set(NO_NATIONAL_STRATEGY))
    records = [
        {
            "country": country,
            "national_strategy": country in NATIONAL_STRATEGY,
            "housing_first_national": country in HOUSING_FIRST_NATIONAL,
        }
        for country in countries
    ]
    return snap.read_from_records(records).format(["country"], short_name="strategies")
