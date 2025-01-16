from datetime import datetime

import pandas as pd
from st_aggrid.grid_options_builder import GridOptionsBuilder

# Date when the new views metric started to be recorded.
MIN_DATE = datetime.strptime("2024-11-01", "%Y-%m-%d")
TODAY = datetime.today()
GRAPHERS_BASE_URL = "https://ourworldindata.org/grapher/"
# List of auxiliary steps to be (optionally) excluded from the DAG.
# It may be convenient to ignore these steps because the analytics are heavily affected by a few producers (e.g. those that are involved in the population and income groups datasets).
AUXILIARY_STEPS = [
    "data://garden/demography/.*/population",
    # Primary energy consumption is loaded by GCB.
    "data://garden/energy/.*/primary_energy_consumption",
    "data://garden/ggdc/.*/maddison_project_database",
    "data://garden/wb/.*/income_groups",
]


def columns_producer(min_date, max_date):
    # Define columns to be shown.
    cols_prod = {
        "producer": {
            "headerName": "Producer",
            "headerTooltip": "Name of the producer. This is NOT the name of the dataset.",
            "filter": "agTextColumnFilter",
        },
        "n_charts": {
            "headerName": "Charts",
            "headerTooltip": "Number of charts using data from a producer.",
        },
        "views_custom": {
            "headerName": "Views in custom range",
            "headerTooltip": f"Number of renders between {min_date} and {max_date}.",
        },
        "views_365d": {
            "headerName": "Views 365 days",
            "headerTooltip": "Number of renders in the last 365 days.",
        },
        "views_30d": {
            "headerName": "Views 30 days",
            "headerTooltip": "Number of renders in the last 30 days.",
        },
    }
    return cols_prod


def make_grid(df: pd.DataFrame, column_config, selection: bool = False):
    gb = GridOptionsBuilder.from_dataframe(
        df,
    )

    gb.configure_grid_options(
        # domLayout="autoHeight",
        enableCellTextSelection=True,
        suppressSizeToFit=False,  # Allows dynamic resizing to fit.
    )

    gb.configure_default_column(
        editable=False,
        groupable=True,
        sortable=True,
        filterable=True,
        resizable=True,
        autoSizeColumns=True,  # Ensures all columns can auto-size.
    )

    # OPTIONAL: Enable selection.
    if selection:
        gb.configure_selection(
            selection_mode="multiple",
            use_checkbox=True,
            rowMultiSelectWithClick=True,
            suppressRowDeselection=False,
            groupSelectsChildren=True,
            groupSelectsFiltered=True,
        )

    # Configure pagination with dynamic page size.
    gb.configure_pagination(
        # paginationAutoPageSize=False,
        # paginationPageSize=20,
    )

    # Apply column configurations directly from the dictionary.
    for column, config in column_config.items():
        gb.configure_column(column, **config)

    # Generate grid options
    grid_options = gb.build()

    return grid_options
