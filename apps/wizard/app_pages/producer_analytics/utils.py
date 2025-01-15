from datetime import datetime

# Date when the new views metric started to be recorded.
MIN_DATE = datetime.strptime("2024-11-01", "%Y-%m-%d")
TODAY = datetime.today()
GRAPHERS_BASE_URL = "https://ourworldindata.org/grapher/"


def columns_producer(min_date, max_date):
    # Define columns to be shown.
    cols_prod = {
        "producer": {
            "headerName": "Producer",
            "headerTooltip": "Name of the producer. This is NOT the name of the dataset.",
        },
        "n_charts": {
            "headerName": "Charts",
            "headerTooltip": "Number of charts using data from a producer.",
        },
        "renders_custom": {
            "headerName": "Views in custom range",
            "headerTooltip": f"Number of renders between {min_date} and {max_date}.",
        },
        "renders_365d": {
            "headerName": "Views 365 days",
            "headerTooltip": "Number of renders in the last 365 days.",
        },
        "renders_30d": {
            "headerName": "Views 30 days",
            "headerTooltip": "Number of renders in the last 30 days.",
        },
    }
    return cols_prod
