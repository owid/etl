"""Load a garden dataset and create a grapher dataset.

Each country's effective year is embedded in its fur farming status (e.g. "Banned (2005)") so that the map tooltip
shows it. The colors and labels of the chart map each status-year value back to its plain status, so the legend shows
each status once.
"""

import re

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Get current year from this step's version.
CURRENT_YEAR = int(paths.version.split("-")[0])

# Fur farming statuses whose year is the year of a ban (or a phase-out).
# NOTE: The data has a single effective year per country. For a country with no fur farms but a ban on fur trading
# (Israel), that year is the year of the ban on fur trading, so it is not added to the fur farming status.
STATUSES_WITH_YEAR = [
    "Banned",
    "Banned but not yet in effect",
    "Partially banned",
    "Phased out due to stricter regulations",
]


def add_colors_and_labels_of_each_status_year(tb: Table, column: str) -> None:
    # Give each status-year value (e.g. "Banned (2005)") the color of its plain status, and label it as such.
    color_scale = tb[column].metadata.presentation.grapher_config["map"]["colorScale"]  # ty: ignore
    status_colors = color_scale["customCategoryColors"]
    plain_statuses = {value: re.sub(r" \(\d{4}\)$", "", value) for value in sorted(set(tb[column].dropna()))}
    error = "Status values in the data do not match the statuses with a color defined in the metadata."
    assert set(plain_statuses.values()) <= set(status_colors), error
    color_scale["customCategoryColors"] = {value: status_colors[plain] for value, plain in plain_statuses.items()}
    color_scale["customCategoryLabels"] = {value: plain for value, plain in plain_statuses.items()}


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("fur_laws")
    tb = ds_garden.read("fur_laws")

    #
    # Process data.
    #
    # There is a new category, "NO DATA".
    # For better visualization, we will replace them with nan.
    tb = tb.astype({"fur_farming_status": "str", "fur_trading_status": "str", "fur_farms_active": "str"})
    for column in ["fur_farming_status", "fur_trading_status", "fur_farms_active"]:
        tb.loc[tb[column] == "NO DATA", column] = None

    # Embed each country's effective year in its fur farming status, so it shows in the map tooltip.
    status = tb["fur_farming_status"]
    has_year = status.isin(STATUSES_WITH_YEAR) & tb["ban_effective_year"].notna()
    with_year = status + " (" + tb["ban_effective_year"].astype("Int64").astype("str") + ")"
    new_status = status.copy()
    new_status[has_year] = with_year[has_year]
    tb["fur_farming_status"] = new_status.copy_metadata(status)
    add_colors_and_labels_of_each_status_year(tb, column="fur_farming_status")

    # Format table conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
