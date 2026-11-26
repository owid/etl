"""Load a garden dataset and create a grapher dataset.

Each country's effective year is embedded in the status value (e.g. "Banned (2015)") so that the map tooltip shows it.
The colors and labels of the chart map each status-year value back to its plain status, so the legend shows each status
once.
"""

import re

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Current year.
CURRENT_YEAR = int(paths.version.split("-")[0])


def add_colors_and_labels_of_each_status_year(tb: Table) -> None:
    # Give each status-year value (e.g. "Banned (2015)") the color of its plain status, and label it as such.
    color_scale = tb["status"].metadata.presentation.grapher_config["map"]["colorScale"]  # ty: ignore
    status_colors = color_scale["customCategoryColors"]
    plain_statuses = {value: re.sub(r" \(\d{4}\)$", "", value) for value in sorted(set(tb["status"]))}
    error = "Status values in the data do not match the statuses with a color defined in the metadata."
    assert set(plain_statuses.values()) <= set(status_colors), error
    color_scale["customCategoryColors"] = {value: status_colors[plain] for value, plain in plain_statuses.items()}
    color_scale["customCategoryLabels"] = {value: plain for value, plain in plain_statuses.items()}


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset, and read its main table.
    ds_garden = paths.load_dataset("bullfighting_laws")
    tb = ds_garden.read("bullfighting_laws")

    #
    # Process data.
    #
    # Embed each country's effective year (where known) in the status value so it shows in the map tooltip.
    status = tb["status"]
    has_year = tb["year_effective"].notna()
    with_year = status.astype("str") + " (" + tb["year_effective"].astype("Int64").astype("str") + ")"
    new_status = status.astype("str")
    new_status[has_year] = with_year[has_year]
    tb["status"] = new_status.copy_metadata(status)
    add_colors_and_labels_of_each_status_year(tb)

    # Keep only the status, for the current year (there should be no timeline in the chart).
    tb = tb[["country", "status"]]
    tb["year"] = CURRENT_YEAR

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
