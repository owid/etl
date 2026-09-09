"""Create the chick culling chart from its adjacent config file.

The grapher step embeds each country's effective year in the status value (e.g. "Banned (2023)") so the
map tooltip shows when the law takes (or took) effect. This step expands the plain-status colors defined
in the config into one entry per status-year value found in the data, relabeled back to its plain status,
so the legend shows one entry per status.
"""

import re

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # Load grapher dataset and read its main table.
    ds = paths.load_dataset("chick_culling_laws")
    tb = ds.read("chick_culling_laws")

    #
    # Process data.
    #
    # Expand the plain-status colors into one entry per status-year value found in the data.
    color_scale = config["views"][0]["config"]["map"]["colorScale"]
    status_colors = color_scale["customCategoryColors"]
    plain_statuses = {value: re.sub(r" \(\d{4}\)$", "", value) for value in sorted(set(tb["status"]))}
    error = "Status values in the data do not match the statuses with a color defined in the config."
    assert set(plain_statuses.values()) == set(status_colors), error
    color_scale["customCategoryColors"] = {value: status_colors[plain] for value, plain in plain_statuses.items()}
    color_scale["customCategoryLabels"] = {value: plain for value, plain in plain_statuses.items()}

    #
    # Save outputs.
    #
    # Create and save the chart collection.
    collection = paths.create_collection(config=config)
    collection.save()
