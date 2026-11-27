"""Create the multidim of animal welfare laws, from its adjacent config file.

As in the chart on chick culling (banning_of_chick_culling.py), the chick culling status embeds each country's
effective year (e.g. "Banned (2023)"), so this step expands the colors of each plain status defined in the config into
one entry per status-year value found in the data, labeled with its plain status.
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
    config = paths.load_config()

    # Load grapher dataset of chick culling laws, and read its main table.
    tb_chick = paths.load_dataset("chick_culling_laws").read("chick_culling_laws")

    #
    # Process data.
    #
    # Expand the colors of each plain status into one entry per status-year value found in the data.
    view = next(view for view in config["views"] if view["dimensions"]["metric"] == "chick_culling_status")
    color_scale = view["config"]["map"]["colorScale"]
    status_colors = color_scale["customCategoryColors"]
    plain_statuses = {value: re.sub(r" \(\d{4}\)$", "", value) for value in sorted(set(tb_chick["status"]))}
    error = "Status values in the data do not match the statuses with a color defined in the config."
    assert set(plain_statuses.values()) == set(status_colors), error
    color_scale["customCategoryColors"] = {value: status_colors[plain] for value, plain in plain_statuses.items()}
    color_scale["customCategoryLabels"] = {value: plain for value, plain in plain_statuses.items()}

    #
    # Save outputs.
    #
    c = paths.create_chart(config=config)
    c.save()
