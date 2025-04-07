"""
This script takes three input files and combines them into a big explorer spreadsheet for the global food explorer.
The files are:
- (1) global-food-explorer.template.tsv: A template file that contains the header and footer of the spreadsheet, together with some placeholders.
- (2) foods.csv: a list of foods and their singular and plural names.
- (3) views-per-food.csv: a list of all available views for every food, including subtitle etc. The title can contain placeholders which are then filled out with the food name.
This is all further complicated by the fact that we have different tag for food products, which enable views with different columns, units and subtitles.
We take the cartesian product between (2) and (3) - according to the tag -, sprinkle some magic dust to make the titles work, and then place that massive table into the template (1).

NOTE: This script has been migrated and adapted from the original one in owid-content/scripts.

"""

import textwrap
from pathlib import Path
from string import Template

import click
import numpy as np
import pandas as pd
from git import Repo
from owid.catalog import find
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.paths import BASE_DIR

# Initialize log.
log = get_logger()

CURRENT_DIR = Path(__file__).parent


@click.command()
@click.option(
    "--ready-to-merge",
    default=False,
    type=bool,
    help="Use production catalog paths, if the work is ready to be merged to master. Otherwise, use staging links.",
)
def run(ready_to_merge: bool = False):
    if ready_to_merge:
        DATA_FILES_URL = "https://catalog.ourworldindata.org/explorers/faostat/latest/food_explorer/"
    else:
        repo = Repo(BASE_DIR)
        current_branch = repo.active_branch.name
        DATA_FILES_URL = f"http://staging-site-{current_branch}:8881/explorers/faostat/latest/food_explorer/"

    def table_def(food):
        return f"table\t{DATA_FILES_URL}{food}.csv\t{food}"

    with open(CURRENT_DIR / "global-food-explorer.template.tsv", "r") as templateFile:
        template = Template(templateFile.read())
    foods_df = pd.read_csv(CURRENT_DIR / "foods.csv", index_col="slug", dtype=str)
    views_df = pd.read_csv(CURRENT_DIR / "views-per-food.csv", dtype=str)

    # convert space-separated list of tags to an actual list, such that we can explode and merge by tag
    views_df["_tags"] = views_df["_tags"].apply(lambda x: x.split(" "))
    views_df = views_df.explode("_tags").rename(columns={"_tags": "_tag"})
    views_df["_tag"] = views_df["_tag"].str.strip()

    foods_rename = {
        "dropdown": "Food Dropdown",
        "slug": "tableSlug",
        "_tags": "_tags",
        "note": "food__note",
    }

    foods = foods_df.reset_index()[foods_rename.keys()].rename(columns=foods_rename)
    foods["_tags"] = foods["_tags"].apply(lambda x: x.split(" "))
    foods = foods.explode("_tags").rename(columns={"_tags": "_tag"})

    food_tags = set(foods["_tag"])
    view_tags = set(views_df["_tag"])

    symmetric_diff = food_tags.symmetric_difference(view_tags)
    if len(symmetric_diff) > 0:
        log.error(
            f"Found {len(symmetric_diff)} tags that only appear in one of the input files: {', '.join(symmetric_diff)}"
        )

    def substitute_title(row):
        # The title can include placeholders like ${food_singular}, which will be replaced with the actual food name here.
        food_slug = row["tableSlug"]
        food_names = foods_df.loc[food_slug]
        for key in ["title", "subtitle"]:
            if isinstance(row[key], str):
                template = Template(row[key])
                row[key] = template.substitute(
                    food_singular=food_names["singular"],
                    food_singular_lower=food_names["singular"].lower(),
                    food_plural=food_names["plural"],
                    food_plural_lower=food_names["plural"].lower(),
                )
        return row

    # merge on column: _tag
    graphers = views_df.merge(foods).apply(substitute_title, axis=1)
    graphers = graphers.drop(columns="_tag").sort_values(by="Food Dropdown", kind="stable")  # type: ignore
    # drop duplicates introduced by the tag merge
    graphers = graphers.drop_duplicates()

    # join note (footnote) between food and view tables
    graphers["note"] = graphers["food__note"].str.cat(graphers["note"], sep="\\n", na_rep="")
    graphers["note"] = graphers["note"].apply(lambda x: x.strip("\\n"))
    graphers = graphers.drop(columns="food__note")

    # We want to have a consistent column order for easier interpretation of the output.
    # However, if there are any columns added to views-per-food.csv at any point in the future,
    # we want to make sure these are also present in the output.
    # Therefore, we define the column order and also add any remaining columns to the output.
    col_order = [
        "title",
        "Food Dropdown",
        "Metric Dropdown",
        "Unit Radio",
        "Per Capita Checkbox",
        "subtitle",
        "type",
        "ySlugs",
        "tableSlug",
        "note",
        "yScaleToggle",
    ]
    remaining_cols = pd.Index(graphers.columns).difference(pd.Index(col_order)).tolist()
    graphers = graphers.reindex(columns=col_order + remaining_cols)

    if len(remaining_cols) > 0:
        log.warning("ℹ️ Found the following columns not present in col_order:", remaining_cols)

    # Define the default view of the explorer.
    default_view = (
        '`Food Dropdown` == "Maize (corn)" and `Metric Dropdown` == "Production" and `Per Capita Checkbox` == "false"'
    )

    # Mark the default view with defaultView=true. This is always the last column.
    if default_view is not None:
        default_view_mask = graphers.eval(default_view)
        default_view_count = len(graphers[default_view_mask])
        if default_view_count != 1:
            log.error(
                f"Default view ({default_view}) should match exactly one view, but matches {default_view_count} views: {graphers[default_view_mask]}"
            )
        graphers["defaultView"] = np.where(default_view_mask, "true", None)  # type: ignore

    # TODO: Drop the _tag column?
    graphers_tsv = graphers.to_csv(sep="\t", index=False)
    graphers_tsv_indented = textwrap.indent(graphers_tsv, "\t")  # type: ignore

    table_defs = "\n".join([table_def(food) for food in foods_df.index])
    food_slugs = "\t".join(foods_df.index)

    # Get the latest publication year of the data used in the food explorer.
    # To do that, get the most recent publication year of the fbsc and qcl datasets (the two datasets used by the explorer).
    YEAR = find("faostat_qcl|fbsc").sort_values("version", ascending=False).iloc[0]["version"].split("-")[0]

    # Generate the content of the explorer.tsv file.
    warning_message = "# DO NOT EDIT THIS FILE BY HAND. It is automatically generated using a set of input files. Any changes made directly to it will be overwritten.\n\n"
    explorer_content = warning_message + template.substitute(
        food_slugs=food_slugs,
        graphers_tsv=graphers_tsv_indented,
        table_defs=table_defs,
        year=YEAR,
    )

    # DEBUGGING: Write the explorer.tsv file in a local file, to visually inspect it before pushing to DB.
    # Write explorer.tsv file in the current folder (temporarily).
    # with open("./global-food.explorer.tsv", "w", newline="\n") as f:
    #     f.write(explorer_content)

    # Upsert config via Admin API
    admin_api = AdminAPI(OWID_ENV)
    admin_api.put_explorer_config("global-food", explorer_content)


if __name__ == "__main__":
    run()
