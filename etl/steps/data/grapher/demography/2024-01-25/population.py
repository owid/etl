"""Load a garden dataset and create a grapher dataset.

This dataset is a work in progress. We have layed down the foundations for the step, but are waiting until new data is available (both from Maddison and from 2024 WPP).
"""

import re
from copy import deepcopy
from typing import Any, List

import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Each variable has data from 10,000 BCE until 2100. We create two new versions for each variables:
# - Historical: data from 10,000 BCE until YEAR_THRESHOLD - 1.
# - Projection: data from YEAR_THRESHOLD until 2100.
YEAR_THRESHOLD = 2022


def run(dest_dir: str) -> None:
    #
    # Load inputs
    #
    # 2024-01-05 version: HYDE + GAPMINDER + WPP
    # Load garden dataset.
    ds_garden = paths.load_dataset("population", version="2024-01-05")
    # Read table from garden dataset.
    tb_1 = ds_garden["population_original"].update_metadata(short_name="population")
    # Set origins on `source`
    tb_1.source.m.origins = tb_1.population.m.origins

    # 2024-01-25 version: HYDE + MADDISON + WPP
    # Load garden dataset.
    ds_garden = paths.load_dataset("population", version="2024-01-25")
    # Read table from garden dataset.
    tb_2 = ds_garden["population_original"].update_metadata(short_name="population_exp")
    # Set origins on `source`
    tb_2.source.m.origins = tb_2.population.m.origins

    # Fix column names
    tb_2 = tb_2.rename(
        columns={
            "population": "population_exp",
            "source": "source_exp",
        }
    )
    tb_2["population_exp"].metadata.title = "Population (experimental)"
    tb_2["source_exp"].metadata.title = "Source (experimental)"

    #
    # Process data.
    #
    tb_1 = process(tb_1)

    # Tables
    tables = [
        tb_1[["population", "source"]],
        tb_2[["population_exp", "source_exp"]],
    ]

    #
    # Save outputs.
    #

    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def process(table: Table) -> Table:
    """Adapt table for Grapher.

    - Add historical and projection versions of population and world_pop_share metrics (i.e. +4 columns). This is done so that we can plot projections in Grapher using dashed lines.
        - Historical: data from 10,000 BCE until YEAR_THRESHOLD - 1.
        - Projection: data from YEAR_THRESHOLD until 2100.
    - Drop `source` column: Not needed in Grapher.

    NOTE: At the moment, the indicators created by this function are being discarded in a later step. However, we plan on re-incorporating these in the future. This is because this dataset is a work in progress.

    Parameters
    ----------
    table : Table
        Input table.

    Returns
    -------
    Table
        Modified table.
    """
    table = table.reset_index()
    for column in ["population", "world_pop_share"]:
        table = add_projection_and_historical_metrics(table, YEAR_THRESHOLD, column)
    # table = table.drop(columns=["source"])
    table = table.set_index(["country", "year"], verify_integrity=True)
    return table


def add_projection_and_historical_metrics(table: Table, year_threshold: int, metric: str) -> Table:
    """Create projection and historical versions of a metric.

    Parameters
    ----------
    table : Table
        Input table.
    year_threshold : int
        Year after which the data is considered a projection.
    metric : str
        Name of the metric to split.
    description_base : str
        Description of the metric.

    Returns
    -------
    Table
        Modified table. Should have two new columns: `metric_historical` and `metric_projection`.
    """
    # Get mask
    mask = table["year"] < year_threshold
    # Add historical metric
    table = _create_metric_version_from_mask(
        table=table,
        metric=metric,
        mask=mask,
        metric_suffix="historical",
        title_suffix="(historical estimates)",
        display_name_suffix="",
        description=_modify_variable_description_historical(table[metric].metadata.description),
    )
    # Add projection metric
    table = _create_metric_version_from_mask(
        table=table,
        metric=metric,
        mask=-mask,
        metric_suffix="projection",
        title_suffix="(future projections)",
        display_name_suffix="(future projections)",
        description=_modify_variable_description_projection(table[metric].metadata.description),
    )
    return table


def _create_metric_version_from_mask(
    table: Table,
    metric: str,
    mask: List[Any],
    metric_suffix: str,
    title_suffix: str,
    display_name_suffix: str,
    description: str = "",
) -> Table:
    """Create a new metric in the table from a metric that already exists and a mask.

    Parameters
    ----------
    table : Table
        Input table.
    metric : str
        Existing metric. The new metric will be created from this one.
    mask : List[Any]
        Mask to apply to the existing metric to create the new metric.
    metric_suffix : str
        This suffix is added to the name of the new metric: `metric_{metric_suffix}`.
    title_suffix : str
        This title is added to the title of the new metric: `metric {title_suffix}`.
    display_name_suffix : str
        This display title is added to the display title of the new metric: `metric {display_name_suffix}`.
    description : str, optional
        Description of the new metric.

    Returns
    -------
    Table
        Table with the new column.
    """
    # Get dtype
    dtype = table[metric].dtype
    if np.issubdtype(table[metric].dtype, np.integer):
        dtype = "Int64"
    metric_new = f"{metric}_{metric_suffix}"
    table.loc[mask, metric_new] = deepcopy(table.loc[mask, metric])
    table[metric_new].metadata = deepcopy(table[metric].metadata)
    if title_suffix:
        table[metric_new].metadata.title = f"{table[metric_new].metadata.title} {title_suffix}"
    if display_name_suffix:
        if table[metric_new].metadata.display is None:
            table[metric_new].metadata.display = {}
        if "name" in table[metric_new].metadata.display:
            display_name = table[metric_new].metadata.display["name"]
        else:
            display_name = table[metric_new].metadata.title
        table[metric_new].metadata.display["name"] = f"{display_name} {display_name_suffix}"
    table[metric_new].metadata.description = description
    return table.astype({metric_new: dtype})


def _modify_variable_description_historical(variable_description: str) -> str:
    new_description = []
    for line in variable_description.split("\n"):
        match = re.search(r"([\d\-BCE,\s]*):.*", line)
        if match:
            if match.group(1) != f"{YEAR_THRESHOLD}-2100":
                new_description.append(line)
        else:
            new_description.append(line)
    new_description = "\n".join(new_description)
    return new_description


def _modify_variable_description_projection(variable_description: str) -> str:
    new_description = []
    for line in variable_description.split("\n"):
        match = re.search(r"(\* [\d\-BCE,\s]*):.*", line)
        if match:
            if match.group(1) == f"* {YEAR_THRESHOLD}-2100":
                new_description.append(line)
        else:
            new_description.append(line)
    new_description = "\n".join(new_description)
    return new_description
