from copy import deepcopy
from typing import Any, List

import numpy as np
from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR

KEY_INDICATORS_GARDEN = DATA_DIR / "garden/owid/latest/key_indicators"
YEAR_THRESHOLD = 2022


def run(dest_dir: str) -> None:
    # NOTE: this generates shortName `population_density__owid_latest`, perhaps we should keep it as `population_density`
    # and create unique constraint on (shortName, version, namespace) instead of just (shortName, namespace)
    garden_dataset = catalog.Dataset(KEY_INDICATORS_GARDEN)
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(garden_dataset.metadata))

    # Get population table
    table = garden_dataset["population"].reset_index()
    # Create population new metrics
    table = _split_in_projection_and_historical(table, YEAR_THRESHOLD, "population")
    table = _split_in_projection_and_historical(table, YEAR_THRESHOLD, "world_pop_share")
    # table["population_historical"] = deepcopy(table["population"])
    # table["population_projection"] = deepcopy(table["population"])
    # Add population table to dataset
    table = gh.adapt_table_for_grapher(table)
    dataset.add(table)

    # Add land area table to dataset
    table = gh.adapt_table_for_grapher(garden_dataset["land_area"].reset_index())
    dataset.add(table)

    # Add population density table to dataset
    table = gh.adapt_table_for_grapher(garden_dataset["population_density"].reset_index())
    dataset.add(table)

    # Save dataset
    dataset.save()


def _split_in_projection_and_historical(table: catalog.Table, year_threshold: int, metric: str) -> catalog.Table:
    # Get mask
    mask = table["year"] < year_threshold
    # Add historical metric
    table = _add_metric_new(
        table,
        metric,
        mask,
        "historical",
        "(historical estimates)",
        "",
        ["10,000 BCE to 2100", f"10,000 BCE to {year_threshold - 1}"],
    )
    # Add projection metric
    table = _add_metric_new(
        table,
        metric,
        -mask,
        "projection",
        "(future projections)",
        "(future projections)",
        ["10,000 BCE to 2100", f"{year_threshold} to 2100"],
    )
    return table


def _add_metric_new(
    table: catalog.Table,
    metric: str,
    mask: List[Any],
    metric_suffix: str,
    title_suffix: str,
    display_name_suffix: str,
    description_year_replace: List[str],
) -> catalog.Table:
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
        table[metric_new].metadata.display["name"] = f"{table[metric_new].metadata.display['name']} {display_name_suffix}"
    table[metric_new].metadata.description = table[metric_new].metadata.description.replace(*description_year_replace)
    return table.astype({metric_new: dtype})
