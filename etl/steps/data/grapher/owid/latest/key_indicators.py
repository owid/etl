from copy import deepcopy
from typing import Any, List

from owid import catalog

from etl.paths import DATA_DIR

KEY_INDICATORS_GARDEN = DATA_DIR / "garden/owid/latest/key_indicators"
YEAR_THRESHOLD = 2022


def run(dest_dir: str) -> None:
    # NOTE: this generates shortName `population_density__owid_latest`, perhaps we should keep it as `population_density`
    # and create unique constraint on (shortName, version, namespace) instead of just (shortName, namespace)
    garden_dataset = catalog.Dataset(KEY_INDICATORS_GARDEN)
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)

    # Get population table
    table = garden_dataset["population"].reset_index()
    # Create population new metrics
    table = _split_in_projection_and_historical(
        table=table,
        year_threshold=YEAR_THRESHOLD,
        metric="population",
        description_base="Population by country",
    )
    table = _split_in_projection_and_historical(
        table=table,
        year_threshold=YEAR_THRESHOLD,
        metric="world_pop_share",
        description_base="Share of the world's population by country",
    )
    # table["population_historical"] = deepcopy(table["population"])
    # table["population_projection"] = deepcopy(table["population"])
    # Add population table to dataset
    dataset.add(table)

    # Add land area table to dataset
    dataset.add(garden_dataset["land_area"].reset_index())

    # Add population density table to dataset
    dataset.add(garden_dataset["population_density"].reset_index())

    # Save dataset
    dataset.save()


def _split_in_projection_and_historical(
    table: catalog.Table, year_threshold: int, metric: str, description_base: str
) -> catalog.Table:
    # Get mask
    mask = table["year"] < year_threshold
    # Add historical metric
    table = _add_metric_new(
        table=table,
        metric=metric,
        mask=mask,
        metric_suffix="historical",
        title_suffix="(historical estimates)",
        display_name_suffix="",
        description=(
            f"{description_base}, available from 10,000 BCE to {year_threshold - 1}\n\n"
            "10,000 BCE - 1799: Historical estimates by HYDE (v3.2).\n"
            "1800-1949: Historical estimates by Gapminder.\n"
            f"1950-{year_threshold}: Population records by the United Nations - Population Division (2022)."
        ),
    )
    # Add projection metric
    table = _add_metric_new(
        table=table,
        metric=metric,
        mask=-mask,
        metric_suffix="projection",
        title_suffix="(future projections)",
        display_name_suffix="(future projections)",
        description=(
            f"{description_base}, available from {year_threshold} to 2100\n\n{year_threshold}-2100: Projections"
            " based on Medium variant by the United Nations - Population Division (2022)."
        ),
    )
    return table


def _add_metric_new(
    table: catalog.Table,
    metric: str,
    mask: List[Any],
    metric_suffix: str,
    title_suffix: str,
    display_name_suffix: str,
    description: str = "",
) -> catalog.Table:
    metric_new = f"{metric}_{metric_suffix}"
    table.loc[mask, metric_new] = deepcopy(table.loc[mask, metric])
    table[metric_new].metadata = deepcopy(table[metric].metadata)
    if title_suffix:
        table[metric_new].metadata.title = f"{table[metric_new].metadata.title} {title_suffix}"
    if display_name_suffix:
        table[metric_new].metadata.display["name"] = (
            f"{table[metric_new].metadata.display['name']} {display_name_suffix}"
        )
    table[metric_new].metadata.description = description

    # Get dtype
    dtype = table[metric].dtype
    if "int" in str(dtype).lower():
        dtype = "Int64"

    return table.astype({metric_new: dtype})
