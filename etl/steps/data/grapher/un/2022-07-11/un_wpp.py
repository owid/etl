import structlog
import yaml
from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR, STEP_DIR

NAMESPACE = "un"
VERSION = "2022-07-11"
FNAME = "un_wpp"
TNAME = "un_wpp"
SOURCE_NAME_DISPLAY = "UN, World Population Prospects (2022)"


log = structlog.get_logger()


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATA_DIR / "garden" / NAMESPACE / VERSION / FNAME)
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    dataset.save()

    # Get table (in appropriate format)
    table = _get_shaped_table(garden_dataset)

    # Filter rows
    table = _filter_rows(table)

    # Add metadata
    table = _propagate_metadata(dataset, table)

    for wide_table in gh.long_to_wide_tables(table):
        # table is generated for every column, use it as a table name
        wide_table.metadata.short_name = wide_table.columns[0]
        dataset.add(wide_table)
    dataset.save()


def _get_shaped_table(dataset: catalog.Dataset) -> catalog.Table:
    table = dataset[TNAME].reset_index()

    # use entity_id and year as indexes in grapher
    table = table.rename(columns={"location": "country", "metric": "variable"}).set_index(
        ["country", "year", "sex", "age", "variant"]
    )[["variable", "value"]]
    return table


def _propagate_metadata(dataset: catalog.Dataset, table: catalog.Table) -> catalog.Table:
    with open(STEP_DIR / "data/garden/un/2022-07-11/un_wpp/un_wpp.meta.yml", "r") as f:
        meta = yaml.safe_load(f)

    meta_map = {}
    for var_name, var_meta in meta["tables"][TNAME]["variables"].items():
        var_meta = catalog.VariableMeta(**var_meta)
        meta_map[var_name] = var_meta
        var_meta.sources = dataset.metadata.sources
        # Temporary
        var_meta.sources[0].name = SOURCE_NAME_DISPLAY

    table["meta"] = table["variable"].astype(object).map(meta_map)
    return table


def _filter_rows(table: catalog.Table) -> catalog.Table:
    variants_valid = ["estimates", "low", "medium", "high", "constant fertility"]
    shape_0 = table.shape[0]
    table = table[table.index.isin(variants_valid, level=4)]
    r = 100 - 100 * round(table.shape[0] / shape_0, 2)
    log.info(f"Removed {r}% rows, by only keeping variants {variants_valid}")
    return table
