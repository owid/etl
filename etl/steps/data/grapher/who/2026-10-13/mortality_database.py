"""Load the WHO Mortality Database garden dataset and create the grapher dataset."""

from etl.grapher import helpers as gh
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Topic tags that differ by cause inside one table. The retired self_inflicted_injuries dataset was tagged Mental Health
# only and the retired homicide dataset Global Health only; the .meta.yml tags a table as a whole (a list cannot vary by
# dimension, and the schema allows no template in it), so the table carries the first and this overrides the second.
TOPIC_TAGS_BY_CAUSE = {"Violence": ["Global Health"]}


def run() -> None:
    # Step 1: Load the fact tables, one per layer of WHO's cause tree, and widen them (one indicator per cause, sex
    # and age group). The `causes` lookup table stays in the catalog; it is not charted.
    ds_garden = paths.load_dataset("mortality_database")
    tables = []
    for name in ds_garden.table_names:
        if name == "causes":
            continue
        tb = gh.long_to_wide(ds_garden.read(name, reset_index=False))
        # Step 2: Per-cause topic tags, where one table holds causes from differently tagged datasets.
        for column in tb.columns:
            tags = TOPIC_TAGS_BY_CAUSE.get((tb[column].metadata.dimensions or {}).get("cause"))
            if tags and tb[column].metadata.presentation is not None:
                tb[column].metadata.presentation.topic_tags = list(tags)
        tables.append(tb)

    # Step 3: Save.
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata, long_to_wide=False)
    ds_grapher.save()
