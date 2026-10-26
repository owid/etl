"""Load the WHO Mortality Database garden dataset and create the grapher dataset.

The garden tables are long, with `cause`, `sex` and `age_group` as dimensions; here they are widened into one
indicator per combination, and three pieces of text that depend on the cause are filled in from data rather
than repeated in the metadata file: the ICD-10 codes that define the cause (from the `causes` table, which reads
them from WHO's own export headers), the layer label in front of a drill-down's internal title (from the table
it is published in) and the topic tag of a cause whose retired dataset was tagged differently from its table.
"""

from etl.grapher import helpers as gh
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# The label a drill-down table puts in front of its indicators' internal titles, so the admin's flat list tells the
# layers apart; the partition tier (`mortality_database`) has no prefix.
TABLE_LABELS = {
    "vaccine_preventable_diseases": "Vaccine-preventable diseases",
    "intentional_injuries": "Intentional injuries",
    "cancers": "Cancers",
}
# Topic tags that differ by cause inside one table: the retired self_inflicted_injuries dataset was tagged Mental
# Health only (the table's tag) and the retired homicide dataset Global Health only.
TOPIC_TAGS_BY_CAUSE = {"Violence": ["Global Health"]}
ICD_SENTENCE = "The International Classification of Diseases (Version 10) codes that define {cause} are {codes}."


def run() -> None:
    # Step 1: Load the garden dataset; `causes` is a lookup table for this step and is not charted.
    ds_garden = paths.load_dataset("mortality_database")
    codes = ds_garden.read("causes").set_index("cause")["icd10_codes"]

    # Step 2: Widen each fact table and fill in the per-cause text (see the module docstring).
    tables = []
    for name in ds_garden.table_names:
        if name == "causes":
            continue
        tb = gh.long_to_wide(ds_garden.read(name, reset_index=False))
        for column in tb.columns:
            meta = tb[column].metadata
            cause = (meta.dimensions or {}).get("cause")
            # most_common_cancer: its cause is the value, not a dimension; broad_cause_groups: our groups, not WHO's.
            if cause is None or name == "broad_cause_groups":
                continue
            # A string, as the rendering of a one-item YAML list leaves it in the grapher channel.
            meta.description_key = ICD_SENTENCE.format(cause=cause_in_text(cause), codes=codes[cause])
            if name in TABLE_LABELS:
                meta.title = f"{TABLE_LABELS[name]} › {meta.title}"
            if cause in TOPIC_TAGS_BY_CAUSE and meta.presentation is not None:
                meta.presentation.topic_tags = list(TOPIC_TAGS_BY_CAUSE[cause])
        tables.append(tb)

    # Step 3: Save.
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata, long_to_wide=False)
    ds_grapher.save()


def cause_in_text(cause: str) -> str:
    """The cause as it reads inside a sentence; the same rule as `cause_in_text` in the garden metadata."""
    if cause in ("Hepatitis B", "Japanese encephalitis"):
        return cause
    if cause == "Violence":
        return "homicide"
    return cause.lower()
