"""Common grapher step for all FAOSTAT domains."""

from pathlib import Path

from owid.catalog import Table

from etl.helpers import PathFinder

# Define path to current folder, namespace and version of all datasets in this folder.
CURRENT_DIR = Path(__file__).parent
VERSION = CURRENT_DIR.name

# FAOSTAT reports the USSR as a single entity until 1991, and its fifteen successor states separately from 1992.
# In OWID region aggregates, the USSR is counted entirely inside Europe and Upper-middle-income countries, so the
# aggregates of continents and income groups jump abruptly between 1991 and 1992, when its area is redistributed
# among the successor states' regions. FAOSTAT's own continental aggregates break identically (e.g. land area in
# "Europe (FAO)" falls by the same 455 million hectares as in "Europe"), so they get the same annotation.
# NOTE: The following fields are added in this grapher step (and not in the garden step) so that they do not propagate
# to datasets derived from the garden dataset, where they may not apply.
# Item codes in faostat_rl with USSR data (hence affected by this issue in their OWID region aggregates).
USSR_BREAKUP_AFFECTED_ITEM_CODES_RL = {
    # Items with USSR data from 1961 to 1991.
    "00006600",  # Country area
    "00006601",  # Land area
    "00006602",  # Agriculture
    "00006610",  # Agricultural land
    "00006620",  # Cropland
    "00006621",  # Arable land
    "00006650",  # Permanent crops
    "00006655",  # Permanent meadows and pastures
    "00006680",  # Inland waters
    "00006690",  # Land area equipped for irrigation
    # Items with USSR data from 1990 to 1991 (the jump mostly affects income groups).
    "00006646",  # Forest land
    "00006670",  # Other land
    "00006714",  # Primary forest
    "00006716",  # Planted forest
    "00006717",  # Naturally regenerating forest
}
# Element codes in faostat_rl affected by the same issue (the "Share in ..." elements are not affected, since they are
# never aggregated by OWID).
USSR_BREAKUP_AFFECTED_ELEMENT_CODES_RL = {
    "005110",  # Area
    "5110pc",  # Area per capita
}
# Explanation of the issue, shown in the "What you should know about this data" section.
# NOTE: Entity annotations (defined below) only render on charts with entity-labeled series (e.g. line charts of one
# indicator for multiple entities), so this point is the only visible explanation on other charts, e.g. stacked charts.
USSR_BREAKUP_DESCRIPTION_KEY = (
    "FAOSTAT reports the USSR as a single entity until 1991, and its successor states separately from 1992. "
    "This causes an abrupt break between 1991 and 1992 in the aggregates for Europe, Asia, High-income countries, "
    "Upper-middle-income countries, and Lower-middle-income countries."
)
# Annotations shown next to the affected entities in charts (only rendered when one of these entities is selected).
USSR_BREAKUP_ENTITY_ANNOTATIONS = "\n".join(
    [
        "Europe: Break in 1992: the USSR's successors are split between Europe and Asia",
        "Europe (FAO): Break in 1992: the USSR's successors are split between Europe and Asia",
        "Asia: Break in 1992: the USSR's successors are split between Europe and Asia",
        "Asia (FAO): Break in 1992: the USSR's successors are split between Europe and Asia",
        "High-income countries: Break in 1992: the USSR's successors are split across income groups",
        "Upper-middle-income countries: Break in 1992: the USSR's successors are split across income groups",
        "Lower-middle-income countries: Break in 1992: the USSR's successors are split across income groups",
    ]
)


def add_ussr_breakup_annotations(tb: Table) -> None:
    """Annotate the region aggregates affected by the redistribution of the USSR among its successor states.

    NOTE: Metadata is normally defined in the garden step, but these fields are deliberately added here instead:
    derived datasets read the garden dataset, and variable operations (e.g. forest_area / country_area in
    fra_forest_extent) carry description_key along, so a garden-level version leaked this text into indicators
    where it does not apply. Charts and data pages read the grapher channel, so they still get both fields.
    """
    for column in tb.columns:
        item, item_code, element, element_code, unit = sum(
            [[j.strip() for j in i.split("|")] for i in tb[column].metadata.title.split("||")], []
        )
        if (item_code in USSR_BREAKUP_AFFECTED_ITEM_CODES_RL) and (
            element_code in USSR_BREAKUP_AFFECTED_ELEMENT_CODES_RL
        ):
            tb[column].display["entityAnnotationsMap"] = USSR_BREAKUP_ENTITY_ANNOTATIONS
            assert tb[column].metadata.description_key is None, "Unexpected description_key; merge manually."
            tb[column].metadata.description_key = USSR_BREAKUP_DESCRIPTION_KEY


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = Path(dest_dir).name

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Load latest garden dataset.
    ds_garden = paths.load_dataset(dataset_short_name)

    # Load wide  table from dataset.
    tb_garden = ds_garden[f"{dataset_short_name}_flat"]

    #
    # Process data.
    #
    if dataset_short_name == "faostat_rl":
        add_ussr_breakup_annotations(tb=tb_garden)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(
        tables=[tb_garden], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()
