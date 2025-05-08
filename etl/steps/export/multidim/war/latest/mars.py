# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# INDICATOR dimension (columns starting with this prefix)
DIMENSION_INDICATOR = {
    # Deaths
    "number_deaths_ongoing_conflicts_high__": "deaths",
    "number_deaths_ongoing_conflicts_low__": "deaths",
    # Death rate
    "number_deaths_ongoing_conflicts_high_per_capita": "death_rate",
    "number_deaths_ongoing_conflicts_low_per_capita": "death_rate",
    # New wars: number
    "number_new_conflicts__": "wars_new",
    "number_new_conflicts_per_country": "wars_new_country_rate",
    "number_new_conflicts_per_country_pair": "wars_new_country_pair_rate",
    # Ongoing wars: number
    "number_ongoing_conflicts__": "wars_ongoing",
    "number_ongoing_conflicts_per_country": "wars_ongoing_country_rate",
    "number_ongoing_conflicts_per_country_pair": "wars_ongoing_country_pair_rate",
}
# ESTIMATE dimension
DIMENSION_ESTIMATE = {
    "high": "high",
    "low": "low",
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("mars")
    tb = ds.read("mars", load_data=False)

    # Adjust dimension metadata
    tb = adjust_dimensions(tb)

    # Create MDIM
    mdim = paths.create_mdim_v2(
        config,
        mdim_name="mars",
        tb=tb,
        indicator_names=[
            "deaths",
            "death_rate",
            "wars_ongoing",
            "wars_ongoing_country_rate",
        ],
        dimensions={
            "conflict_type": ["all", "civil war", "others (non-civil)"],
            "estimate": "*",
        },
    )

    # Edit indicator-level display settings
    for view in mdim.views:
        if view.dimensions["conflict_type"] == "civil war":
            assert view.indicators.y is not None
            view.indicators.y[0].display = {"name": "Civil wars"}
        elif view.dimensions["conflict_type"] == "others (non-civil)":
            assert view.indicators.y is not None
            view.indicators.y[0].display = {"name": "Intrastate wars"}
        elif view.dimensions["conflict_type"] == "all":
            assert view.indicators.y is not None
            view.indicators.y[0].display = {"name": f"{view.dimensions["estimate"].title()} estimate"}

    # Group certain views together: used to create StackedBar charts
    mdim.group_views(
        params=[
            {
                "dimension": "conflict_type",
                "choices": ["civil war", "others (non-civil)"],
                "choice_new_slug": "all",
                "config_new": {
                    "chartTypes": ["StackedBar"],
                    "hideAnnotationFieldsInTitle": {
                        "time": True,
                    },
                },
                "overwrite_dimension_choice": True,
            },
            {
                "dimension": "estimate",
                "choices": ["low", "high"],
                "choice_new_slug": "low_high",
                "config_new": {
                    "selectedFacetStrategy": "entity",
                    "hideAnnotationFieldsInTitle": {
                        "time": True,
                    },
                },
            },
        ]
    )

    # Remove certain views
    mdim.drop_views(
        [
            {"conflict_type": ["civil war", "others (non-civil)"]},
            {"estimate": ["high"]},
        ]
    )

    # Edit FAUST
    for view in mdim.views:
        if view.dimensions["indicator"] == "deaths":
            view.config = {
                **(view.config or {}),
                "title": "Deaths in wars",
                "subtitle": "Included are deaths of combatants due to fighting in [interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars that were ongoing that year.",
            }
        elif view.dimensions["indicator"] == "death_rate":
            view.config = {
                **(view.config or {}),
                "title": "Death rate in wars",
                "subtitle": "Deaths of combatants due to fighting, per 100,000 people. Included are [interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars that were ongoing that year.",
            }
        elif view.dimensions["indicator"] == "wars_ongoing":
            view.config = {
                **(view.config or {}),
                "title": "Number of wars",
                "subtitle": "Included are [interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars that were ongoing that year.",
            }
        elif view.dimensions["indicator"] == "wars_ongoing_country_rate":
            view.config = {
                **(view.config or {}),
                "title": "Rate of wars",
                "subtitle": "The number of wars divided by the number of all states. This accounts for the changing number of states over time. Included are [interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars that were ongoing that year.",
            }

    # Save & upload
    mdim.save()


def adjust_dimensions(tb):
    # 1. Adjust indicators dictionary reference (maps full column name to actual indicator)
    dims = {}
    for prefix, indicator_name in DIMENSION_INDICATOR.items():
        columns = list(tb.filter(regex=prefix).columns)
        dims = {**dims, **{c: indicator_name for c in columns}}

    # 2. Iterate over columns and adjust dimensions
    columns = [col for col in tb.columns if col not in {"year", "country"}]
    for col in columns:
        # Overwrite original_short_name to actual indicator name
        if col not in dims:
            raise Exception(f"Column {col} not in indicator mapping")
        tb[col].metadata.original_short_name = dims[col]

        # Add high estimate dimension
        if "high" in col:
            tb[col].metadata.dimensions["estimate"] = "high"
        # Add low estimate dimension
        elif "low" in col:
            tb[col].metadata.dimensions["estimate"] = "low"
        # Add 'NA'
        else:
            tb[col].metadata.dimensions["estimate"] = "na"

    # 3. Adjust table-level dimension metadata
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.append(
            {
                "name": "estimate",
                "slug": "estimate",
            }
        )
    return tb
