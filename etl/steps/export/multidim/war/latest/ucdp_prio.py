# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# INDICATOR dimension (columns starting with this prefix)
DIMENSION_INDICATOR = {
    # Deaths
    "number_deaths_ongoing_conflicts__": "deaths",
    "number_deaths_ongoing_conflicts_high__": "deaths",
    "number_deaths_ongoing_conflicts_low__": "deaths",
    # Death rate
    "number_deaths_ongoing_conflicts_per_capita": "death_rate",
    "number_deaths_ongoing_conflicts_high_per_capita": "death_rate",
    "number_deaths_ongoing_conflicts_low_per_capita": "death_rate",
    # New wars: number
    # "number_new_conflicts__": "wars_new",
    # "number_new_conflicts_per_country": "wars_new_country_rate",
    # "number_new_conflicts_per_country_pair": "wars_new_country_pair_rate",
    # # Ongoing wars: number
    # "number_ongoing_conflicts__": "wars_ongoing",
    # "number_ongoing_conflicts_per_country": "wars_ongoing_country_rate",
    # "number_ongoing_conflicts_per_country_pair": "wars_ongoing_country_pair_rate",
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds_up = paths.load_dataset("ucdp_prio")
    ds_u = paths.load_dataset("ucdp")
    tb_up = ds_up.read("ucdp_prio", load_data=False)
    # tb_u = ds_u.read("ucdp", load_data=False)

    tb_up = adjust_dimensions(tb_up)

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb_up,
        indicator_names=[
            "deaths",
            "death_rate",
            # "wars_ongoing",
            # "wars_ongoing_country_rate",
        ],
        dimensions={
            "conflict_type": [
                "state-based",
                "interstate",
                "intrastate (non-internationalized)",
                "intrastate (internationalized)",
                "extrasystemic",
            ],
            "estimate": "*",
        },
        common_view_config={
            "hideAnnotationFieldsInTitle": {
                "time": True,
            },
        },
    )

    # Edit indicator-level display settings
    choice_names = c.get_choice_names("conflict_type")
    for view in c.views:
        for slug, name in choice_names.items():
            if view.dimensions["conflict_type"] == slug:
                assert view.indicators.y is not None
                view.indicators.y[0].display = {"name": name}

    # Aggregate views
    c.group_views(
        params=[
            {
                "dimension": "conflict_type",
                "choices": [
                    "interstate",
                    "intrastate (internationalized)",
                    "intrastate (non-internationalized)",
                    "extrasystemic",
                ],
                "choice_new_slug": "state-based-stacked",
                "config_new": {
                    "chartTypes": ["StackedBar"],
                    "hideAnnotationFieldsInTitle": {
                        "time": True,
                    },
                },
            },
            {
                "dimension": "estimate",
                "choices": ["low", "high", "best"],
                "choice_new_slug": "best-ci",
                "config_new": {
                    "selectedFacetStrategy": "entity",
                    "hideAnnotationFieldsInTitle": {
                        "time": True,
                    },
                },
            },
        ]
    )

    c.drop_views(
        [
            {"estimate": ["low", "high"]},
        ]
    )

    # Edit FAUST
    edit_faust(c)

    # Save & upload
    c.save()


def adjust_dimensions(tb):
    """Add dimensions to table columns.

    It adds field `indicator` and `estimate`.
    """
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
        if "_high_" in col:
            tb[col].metadata.dimensions["estimate"] = "high"
        # Add low estimate dimension
        elif "_low_" in col:
            tb[col].metadata.dimensions["estimate"] = "low"
        # Add 'NA'
        else:
            tb[col].metadata.dimensions["estimate"] = "best"

    # 3. Adjust table-level dimension metadata
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.append(
            {
                "name": "estimate",
                "slug": "estimate",
            }
        )
    return tb


def edit_faust(c):
    """Edit FAUST of views: Chart and indicator-level."""
    choice_names = c.get_choice_names("conflict_type")
    for view in c.views:
        # Edit title and subtitle in charts
        edit_view_title(view, choice_names)

        # Edit FAUST in charts with CI (color, display names). Indicator-level.
        edit_view_display_estimates_ci(view)


def edit_view_title(view, conflict_renames):
    """Edit FAUST titles and subtitles."""
    # Get conflict type name
    conflict_name = "state-based conflicts"
    if view.dimensions["conflict_type"] not in {"state-based", "state-based-stacked"}:
        conflict_name = conflict_renames.get(view.dimensions["conflict_type"]).lower()

    # Add title based on indicator
    if view.dimensions["indicator"] == "deaths":
        view.config = {
            **(view.config or {}),
            "title": f"Deaths in {conflict_name}",
        }
    elif view.dimensions["indicator"] == "death_rate":
        view.config = {
            **(view.config or {}),
            "title": f"Death rate in {conflict_name}",
        }
    elif view.dimensions["indicator"] == "wars_ongoing":
        view.config = {
            **(view.config or {}),
            "title": f"Number of {conflict_name}",
            "subtitle": "Included are [interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars that were ongoing that year.",
        }
    elif view.dimensions["indicator"] == "wars_ongoing_country_rate":
        view.config = {
            **(view.config or {}),
            "title": f"Rate of {conflict_name}",
            "subtitle": "The number of wars divided by the number of all states. This accounts for the changing number of states over time. Included are [interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars that were ongoing that year.",
        }


def edit_view_display_estimates_ci(view):
    """Edit FAUST estimates for confidence intervals."""
    if view.dimensions["estimate"] == "best-ci":
        assert view.indicators.y is not None
        for indicator in view.indicators.y:
            if "_high_" in indicator.catalogPath:
                indicator.display = {
                    "name": "High estimate",
                    "color": "#C3AEA6",
                }
            elif "_low_" in indicator.catalogPath:
                indicator.display = {
                    "name": "Low estimate",
                    "color": "#C3AEA6",
                }
            else:
                indicator.display = {
                    "name": "Best estimate",
                    "color": "#B13507",
                }
