"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("ucdp")
    tb = ds.read("ucdp", load_data=False)

    # Filter unnecessary columns
    tb = tb.filter(regex="^country|^year|^number_deaths_ongoing|^number_ongoing_conflicts__")

    #
    # (optional) Adjust dimensions if needed
    #
    tb = adjust_dimensions(tb)

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="ucdp",
        tb=tb,
        indicator_names=[
            "deaths",
            "death_rate",
            "num_conflicts",
        ],
        common_view_config={
            "hideAnnotationFieldsInTitle": {
                "time": True,
            },
        },
        # dimensions={},
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
        groups=[
            {
                "dimension": "conflict_type",
                "choices": [
                    "interstate",
                    "intrastate",
                    "non-state conflict",
                    "one-sided violence",
                ],
                "choice_new_slug": "all_stacked",
                "view_config": {
                    "chartTypes": ["StackedBar"],
                    "hideAnnotationFieldsInTitle": {
                        "time": True,
                    },
                },
            },
            {
                "dimension": "estimate",
                "choices": ["low", "high", "best"],
                "choice_new_slug": "best_ci",
                "view_config": {
                    "selectedFacetStrategy": "entity",
                    "hideAnnotationFieldsInTitle": {
                        "time": True,
                    },
                },
            },
            {
                "dimension": "people",
                "choices": ["combatants", "civilians", "unknown"],
                "choice_new_slug": "all_stacked",
                "view_config": {
                    "chartTypes": ["StackedBar"],
                    "selectedFacetStrategy": "entity",
                    "hideAnnotationFieldsInTitle": {
                        "time": True,
                    },
                    "sortBy": "custom",
                },
            },
        ]
    )

    # Drop views
    c.drop_views(
        [
            {
                "conflict_type": [
                    "extrasystemic",
                    "intrastate (internationalized)",
                    "intrastate (non-internationalized)",
                    "state-based",
                ]
            },
            {"estimate": ["low", "high"]},
            {"people": ["combatants", "civilians", "unknown"]},
        ]
    )

    #
    # (optional) Edit views
    #
    for view in c.views:
        # Edit title and subtitle in charts
        edit_view_title(view, choice_names)

        # Edit FAUST in charts with CI (color, display names). Indicator-level.
        edit_view_display_estimates_ci(view)

    #
    # Save garden dataset.
    #
    c.save()


def adjust_dimensions(tb):
    """Add dimensions to table columns.

    It adds fields:
    - `estimate`
    - `people`

    And tweaks original_short_name to make this consistent.
    """

    # Mapping columns to indicators
    indicator_mapping_ = {
        # Deaths
        "^number_deaths_ongoing_conflicts(_combatants|_civilians|_high|_low|_unknown)?__": "deaths",
        "^number_deaths_ongoing_conflicts(_high|_low)?_per_capita__": "death_rate",
        "^number_ongoing_conflicts__": "num_conflicts",
    }

    # 1. Adjust indicators dictionary reference (maps full column name to actual indicator)
    indicator_mapping = {}
    for prefix, indicator_name in indicator_mapping_.items():
        columns = list(tb.filter(regex=prefix).columns)
        indicator_mapping = {**indicator_mapping, **{c: indicator_name for c in columns}}

    # 2. Iterate over columns and adjust dimensions
    columns = [col for col in tb.columns if col not in {"year", "country"}]
    for col in columns:
        # Overwrite original_short_name to actual indicator name
        if col not in indicator_mapping:
            raise Exception(f"Column {col} not in indicator mapping")
        tb[col].metadata.original_short_name = indicator_mapping[col]

        # Add estimate
        if indicator_mapping[col] not in {"deaths", "death_rate"}:
            tb[col].metadata.dimensions["estimate"] = "na"
        else:
            if "_high_" in col:
                tb[col].metadata.dimensions["estimate"] = "high"
            # Add low estimate dimension
            elif "_low_" in col:
                tb[col].metadata.dimensions["estimate"] = "low"
            # Add 'Best'
            else:
                tb[col].metadata.dimensions["estimate"] = "best"

        # Add people dimension
        if indicator_mapping[col] == "deaths":
            if "_combatants_" in col:
                tb[col].metadata.dimensions["people"] = "combatants"
            # Add low estimate dimension
            elif "_civilians_" in col:
                tb[col].metadata.dimensions["people"] = "civilians"
            # Add 'Best'
            elif "_unknown_" in col:
                tb[col].metadata.dimensions["people"] = "unknown"
            else:
                tb[col].metadata.dimensions["people"] = "all"
        else:
            tb[col].metadata.dimensions["people"] = "na"

    # 3. Adjust table-level dimension metadata
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.extend(
            [
                {
                    "name": "estimate",
                    "slug": "estimate",
                },
                {
                    "name": "people",
                    "slug": "people",
                },
            ]
        )
    return tb


def edit_view_title(view, conflict_renames):
    """Edit FAUST titles and subtitles."""
    # Get conflict type name
    conflict_name = "armed conflicts"
    if view.dimensions["conflict_type"] not in {"all", "all_stacked"}:
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
    elif view.dimensions["indicator"] == "num_conflicts":
        if view.dimensions["conflict_type"] == "one-sided violence":
            title = "Number of episodes of one-sided violence"
        else:
            title = f"Number of {conflict_name}"
        view.config = {
            **(view.config or {}),
            "title": title,
        }


def edit_view_display_estimates_ci(view):
    """Edit FAUST estimates for confidence intervals."""
    if view.dimensions["estimate"] == "best_ci":
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
    if view.dimensions["people"] == "all_stacked":
        assert view.indicators.y is not None
        for indicator in view.indicators.y:
            if "_combatants_" in indicator.catalogPath:
                indicator.display = {
                    "name": "Combatant deaths",
                    "color": "#00847E",
                }
            elif "_civilians_" in indicator.catalogPath:
                indicator.display = {
                    "name": "Civilian deaths",
                    "color": "#C15065",
                }
            else:
                indicator.display = {
                    "name": "Unclear deaths",
                    "color": "#00295B",
                }
