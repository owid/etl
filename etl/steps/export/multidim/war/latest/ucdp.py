"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COMMON_CONFIG = {
    "hasMapTab": True,
    "originUrl": "ourworldindata.org/ucdp",
    "relatedQuestions": [
        {
            "text": "How do different approaches measure armed conflicts and their deaths?",
            "url": "https://ourworldindata.org/conflict-data-how-do-researchers-measure-armed-conflicts-and-their-deaths",
        }
    ],
    "map": {
        "colorScale": {"baseColorScheme": "OrRd"},
    },
    "hideAnnotationFieldsInTitle": {
        "time": True,
    },
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("ucdp")
    tb = ds.read("ucdp", load_data=False)
    ds_pre = paths.load_dataset("ucdp_preview")
    tb_pre = ds_pre.read("ucdp_preview", load_data=False)

    # Filter unnecessary columns
    tb = tb.filter(regex="^country|^year|^number_deaths_ongoing|^number_ongoing_conflicts__")
    tb_pre = tb_pre.filter(regex="^country|^year|^number_deaths_ongoing|^number_ongoing_conflicts__")

    #
    # (optional) Adjust dimensions if needed
    #
    tb = adjust_dimensions(tb)
    tb_pre = adjust_dimensions(tb_pre)

    #
    # Create collection object
    #
    choices = {tb[col].m.dimensions["conflict_type"] for col in tb.columns if col not in {"country", "year"}} - {"all"}

    c = paths.create_collection(
        config=config,
        short_name="ucdp",
        tb=[tb, tb_pre],
        indicator_names=[
            [
                "deaths",
                "death_rate",
                "num_conflicts",
            ],
            ["deaths"],
        ],
        dimensions=[
            {"conflict_type": list(choices), "estimate": "*", "people": "*"},
            {"conflict_type": ["all"], "estimate": "*", "people": "*"},
        ],
        common_view_config=COMMON_CONFIG,
        indicator_as_dimension=True,
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
            # By conflict type
            {
                "dimension": "conflict_type",
                "choices": [
                    "interstate",
                    "intrastate",
                    "non-state conflict",
                    "one-sided violence",
                ],
                "choice_new_slug": "all_stacked",
                "view_config": COMMON_CONFIG
                | {
                    "chartTypes": ["StackedBar"],
                    "hasMapTab": False,
                },
                "view_metadata": {"description_key": lambda view: _set_description_key(view, tb)},
            },
            # By death type
            {
                "dimension": "people",
                "choices": ["combatants", "civilians", "unknown"],
                "choice_new_slug": "all_stacked",
                "view_config": COMMON_CONFIG
                | {
                    "chartTypes": ["StackedBar"],
                    "sortBy": "custom",
                    "hasMapTab": False,
                },
                "view_metadata": {
                    "description_key": lambda view: _set_description_key(view, tb),
                },
            },
            # Best + CI estimates
            {
                "dimension": "estimate",
                "choices": ["low", "high", "best"],
                "choice_new_slug": "best_ci",
                "view_config": COMMON_CONFIG | {"hasMapTab": False},
                "view_metadata": {"description_key": lambda view: _set_description_key(view, tb)},
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
            {"conflict_type": "one-sided violence", "people": "all_stacked"},
        ]
    )

    #
    # (optional) Edit views
    #
    for view in c.views:
        # Edit FAUST in charts with CI (color, display names). Indicator-level.
        edit_indicator_displays(view)

    # Edit view configs
    c.set_global_config(
        {
            "timelineMinTime": 1989,
            "selectedFacetStrategy": "entity",
            "title": lambda view: _set_title(view, choice_names),
            "subtitle": lambda view: _set_subtitle(view),
            "note": lambda view: _set_note(view),
            "hideRelativeToggle": lambda view: (view.dimensions["people"] != "all_stacked")
            and (view.dimensions["conflict_type"] != "all_stacked"),
            "hideFacetControl": lambda view: view.dimensions["estimate"] == "best_ci",
            "yAxis": {
                "facetDomain": lambda view: "independent" if view.d.conflict_type == "all" else "shared",
            },
        },
    )

    c.edit_views(
        edits=[
            {
                "dimensions": {
                    "indicator": "deaths",
                    "conflict_type": "all",
                    "people": "all",
                    "estimate": "best",
                },
                "config": {
                    "yAxis": {
                        "facetDomain": "independent",
                    },
                },
            }
        ]
    )

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


def _set_description_key(view, tb):
    if view.dimensions["conflict_type"] in ("all", "all_stacked"):
        column = "number_deaths_ongoing_conflicts__conflict_type_all"
    elif view.dimensions["conflict_type"] == "intrastate":
        column = "number_deaths_ongoing_conflicts__conflict_type_intrastate"
    elif view.dimensions["conflict_type"] == "interstate":
        column = "number_deaths_ongoing_conflicts__conflict_type_interstate"
    elif view.dimensions["conflict_type"] == "non-state conflict":
        column = "number_deaths_ongoing_conflicts__conflict_type_non_state_conflict"
    else:
        return []
    keys = tb[column].m.description_key

    if (view.dimensions["estimate"] == "best_ci") or (view.dimensions["indicator"] == "num_conflicts"):
        assert keys[-1].startswith("'Best' death estimates")
        keys = keys[:-1] + [None]

    return keys


def _set_title(view, choice_names):
    conflict_name = _get_conflict_name(view, choice_names)
    if view.dimensions["indicator"] in ("deaths", "death_rate"):
        # Condition by indicator: "Deaths" vs "Death rate"
        if view.dimensions["indicator"] == "death_rate":
            title = "Death rate"
        else:
            title = "Deaths"

        # Condition by conflict type: "in" vs "from", add conflict_name
        title += f" {'from' if view.dimensions['conflict_type'] == 'one-sided violence' else 'in'} {conflict_name} based on where they occurred"

        # Condition by people: "Civilian and combatant" breakdown or aggregate
        if view.dimensions["people"] == "all_stacked":
            title = f"Civilian and combatant {title.lower()}"
    else:
        title = f"Number of {'cases of ' if view.dimensions['conflict_type'] == 'one-sided violence' else ''}{conflict_name}"

    # Add 'by type' at the end if applicable
    if view.dimensions["conflict_type"] == "all_stacked":
        title += " by type"
    return title


def _set_subtitle(view):
    """Set subtitle based on view dimensions."""
    if view.dimensions["conflict_type"] == "one-sided violence":
        if view.dimensions["indicator"] == "num_conflicts":
            return "Included are cases of [one-sided violence against civilians](#dod:onesided-ucdp) that were ongoing that year."
        return f"Reported deaths of civilians due to [one-sided violence](#dod:onesided-ucdp) that was ongoing that year{', per 100,000 people' if view.dimensions['indicator']=='death_rate' else ''}. Deaths due to disease and starvation resulting from one-sided violence are not included."

    # DoD
    if view.dimensions["conflict_type"] == "all":
        dod = "[armed conflicts](#dod:armed-conflict-ucdp)"
    elif view.dimensions["conflict_type"] == "interstate":
        dod = "[interstate conflicts](#dod:interstate-ucdp)"
    elif view.dimensions["conflict_type"] == "intrastate":
        dod = "[civil conflicts](#dod:intrastate-ucdp)"
    elif view.dimensions["conflict_type"] == "non-state conflict":
        dod = "[non-state conflicts](#dod:nonstate-ucdp)"
    elif view.dimensions["conflict_type"] == "all_stacked":
        dod = "[interstate](#dod:interstate-ucdp), [civil](#dod:intrastate-ucdp), [non-state](#dod:nonstate-ucdp) conflicts, and [violence against civilians](#dod:onesided-ucdp)"
    else:
        raise ValueError(f"Unknown conflict type: {view.dimensions['conflict_type']}")

    if view.dimensions["indicator"] in ("deaths", "death_rate"):
        # Subtitle template
        subtitle_template = "Reported deaths of combatants and civilians due to fighting{placeholder} that were ongoing that year. Deaths due to disease and starvation resulting from the conflict are not included."

        # Define subtitle
        if view.dimensions["indicator"] == "deaths":
            return subtitle_template.format(placeholder=f" in {dod}")
        elif view.dimensions["indicator"] == "death_rate":
            return subtitle_template.format(placeholder=f", per 100,000 people. Included are {dod}")
    elif view.dimensions["indicator"] == "num_conflicts":
        return f"Included are {dod} that were ongoing that year."
    return ""


def _set_note(view):
    """Set subtitle based on view dimensions."""
    note = ""
    if view.dimensions["estimate"] == "best_ci":
        note = "'Best' estimates as identified by UCDP."
    elif view.dimensions["indicator"] == "num_conflicts":
        note = "Some conflicts affect several countries and regions. The sum across all countries and regions can therefore be higher than the total number."
    return note


def _get_conflict_name(view, choice_names):
    """Get conflict name based on view dimensions."""
    conflict_name = "armed conflicts"
    if view.dimensions["conflict_type"] == "one-sided violence":
        conflict_name = "one-sided violence against civilians"
    elif view.dimensions["conflict_type"] not in {"all", "all_stacked"}:
        conflict_name = choice_names[view.dimensions["conflict_type"]].lower()
    return conflict_name


def edit_indicator_displays(view):
    """Edit FAUST estimates for confidence intervals."""
    # Set color to red if there is only one line in the chart
    if (view.indicators.y is not None) and (len(view.indicators.y) == 1):
        if view.indicators.y[0].display is None:
            view.indicators.y[0].display = {"color": "#B13507"}
        else:
            view.indicators.y[0].display["color"] = "#B13507"

    # Set colors for stacked bar charts
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
