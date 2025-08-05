"""I've implemented a simple version of create_collections with support for multiple tables. We should move this somewhere so others can use, or just replace the behavior of paths.create_collection."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


COMMON_CONFIG = {
    "originUrl": "ourworldindata.org/war-and-peace",
    "relatedQuestions": [
        {
            "text": "How do different approaches measure armed conflicts and their deaths?",
            "url": "https://ourworldindata.org/conflict-data-how-do-researchers-measure-armed-conflicts-and-their-deaths",
        }
    ],
    "hideAnnotationFieldsInTitle": {
        "time": True,
        "entity": True,
    },
    "entityType": "region",
    "entityTypePlural": "regions",
    "chartTypes": ["StackedBar"],
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ## UCDP/PRIO
    ds_up = paths.load_dataset("ucdp_prio")
    tb_up = ds_up.read("ucdp_prio")
    ## UCDP
    ds_u = paths.load_dataset("ucdp")
    tb_ucdp = ds_u.read("ucdp", load_data=False)

    # Filter unnecessary columns
    tb_ucdp = tb_ucdp.filter(regex="^country|^year|^number_ongoing_conflicts")

    # Adjust dimension metadata
    ## UCDP/PRIO
    tb_up = adjust_dimensions_ucdp_prio(tb_up)
    ## UCDP
    tb_ucdp = adjust_dimensions_ucdp(tb_ucdp)

    # Create collections
    c = paths.create_collection(
        tb=[tb_up, tb_ucdp],
        config=config,
        indicator_names=[
            ["deaths", "death_rate"],
            ["wars_ongoing", "wars_ongoing_country_rate"],
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
        common_view_config=COMMON_CONFIG,
    )

    # Edit indicator-level display settings
    choice_names = c.get_choice_names("conflict_type")
    for view in c.views:
        for slug, name in choice_names.items():
            if view.d.conflict_type == slug:
                assert view.indicators.y is not None
                view.indicators.y[0].display = {"name": name}

    # Aggregate views
    c.group_views(
        groups=[
            {
                "dimension": "conflict_type",
                "choices": [
                    "interstate",
                    "intrastate (internationalized)",
                    "intrastate (non-internationalized)",
                    "extrasystemic",
                ],
                "choice_new_slug": "state_based_stacked",
                "view_config": COMMON_CONFIG
                | {
                    "chartTypes": ["StackedBar"],
                    "selectedFacetStrategy": "entity",
                },
            },
            {
                "dimension": "estimate",
                "choices": ["low", "high", "best"],
                "choice_new_slug": "best_ci",
                "view_config": COMMON_CONFIG
                | {
                    "selectedFacetStrategy": "entity",
                    "chartTypes": ["LineChart"],
                },
            },
        ]
    )

    # Drop views
    c.drop_views(
        [
            {"estimate": ["low", "high"]},
        ]
    )

    # Edit FAUST
    region_names = tb_up.dropna(subset="number_deaths_ongoing_conflicts__conflict_type_all")["country"].unique()
    edit_faust(c, tb_ucdp, tb_up, region_names)

    # Save & upload
    c.save()


def adjust_dimensions_ucdp_prio(tb):
    """Add dimensions to table columns.

    It adds field `indicator` and `estimate`.
    """

    def fct(tb, col):
        # Add high estimate dimension
        if "_high_" in col:
            tb[col].metadata.dimensions["estimate"] = "high"
        # Add low estimate dimension
        elif "_low_" in col:
            tb[col].metadata.dimensions["estimate"] = "low"
        # Add 'Best'
        else:
            tb[col].metadata.dimensions["estimate"] = "best"

    adjust_dimensions(
        tb,
        {
            # Deaths
            "number_deaths_ongoing_conflicts__": "deaths",
            "number_deaths_ongoing_conflicts_high__": "deaths",
            "number_deaths_ongoing_conflicts_low__": "deaths",
            # Death rate
            "number_deaths_ongoing_conflicts_per_capita": "death_rate",
            "number_deaths_ongoing_conflicts_high_per_capita": "death_rate",
            "number_deaths_ongoing_conflicts_low_per_capita": "death_rate",
        },
        fct,
    )
    return tb


def adjust_dimensions_ucdp(tb):
    """Add dimensions to table columns.

    It adds field `indicator` and `estimate`.
    """

    def fct(tb, col):
        tb[col].metadata.dimensions["estimate"] = "na"

    adjust_dimensions(
        tb,
        {
            # # Ongoing wars: number
            "number_ongoing_conflicts__": "wars_ongoing",
            "number_ongoing_conflicts_per_country": "wars_ongoing_country_rate",
            "number_ongoing_conflicts_per_country_pair": "wars_ongoing_country_pair_rate",
        },
        fct,
    )

    return tb


def adjust_dimensions(tb, indicator_dim, fct_dims):  # -> Any:
    """Add dimensions to table columns.

    It adds field `indicator` and `estimate`.
    """
    # 1. Adjust indicators dictionary reference (maps full column name to actual indicator)
    dims = {}
    for prefix, indicator_name in indicator_dim.items():
        columns = list(tb.filter(regex=prefix).columns)
        dims = {**dims, **{c: indicator_name for c in columns}}

    # 2. Iterate over columns and adjust dimensions
    columns = [col for col in tb.columns if col not in {"year", "country"}]
    for col in columns:
        # Overwrite original_short_name to actual indicator name
        if col not in dims:
            raise Exception(f"Column {col} not in indicator mapping")
        tb[col].metadata.original_short_name = dims[col]

        # Add NA as dimension "estimate"
        fct_dims(tb, col)

    # 3. Adjust table-level dimension metadata
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.append(
            {
                "name": "estimate",
                "slug": "estimate",
            }
        )
    return tb


def edit_faust(c, tb_ucdp, tb_up, region_names):
    """Edit FAUST of views: Chart and indicator-level."""
    choice_names = c.get_choice_names("conflict_type")
    for view in c.views:
        # Edit FAUST in charts with CI (color, display names). Indicator-level.
        edit_indicator_displays(view)

        # Set color to red if there is only one line in the chart
        if (view.indicators.y is not None) and (len(view.indicators.y) == 1):
            if view.indicators.y[0].display is None:
                view.indicators.y[0].display = {"color": "#B13507"}
            else:
                view.indicators.y[0].display["color"] = "#B13507"

    c.set_global_config(
        {
            "title": lambda view: _set_title(view, choice_names),
            "subtitle": lambda view: _set_subtitle(view),
            "timelineMinTime": 1946,
            "note": lambda view: _set_note(view),
            "hideRelativeToggle": lambda view: view.d.conflict_type != "state_based_stacked",
            "hideFacetControl": False,
            "includedEntityNames": region_names,
        }
    )
    c.set_global_metadata(
        {
            "description_short": (
                lambda view: _set_subtitle(view)
                if ((view.d.conflict_type == "state_based_stacked") or (view.d.estimate == "best_ci"))
                else None
            ),
            "description_key": lambda view: _set_description_key(view, tb_ucdp=tb_ucdp, tb_up=tb_up),
        }
    )


def _set_description_key(view, tb_ucdp, tb_up):
    if (view.d.conflict_type == "state_based_stacked") or (view.d.estimate == "best_ci"):
        if view.d.indicator == "deaths":
            keys = tb_up["number_deaths_ongoing_conflicts__conflict_type_state_based"].metadata.description_key
        elif view.d.indicator == "death_rate":
            keys = tb_up[
                "number_deaths_ongoing_conflicts_per_capita__conflict_type_state_based"
            ].metadata.description_key
        elif view.d.indicator == "wars_ongoing":
            keys = tb_ucdp["number_ongoing_conflicts__conflict_type_all"].metadata.description_key
        elif view.d.indicator == "wars_ongoing_country_rate":
            keys = tb_ucdp["number_ongoing_conflicts_per_country__conflict_type_all"].metadata.description_key
        else:
            raise ValueError(f"Unknown indicator: {view.d.indicator}")

        # return
        if view.d.estimate == "best_ci":
            assert keys[-1].startswith('We show here the "best" death')
            keys = keys[:-1]  # + [None]
        return keys
    return None


def _set_title(view, choice_names):
    conflict_name = _set_title_ending(view, choice_names)
    if view.d.indicator == "deaths":
        return f"Deaths in {conflict_name}"
    elif view.d.indicator == "death_rate":
        return f"Death rate in {conflict_name}"
    elif view.d.indicator == "wars_ongoing":
        return f"Number of {conflict_name}"
    elif view.d.indicator == "wars_ongoing_country_rate":
        return f"Rate of {conflict_name}"
    else:
        raise ValueError(f"Unknown indicator: {view.d.indicator}")


def _set_subtitle(view):
    dods = _set_dods(view)
    subtitle_deaths = f"Reported deaths of combatants and civilians due to fighting{{placeholder}} {dods} conflicts. Deaths due to disease and starvation resulting from the conflict are not included."

    if view.d.indicator == "deaths":
        return subtitle_deaths.format(placeholder=" in")
    elif view.d.indicator == "death_rate":
        return subtitle_deaths.format(placeholder=", per 100,000 people. Included are")
    elif view.d.indicator == "wars_ongoing":
        return f"Included are {dods} conflicts."
    elif view.d.indicator == "wars_ongoing_country_rate":
        return f"The number of conflicts divided by the number of all states. This accounts for the changing number of states over time. Included are {dods} conflicts."
    else:
        raise ValueError(f"Unknown indicator: {view.d.indicator}")


def edit_indicator_displays(view):
    """Edit FAUST estimates for confidence intervals."""
    if view.d.estimate == "best_ci":
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


def _set_dods(view):
    # DoD
    if view.d.conflict_type in ("state-based", "state_based_stacked"):
        dods = (
            "[interstate](#dod:interstate-ucdp), [civil](#dod:intrastate-ucdp), and [colonial](#dod:extrasystemic-ucdp)"
        )
    elif view.d.conflict_type == "interstate":
        dods = "[interstate conflicts](#dod:interstate-ucdp)"
    elif view.d.conflict_type == "intrastate (internationalized)":
        dods = "[foreign-backed civil conflicts](#dod:intrastate-ucdp)"
    elif view.d.conflict_type == "intrastate (non-internationalized)":
        dods = "[domestic civil conflicts](#dod:intrastate-ucdp)"
    elif view.d.conflict_type == "extrasystemic":
        dods = "[colonial conflicts](#dod:extrasystemic-ucdp)"
    else:
        raise ValueError(f"Unknown conflict type: {view.d.conflict_type}")

    return dods


def _set_title_ending(view, choice_names):
    title = "all conflicts involving states"
    if view.d.conflict_type not in {"state-based", "state_based_stacked"}:
        title = choice_names.get(view.d.conflict_type).lower()

    if view.d.conflict_type == "state_based_stacked":
        title += " by type"
    return title


def _set_note(view):
    if view.d.indicator in ("wars_ongoing", "wars_ongoing_country_rate"):
        return "Some conflicts affect several regions. The sum across all regions can therefore be higher than the total number."
    if view.d.indicator in ("deaths", "death_rate") and (view.d.estimate == "best_ci"):
        return '"Best" estimates as identified by UCDP and PRIO.'
