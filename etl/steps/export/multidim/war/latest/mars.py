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

# Common config
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
    },
    "entityType": "region",
    "entityTypePlural": "regions",
    "chartTypes": ["StackedBar"],
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("mars")
    tb = ds.read("mars", load_data=False)

    # Adjust dimension metadata
    tb = adjust_dimensions(tb)

    # Create MDIM
    c = paths.create_collection(
        config=config,
        short_name="mars",
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
        common_view_config=COMMON_CONFIG,
    )

    # Edit indicator-level display settings
    for view in c.views:
        _edit_indicator_display(view)

    # Group certain views together: used to create StackedBar charts
    c.group_views(
        groups=[
            {
                "dimension": "conflict_type",
                "choices": ["civil war", "others (non-civil)"],
                "choice_new_slug": "all_stacked",
                "view_config": COMMON_CONFIG
                | {
                    "selectedFacetStrategy": "entity",
                    "chartTypes": ["StackedBar"],
                    "hasMapTab": False,
                },
                "view_metadata": {
                    "description_short": lambda view: _set_subtitle(view),
                    "description_key": lambda view: _set_description_key(view, tb),
                },
                # "overwrite_dimension_choice": True,
            },
            {
                "dimension": "estimate",
                "choices": ["low", "high"],
                "choice_new_slug": "low_high",
                "view_config": COMMON_CONFIG
                | {
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "chartTypes": ["LineChart"],
                },
                "view_metadata": {
                    "description_short": lambda view: _set_subtitle(view),
                    "presentation": {
                        "title_public": lambda view: f"{_set_title(view)}, low and high estimates",
                    },
                },
            },
        ]
    )

    # Remove certain views
    c.drop_views(
        [
            # {"conflict_type": ["civil war", "others (non-civil)"]},
            {"estimate": ["high"]},
        ]
    )

    # Edit FAUST
    c.set_global_config(
        {
            "timelineMinTime": 1800,
            "title": lambda view: _set_title(view),
            "subtitle": lambda view: _set_subtitle(view),
            "hideRelativeToggle": lambda view: (view.d.conflict_type != "all_stacked"),
            "hideFacetControl": False,
        }
    )

    for view in c.views:
        if view.d.estimate == "low_high":
            assert view.indicators.y is not None
            for indicator in view.indicators.y:
                assert indicator.display is not None
                if view.d.conflict_type == "civil war":
                    color_low = "#5C1B04"
                    color_high = "#CF8063"
                elif view.d.conflict_type == "others (non-civil)":
                    color_low = "#12213C"
                    color_high = "#748AB0"
                elif view.d.conflict_type == "all":
                    color_low = "#2F1146"
                    color_high = "#B084D1"
                else:
                    raise ValueError(f"Unknown conflict type {view.d.conflict_type}")

                if "_low_" in indicator.catalogPath:
                    indicator.display = {
                        "name": "Low estimate",
                        "color": color_low,
                    }
                elif "_high_" in indicator.catalogPath:
                    indicator.display = {
                        "name": "High estimate",
                        "color": color_high,
                    }

        # Set color to red if there is only one line in the chart
        if (view.indicators.y is not None) and (len(view.indicators.y) == 1):
            if view.indicators.y[0].display is None:
                view.indicators.y[0].display = {"color": "#B13507"}
            else:
                view.indicators.y[0].display["color"] = "#B13507"
    # Save & upload
    c.save()


def _set_description_key(view, tb):
    if view.d.indicator in ("deaths", "death_rate"):
        column = "number_deaths_ongoing_conflicts_high__conflict_type_all"
    elif view.d.indicator in ("wars_ongoing", "wars_ongoing_country_rate"):
        column = "number_ongoing_conflicts__conflict_type_all"
    else:
        return []
    return tb[column].m.description_key


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


def _set_title(view):
    conflict_type = get_conflict_type(view.d.conflict_type)
    if view.d.indicator == "deaths":
        return f"Deaths in {conflict_type}"
    elif view.d.indicator == "death_rate":
        return f"Death rate in {conflict_type}"
    elif view.d.indicator == "wars_ongoing":
        return f"Number of {conflict_type}"
    elif view.d.indicator == "wars_ongoing_country_rate":
        return f"Rate of {conflict_type}"
    else:
        raise ValueError(f"Unknown indicator {view.d.indicator}")


def _set_subtitle(view):
    dods = get_dods(view.d.conflict_type)
    if view.d.indicator == "deaths":
        return f"Deaths of combatants due to fighting in {dods}. Civilian deaths and deaths of combatants due to disease and starvation resulting from the war are not included."
    elif view.d.indicator == "death_rate":
        return f"Deaths of combatants due to fighting, per 100,000 people. Included are {dods}. Civilian deaths and deaths of combatants due to disease and starvation resulting from the war are not included."
    elif view.d.indicator == "wars_ongoing":
        return f"Included are {dods}."
    elif view.d.indicator == "wars_ongoing_country_rate":
        return f"The number of conflicts divided by the number of all states. This accounts for the changing number of states over time. Included are {dods} wars."
    else:
        raise ValueError(f"Unknown indicator {view.d.indicator}")


def get_conflict_type(ctype):
    if ctype == "civil war":
        return "civil wars"
    elif ctype == "others (non-civil)":
        return "interstate wars"
    elif ctype == "all":
        return "wars"
    elif ctype == "all_stacked":
        return "wars by type"
    else:
        raise ValueError(f"Unknown conflict type {ctype}")


def get_dods(ctype):
    if ctype in ("all", "all_stacked"):
        return "[interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars"
    elif ctype == "civil war":
        return "[civil wars](#dod:civil-war-mars)"
    elif ctype == "others (non-civil)":
        return "[interstate wars](#dod:interstate-war-mars)"
    else:
        raise ValueError(f"Unknown conflict type {ctype}")


def _edit_indicator_display(view):
    if view.d.conflict_type == "civil war":
        assert view.indicators.y is not None
        view.indicators.y[0].display = {
            "name": "Civil wars",
            "color": "#B13507",
        }
    elif view.d.conflict_type == "others (non-civil)":
        assert view.indicators.y is not None
        view.indicators.y[0].display = {
            "name": "Interstate wars",
            "color": "#4C6A9C",
        }
    elif view.d.conflict_type == "all":
        assert view.indicators.y is not None
        view.indicators.y[0].display = {
            "name": "All wars",
            "color": "#6D3E91",
        }
    #     if view.dimensions["estimate"] == "high":
    #         view.indicators.y[0].display["color"] = "#561802"
    #     elif view.dimensions["estimate"] == "low":
    #         view.indicators.y[0].display["color"] = "#B13507"
