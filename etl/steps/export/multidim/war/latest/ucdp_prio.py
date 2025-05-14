"""I've implemented a simple version of create_collections with support for multiple tables. We should move this somewhere so others can use, or just replace the behavior of paths.create_collection."""

from etl.collection import combine_collections
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ## UCDP/PRIO
    ds_up = paths.load_dataset("ucdp_prio")
    tb_up = ds_up.read("ucdp_prio", load_data=False)
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
    c = create_collection_multiple_tables(
        tbs=[tb_up, tb_ucdp],
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
                "choice_new_slug": "state_based_stacked",
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
                "choice_new_slug": "best_ci",
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


def create_collection_multiple_tables(tbs, config, indicator_names, dimensions, common_view_config):
    """This function should be migrated somewhere for everyone to use.

    It is trying to support creation of Collections based on multiple tables.

    Ideas:
        - tbs: List[Table]
        - indicator_names:
            List[List[str]]: Each element contains the indicator names for the corresponding table. Length of indicator_names should match the length of tbs.
            List[str]: A single list of indicator names. It should be applicable to all tables (i.e. all tables should have these indicator names).
        - config: ? unclear
        - dimensions: Similar behavior as indicator_names.
        - common_view_config: Similar behavior as indicator_names.

        Possibly more arguments needed to match create_collection and combine_collections.
    """
    # Create collections
    collections = []
    for tb, names in zip(tbs, indicator_names):
        c_ = paths.create_collection(
            config=config,
            tb=tb,
            indicator_names=names,
            dimensions=dimensions,
            common_view_config=common_view_config,
        )
        collections.append(c_)

    c = combine_collections(
        collections=collections,
        collection_name=paths.short_name,  # Optional: add option to force a certain short_name
        config=config,
    )

    return c


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


def edit_faust(c):
    """Edit FAUST of views: Chart and indicator-level."""
    choice_names = c.get_choice_names("conflict_type")
    for view in c.views:
        # Edit title and subtitle in charts
        # edit_view_title(view, choice_names)

        # Edit FAUST in charts with CI (color, display names). Indicator-level.
        edit_view_display_estimates_ci(view)


def edit_view_title(view, conflict_renames):
    """Edit FAUST titles and subtitles."""
    # Get conflict type name
    conflict_name = "state-based conflicts"
    if view.dimensions["conflict_type"] not in {"state-based", "state_based_stacked"}:
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
            # "subtitle": "Included are [interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars that were ongoing that year.",
        }
    elif view.dimensions["indicator"] == "wars_ongoing_country_rate":
        view.config = {
            **(view.config or {}),
            "title": f"Rate of {conflict_name}",
            # "subtitle": "The number of wars divided by the number of all states. This accounts for the changing number of states over time. Included are [interstate](#dod:interstate-war-mars) and [civil](#dod:civil-war-mars) wars that were ongoing that year.",
        }


def edit_view_display_estimates_ci(view):
    """Edit FAUST estimates for confidence intervals."""
    print(view.dimensions["estimate"])
    if view.dimensions["estimate"] == "best_ci":
        print(1)
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
                    "name": "Beest estimate",
                    "color": "#B13507",
                }
    else:
        print(0)
