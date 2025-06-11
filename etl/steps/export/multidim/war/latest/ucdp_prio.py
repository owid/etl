"""I've implemented a simple version of create_collections with support for multiple tables. We should move this somewhere so others can use, or just replace the behavior of paths.create_collection."""

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
    c = paths.create_collection_v2(
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
        ]
    )

    # Drop views
    c.drop_views(
        [
            {"estimate": ["low", "high"]},
        ]
    )

    # Edit FAUST
    edit_faust(c)

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


def edit_faust(c):
    """Edit FAUST of views: Chart and indicator-level."""
    choice_names = c.get_choice_names("conflict_type")
    for view in c.views:
        # Edit FAUST in charts with CI (color, display names). Indicator-level.
        edit_indicator_displays(view)

    c.edit_views(
        [
            {"config": {"timelineMinTime": 1946}},
            {"dimensions": {"indicator": "deaths"}, "config": {"title": "Deaths in {conflict_name}"}},
            {
                "dimensions": {"indicator": "death_rate"},
                "config": {"title": "Death rate in {conflict_name}"},
            },
            {
                "dimensions": {"indicator": "wars_ongoing"},
                "config": {"title": "Number of {conflict_name}"},
            },
            {
                "dimensions": {"indicator": "wars_ongoing_country_rate"},
                "config": {"title": "Rate of {conflict_name}"},
            },
        ],
        params={
            "conflict_name": lambda view: _get_conflict_type(view, choice_names),
        },
    )


def edit_indicator_displays(view):
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


def _get_conflict_type(view, choice_names):
    conflict_name = "state-based conflicts"
    if view.dimensions["conflict_type"] not in {"state-based", "state_based_stacked"}:
        conflict_name = choice_names.get(view.dimensions["conflict_type"]).lower()
    return conflict_name
