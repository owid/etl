"""Poverty multidim from the World Bank Poverty and Inequality Platform."""

from copy import deepcopy

from etl.helpers import PathFinder
from etl.viz import filter_columns_by_dimension_choices

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year
PPP_YEAR = 2021

# Define indicators to use
INDICATORS = [
    "headcount_ratio",
    "headcount",
]

# NOTE: Update lines when prices change
DIMENSIONS_CONFIG = {
    "poverty_line": ["100", "300", "420", "830", "1000", "2000", "3000", "4000"],
    "table": ["Income or consumption consolidated", "Income with spells", "Consumption with spells"],
    "survey_comparability": "*",
}

# Settings every view gets, grouped ones included (they are created after `common_views` would apply).
ALL_VIEWS_CONFIG = {
    "originUrl": "ourworldindata.org/poverty",
}

# Poverty lines shown in the views that combine several of them (defined in the config).
# NOTE: Update lines when prices change
POVERTY_LINES_COMBINED = {
    "multiple_lines": ["300", "420", "830", "3000"],
    "distribution": ["300", "420", "830", "1000"],
}

# Combined views exist only in the "No spells" variant, with no "show breaks in the data" toggle. So two of the
# garden bullets are edited: the show-breaks bullet is dropped, and the income/consumption bullet loses its "you can
# see the original points via breaks" tail. The OLD_* constants mirror the garden texts verbatim; the assertions
# below catch drift in the source.
OLD_SHOW_BREAKS = "The exact surveys that countries run can change over time, creating breaks across which data points are less comparable. The World Bank provides an indicator of where these breaks occur, and this chart gives the option to show the breaks."
OLD_INCOME_CONSUMPTION = "For a small number of countries and years, the data source provides two estimates: one measuring households' income and one measuring consumption. To show a single series over time, we keep only one of these observations. You can see which measure each data point corresponds to by selecting the option to show breaks in the data."
NEW_INCOME_CONSUMPTION_NO_BREAKS = "For a small number of countries and years, the data source provides two estimates: one measuring households' income and one measuring consumption. To show a single series over time, we keep only one of these observations."


def run() -> None:
    #
    # Load inputs.
    #
    # Default chart config
    config = paths.load_config()

    # Load grapher dataset (only metadata, which is much faster).
    ds = paths.load_dataset("world_bank_pip")
    tb = ds.read("poverty", load_data=False)

    # Keep only indicators for a specific PPP year, and drop that dimension.
    tb = filter_columns_by_dimension_choices(tb, {"ppp_version": PPP_YEAR})

    # Remove dimensions that are not needed.
    for column in tb.columns:
        dimensions = tb[column].metadata.dimensions
        if dimensions:
            for dimension in ["welfare_type"]:
                dimensions.pop(dimension, None)

    # Get all survey_comparability values except "No spells" for spell views, sorted so that the series keep the
    # same order in every build.
    survey_comp_values = set()
    for col in tb.columns:
        if tb[col].metadata.dimensions and "survey_comparability" in tb[col].metadata.dimensions:
            survey_comp_values.add(tb[col].metadata.dimensions["survey_comparability"])
    survey_comp_spells = sorted(
        (v for v in survey_comp_values if v != "No spells"), key=lambda v: (len(str(v)), str(v))
    )

    #
    # Create chart object
    #
    c = paths.create_chart(
        config=config,
        short_name="poverty_pip",
        tb=tb,
        indicator_names=INDICATORS,
        dimensions=DIMENSIONS_CONFIG,
    )

    # First, group survey_comparability (this must happen first)
    c.group_views(
        groups=[
            {
                "dimension": "survey_comparability",
                "choices": survey_comp_spells,
                "choice_new_slug": "Spells",
                "replace": True,
                "view_config": {
                    "hideRelativeToggle": False,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                },
            },
        ],
    )

    # Then, group the table dimension
    c.group_views(
        groups=[
            {
                "dimension": "table",
                "choices": ["Income with spells", "Consumption with spells"],
                "choice_new_slug": "Income or consumption consolidated",
                "replace": True,
                "view_config": {
                    "hideRelativeToggle": False,
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["LineChart"],
                },
            },
        ],
    )

    # Add the settings shared by all views.
    for view in c.views:
        view.config = {**deepcopy(ALL_VIEWS_CONFIG), **(view.config or {})}

    # Build the key information of the views that combine several poverty lines.
    for view in c.views:
        poverty_lines = POVERTY_LINES_COMBINED.get(view.dimensions["poverty_line"])
        if poverty_lines:
            columns = [
                _column_name(indicator=view.dimensions["indicator"], poverty_line=line) for line in poverty_lines
            ]
            view.metadata = view.metadata or {}
            view.metadata["description_key"] = _combined_description_key(tb, columns)

    #
    # Save & upload
    #
    c.save()


def _column_name(indicator: str, poverty_line: str) -> str:
    """Return the column of the consolidated series of an indicator at a poverty line."""
    return f"{indicator}__ppp_version_{PPP_YEAR}__poverty_line_{poverty_line}__welfare_type_income_or_consumption__table_income_or_consumption_consolidated__survey_comparability_no_spells"


def _combined_description_key(tb, columns: list[str]) -> list[str]:
    """Return the key information of a view that shows several poverty lines.

    The bullets that differ between poverty lines come first (e.g. what the $3 and $4.20 lines represent), in the
    order of the lines. The bullets shared by all lines follow once, edited for a view without a breaks toggle.
    """
    bullets_by_column = [_description_key_bullets(tb, column) for column in columns]
    for column, bullets in zip(columns, bullets_by_column):
        assert OLD_SHOW_BREAKS in bullets and OLD_INCOME_CONSUMPTION in bullets, (
            f"Breaks bullets not found in {column}.description_key — garden text changed, update OLD_SHOW_BREAKS/OLD_INCOME_CONSUMPTION."
        )

    # Split bullets between those shared by all poverty lines and those specific to a line.
    bullets_shared = [
        bullet for bullet in bullets_by_column[0] if all(bullet in bullets for bullets in bullets_by_column)
    ]
    bullets_by_line = [bullet for bullets in bullets_by_column for bullet in bullets if bullet not in bullets_shared]
    assert bullets_by_line, "No bullet specific to a poverty line found — garden text changed, review this function."

    # Edit the shared bullets that mention the option to show breaks.
    bullets_shared_edited = []
    for bullet in bullets_shared:
        if bullet == OLD_SHOW_BREAKS:
            continue
        if bullet == OLD_INCOME_CONSUMPTION:
            bullet = NEW_INCOME_CONSUMPTION_NO_BREAKS
        bullets_shared_edited.append(bullet)

    return bullets_by_line + bullets_shared_edited


def _description_key_bullets(tb, col_name: str) -> list[str]:
    """Return an indicator's description_key as a list of bullet strings.

    The grapher channel stores description_key as a single markdown string (bullets joined with "\\n- ") for some
    datasets and as a list for others. Normalize both to a list so bullets can be dropped or swapped individually.
    """
    assert col_name in tb.columns, f"Column {col_name} not found in the poverty table."
    dk = tb[col_name].metadata.description_key
    if dk is None:
        return []
    if not isinstance(dk, str):
        return list(dk)
    lines = [line.strip() for line in dk.split("\n") if line.strip()]
    if lines and all(line.startswith("- ") for line in lines):
        return [line[2:].strip() for line in lines]
    return [dk.strip()]
