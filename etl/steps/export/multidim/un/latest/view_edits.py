"""
This module is a bit complex, and we should work towards simplifying this.

But in a nutshell, it contains utils to tweak the views of a collection based on indicator names and dimensions. Note that it is specific to the population and demography collection.

Example: We want to tweak the view title when we are looking at the population indicator for sex="female", age="all" and variant="medium".

EXTRA: There is also the file map_brackets.yml, which contains relevant information for tweaking the map brackets of the views.
"""

import re
from pathlib import Path
from typing import Any

import yaml

from etl.collection.model import Collection

# Last year for which UN WPP publishes estimates (projections start the year after).
# Used as the default `map.time` on grouped projection views so the map opens
# on the final estimate rather than deep into the 2024–2100 projection range.
# Bump this when a new UN WPP edition lands.
UN_WPP_ESTIMATES_LAST_YEAR = 2023


class ViewEditor:
    """Edit views.

    There is a function `edit_views_*` for each different sub-collection.

    NOTE: There might be redundancy in functions.
    """

    def __init__(self, map_brackets_yaml: str | Path):
        self.map_brackets = self._load_map_brackets(map_brackets_yaml)
        # Cache of table_name -> grapher table (metadata only), shared across all
        # edit_views_* calls that resolve grouped-view title/subtitle.
        self._grapher_tables: dict[str, Any] = {}

    def _load_map_brackets(self, map_brackets_yaml: str | Path):
        """Load map brackets.

        sex="female" and "male" are filled based on the "all" brackets.
        """
        with open(map_brackets_yaml) as f:
            dix = yaml.safe_load(f)

        for indicator, values in dix.items():
            if indicator in {
                "population",
                "population_change",
                "deaths",
                "dependency_ratio",
                "dependency_ratio",
                "life_expectancy",
                "infant_deaths",
                "child_deaths",
            }:
                for _, sexes in values["dimensions"].items():
                    if indicator == "population_change":
                        new_brackets = _get_brackets_sex(sexes["all"], 1001)
                    elif indicator == "deaths":
                        new_brackets = _get_brackets_sex(sexes["all"], 300)
                    elif indicator in {"dependency_ratio", "life_expectancy"}:
                        new_brackets = sexes["all"]
                    else:
                        new_brackets = _get_brackets_sex(sexes["all"])

                    if "female" not in sexes:
                        sexes["female"] = new_brackets
                    if "male" not in sexes:
                        sexes["male"] = new_brackets

        return dix

    def _get_color_scheme(self, indicator_name: str):
        if "color" not in self.map_brackets[indicator_name]:
            raise ValueError("Color scheme not specified!")
        return self.map_brackets[indicator_name]["color"]

    def _get_map_brackets(
        self,
        indicator_name,
        age,
        sex,
    ):
        if indicator_name in self.map_brackets:
            if age in self.map_brackets[indicator_name]["dimensions"]:
                assert sex in self.map_brackets[indicator_name]["dimensions"][age]
                return self.map_brackets[indicator_name]["dimensions"][age][sex]
            else:
                raise ValueError(f"Map brackets not specified for {age}")
        else:
            raise ValueError(f"Map brackets not specified for {indicator_name}")

    def _set_map_colorscale_on_view(self, age, sex, indicator_name, view):
        """Set map color scale for a view at (age, sex) using Explorer-style bracket strings.

        Writes into ``view.config.map.colorScale`` (grapher grammar). The Explorer
        equivalent set ``colorScaleScheme`` / ``colorScaleNumericBins`` directly on
        each y-indicator's ``display`` — that only works with the Explorer schema,
        which accepts flat colorScale fields. MDIM strictly validates ``display``
        against the grapher display schema, so we set the per-view map colorScale
        object instead.
        """
        brackets_str, min_value = self._brackets_and_min(indicator_name, age, sex)
        color_scale: dict[str, Any] = {
            "baseColorScheme": self._get_color_scheme(indicator_name),
            "binningStrategy": "manual",
            "customNumericValues": _parse_brackets(brackets_str),
        }
        if min_value is not None:
            # grapher colorScale uses `minValue` (Explorer grammar was `customNumericMinValue`).
            color_scale["minValue"] = min_value

        if view.config is None:
            view.config = {}
        map_cfg = view.config.get("map") or {}
        existing = map_cfg.get("colorScale") or {}
        map_cfg["colorScale"] = {**existing, **color_scale}
        view.config["map"] = map_cfg

    def _brackets_and_min(self, indicator_name, age, sex):
        brackets = self._get_map_brackets(indicator_name, age, sex)
        if isinstance(brackets, dict):
            return brackets["brackets"], brackets.get("min_value")
        return brackets, None

    def edit_views_pop(self, collection: Collection, ds_grapher=None):
        """Edit population collection views.

        Views may have one y-indicator (estimates-only) or two (estimates + projection
        variant, grouped by `create_with_grouped_projections`). The map colorScale
        is set once per view; the legend name is still set per y-indicator.

        When ``ds_grapher`` is provided, grouped projection views also get their
        ``title`` / ``subtitle`` set explicitly from the indicator metadata — see
        ``_apply_grouped_view_metadata``.
        """
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]

            # Edit display
            assert v.indicators.y is not None

            # Map colorscheme: per-view (map is shared across y-indicators).
            self._set_map_colorscale_on_view(age, sex, indicator_name, v)

            # Legend name: per-indicator (each y-series labeled by its age bucket).
            for indicator in v.indicators.y:
                if indicator_name != "population_density":
                    if indicator.display is None:
                        indicator.display = {}
                    indicator.display["name"] = f"{age} years"

            self._apply_grouped_view_metadata(v, ds_grapher)

    def _apply_grouped_view_metadata(self, view, ds_grapher):
        """Populate ``view.config`` title/subtitle for grouped projection views.

        Shared by every ``edit_views_*`` that operates on a collection produced by
        ``create_with_grouped_projections``. Grouped views have two y-indicators
        — **projection variant first, estimates second** — that disagree on their
        ``title`` field (so grapher falls back to the dataset origin like
        "World Population Prospects") and on their ``subtitle`` (so the
        projection-scenario note gets dropped). We fix both:

        - ``title`` copied from the estimates indicator's ``title_public`` (both
          indicators carry the same value for these collections);
        - ``subtitle`` copied from the projection indicator's
          ``grapher_config.subtitle``, which already includes the "Future
          projections are based on …" sentence plus any indicator-specific base
          text.

        The projection-first ordering is load-bearing for the map tab: grapher's
        default ``map.columnSlug`` is y[0], and when y[0] is a projection column
        the grapher's ``projectionColumnInfoBySlug`` auto-matches it with the
        historical column at y[1] and the map shows the full 1950–2100 series
        (historical + projection combined). Reversing the order would limit the
        map to 1950–2023.

        No-op when ``ds_grapher`` is ``None`` or the view is not a grouped
        2-indicator view. The table name is extracted from the indicator's
        catalog path, so this works for any ``un_wpp`` table.
        """
        if ds_grapher is None or view.indicators.y is None or len(view.indicators.y) != 2:
            return

        projection_path = view.indicators.y[0].catalogPath
        estimates_path = view.indicators.y[1].catalogPath

        projection_col = projection_path.split("#")[-1]
        estimates_col = estimates_path.split("#")[-1]

        # Invariant: create_with_grouped_projections concatenates [variant, estimates].
        # If this ever flips, the subtitle we copy (projection's) would come from the
        # wrong indicator, and the map tab would lose its auto-combined full range.
        assert "variant_estimates" not in projection_col, (
            f"Expected first y-indicator to be a projection variant, got {projection_col!r}"
        )
        assert "variant_estimates" in estimates_col, (
            f"Expected second y-indicator to be the estimates variant, got {estimates_col!r}"
        )

        # Extract table from "grapher/un/.../un_wpp/<table>#<column>".
        table_name = estimates_path.split("#")[0].split("/")[-1]
        tb = self._get_grapher_table(ds_grapher, table_name)
        if tb is None or estimates_col not in tb.columns or projection_col not in tb.columns:
            return

        estimates_meta = tb[estimates_col].metadata
        projection_meta = tb[projection_col].metadata

        title = (
            estimates_meta.presentation.title_public
            if estimates_meta.presentation and estimates_meta.presentation.title_public
            else None
        )
        subtitle = None
        if projection_meta.presentation and projection_meta.presentation.grapher_config:
            subtitle = projection_meta.presentation.grapher_config.get("subtitle")

        if view.config is None:
            view.config = {}
        if title:
            view.config["title"] = title.strip()
        if subtitle:
            view.config["subtitle"] = subtitle.strip()
        # Open the map tab at the last estimate year (see `UN_WPP_ESTIMATES_LAST_YEAR`).
        # Otherwise grapher's default "latest" lands on the far end of the projection
        # range (2100). In MDIM grapher config, this is set via `map.time`
        # (the Explorer-grammar equivalent was the flat `mapTargetTime` field).
        map_cfg = view.config.get("map") or {}
        map_cfg["time"] = UN_WPP_ESTIMATES_LAST_YEAR
        view.config["map"] = map_cfg

    def _get_grapher_table(self, ds_grapher, table_name):
        """Cached read of a grapher-channel table (metadata only)."""
        if table_name not in self._grapher_tables:
            self._grapher_tables[table_name] = ds_grapher.read(table_name, load_data=False)
        return self._grapher_tables[table_name]

    def edit_views_manual(self, collection: Collection, ds_grapher=None):
        """Edit views of the manual sub-collection.

        Covers two kinds of view that can't be produced by
        ``create_with_grouped_projections``:
          - multi-indicator stacked views (``age_structure``, ``population_broad``),
            rendered as stacked bar/area with several y-indicators per view;
          - display-only indicator slugs (``child_deaths`` / ``infant_deaths`` /
            ``infant_mortality_rate`` / ``child_mortality_rate``) whose projection
            views are declared directly in the YAML as two y-indicators (estimates
            + projection variant) so the grapher renders the solid-to-dashed
            transition.
        """
        pattern = re.compile(r".*/population#population__sex_(?:[a-z]+)__age_([\d_+(plus)]+)__variant_(?:[a-z]+)$")
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            variant = v.dimensions.get("variant", "estimates")

            # Append projection note for multi-indicator stacked views (age_structure,
            # population_broad). Those have their subtitle set in the YAML common_views
            # and are NOT 2-indicator grouped views, so the generic
            # `_apply_grouped_view_metadata` helper doesn't touch them.
            if variant != "estimates" and v.config and v.config.get("subtitle"):
                v.config["subtitle"] = (
                    v.config["subtitle"]
                    + f" Future projections are based on the [UN's {variant} scenario](#dod:un-projection-scenarios)."
                )

            if indicator_name in {"age_structure", "population_broad"}:
                # Edit display
                assert v.indicators.y is not None
                for indicator in v.indicators.y:
                    match = pattern.match(indicator.catalogPath)
                    if not match:
                        raise ValueError(f"Unexpected indicator path: {indicator.catalogPath}")
                    age = match.group(1).replace("_", "-").replace("plus", "+")
                    display: dict[str, Any] = {"name": f"{age} years"}
                    if indicator_name == "age_structure":
                        display["roundingMode"] = "decimalPlaces"
                        display["numDecimalPlaces"] = 1
                    if indicator.display is None:
                        indicator.display = display
                    else:
                        indicator.display = {**indicator.display, **display}
            elif indicator_name in {"child_deaths", "infant_deaths"}:
                # Map colorscheme: view-level, driven by the view's age/sex dims.
                assert v.indicators.y is not None
                assert 1 <= len(v.indicators.y) <= 2, (
                    f"Expected 1 (estimates-only) or 2 (grouped) y-indicators for {indicator_name}, "
                    f"got {len(v.indicators.y)}"
                )

                sex = v.dimensions["sex"]
                age = v.dimensions["age"]
                self._set_map_colorscale_on_view(age, sex, indicator_name, v)

            # For grouped 2-indicator projection views declared in YAML (infant_deaths,
            # child_deaths, infant_mortality_rate, child_mortality_rate), copy
            # title_public + projection subtitle from grapher metadata onto the view.
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_fr(self, collection, ds_grapher=None):
        """Edit fertility rate collection views."""
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator_name == "fertility_rate":
                self._set_map_colorscale_on_view(age, sex, indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_b(self, collection, ds_grapher=None):
        """Edit births collection views."""
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            sex = v.dimensions["sex"]
            self._set_map_colorscale_on_view(age, sex, indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_ma(self, collection, ds_grapher=None):
        """Edit median age collection views."""
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            sex = v.dimensions["sex"]
            if indicator_name == "median_age":
                self._set_map_colorscale_on_view(age, sex, indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_mig(self, collection, ds_grapher=None):
        """Edit migration collection views."""
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            sex = v.dimensions["sex"]
            if indicator_name in {"net_migration", "net_migration_rate"}:
                self._set_map_colorscale_on_view(age, sex, indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_deaths(self, collection, ds_grapher=None):
        """Edit deaths collection views."""
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            self._set_map_colorscale_on_view(age, sex, indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_le(self, collection, ds_grapher=None):
        """Edit life expectancy collection views."""
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator_name == "life_expectancy":
                # In MDIM grapher config, the y-axis minimum lives on nested `yAxis.min`
                # (the Explorer-grammar equivalent was the flat `yAxisMin` field).
                if v.config is None:
                    v.config = {}
                y_axis = v.config.get("yAxis") or {}
                y_axis["min"] = int(age)
                v.config["yAxis"] = y_axis

            self._set_map_colorscale_on_view(age, sex, indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_sr(self, collection, ds_grapher=None):
        """Edit sex ratio collection views"""
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            if indicator_name == "sex_ratio":
                self._set_map_colorscale_on_view(age, "all", indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_dr(self, collection, ds_grapher=None):
        """Edit dependency ratio collection views."""
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            self._set_map_colorscale_on_view(age, sex, indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)

    def edit_views_rates(self, collection, ds_grapher=None):
        """Edit views for rate-style sub-collections with a single (sex=all, age=all) view.

        Used by ``collection_growth`` (growth_rate) and ``collection_natchange``
        (natural_change_rate).
        """
        for v in collection.views:
            indicator_name = v.dimensions["indicator"]
            assert v.indicators.y is not None
            self._set_map_colorscale_on_view("all", "all", indicator_name, v)
            self._apply_grouped_view_metadata(v, ds_grapher)


def _parse_brackets(brackets_str: str) -> list[float]:
    """Parse an Explorer-grammar bracket string into a list of floats for MDIM.

    Explorer stores bracket boundaries as a semicolon-separated string (e.g.
    ``"0.1;0.2;0.5;1;2;5;10;1"``), and a trailing ``1`` is a legacy log-scale
    flag rather than a bin boundary — see ``_get_brackets_sex`` which strips it
    the same way. MDIM's ``colorScale.customNumericValues`` expects an array of
    numbers, so we drop that flag and cast the remaining values.
    """
    parts = brackets_str.split(";")
    if parts and parts[-1] == "1":
        parts = parts[:-1]
    return [float(p) if "." in p else int(p) for p in parts]


def _get_brackets_sex(brackets, threshold=None):
    """Edit the map brackets of indicators for each sex.

    E.g. for each sex, have the same map brackets as with total but halved, since we are dealing with ~half population. The motivation for this is to avoid defining a large number of map brackets.


    Some conversions:
        - Only-positive:
            Log3: Drop the last digit, and optionally add a first value which is 1/3 (or 0.3) of the first value.
                Example: 1;3;10;30... or 3;10;30...
            Others: Just divide by 2 (except when 25000 or similar, where we divide by 2.5)
        - 0-centred, Log3/Log2/Log10: Drop the last digit, and optionally add smaller values around zero which are 1/3 (or 0.3) of the first value.
            Examples: -30;-10;-3;-1;0;1;3;10;30..., -100;-50;-20;-10;0;10;20;50;100..., -100;-10;0;10;100...

    TODO: add Log2 and Log10
    """

    def _scale_3(num):
        if num.startswith("3") or num.startswith("-3"):
            val = int(num) / 3
        else:
            val = 3 * int(num) / 10
        return str(int(val))

    def _scale_2(num):
        if num.startswith("5") or num.startswith("-5"):
            val = int(num) / 2.5
        else:
            val = int(num) / 2
        return str(int(val))

    def _scale_10(num):
        val = int(num) / 10
        return str(int(val))

    brackets = brackets.split(";")

    if brackets[-1] == "1":
        extra = [brackets[-1]]
        brackets = brackets[:-1]
    else:
        extra = []

    # Conversion when 1;3;10;30... or 3;10;30... (just drop last digit)
    if (int(brackets[1]) / int(brackets[0]) == 3) or (int(brackets[1]) / int(brackets[0]) == 10 / 3):
        new_brackets = brackets[:-1]
        bracket_first = _scale_3(brackets[0])
        if not ((threshold is not None) and (int(bracket_first) < threshold)):
            new_brackets = [bracket_first] + new_brackets

    # Conversion when -30;-10;-3;-1;0;1;3;10;30... (just drop last digit)
    elif -int(brackets[0]) == int(brackets[-1]):
        idx = brackets.index("0")
        lower = brackets[idx - 1]
        upper = brackets[idx + 1]
        if (int(brackets[1]) / int(brackets[0]) == 1 / 3) or (int(brackets[1]) / int(brackets[0]) == 0.3):
            mid_brackets = [_scale_3(lower), brackets[idx], _scale_3(upper)]
        elif int(brackets[1]) / int(brackets[0]) == 0.1:
            mid_brackets = [_scale_10(lower), brackets[idx], _scale_10(upper)]
        else:
            mid_brackets = [_scale_2(lower), brackets[idx], _scale_2(upper)]

        if (threshold is not None) and (int(mid_brackets[-1]) < threshold):
            new_brackets = brackets[:idx] + [brackets[idx]] + brackets[idx + 1 :]
        else:
            new_brackets = brackets[1:idx] + mid_brackets + brackets[idx + 1 : -1]

    # Conversion when 1->2->5->10... (divide by 2, replace 25 with 20)
    else:
        new_brackets = []
        for b in brackets:
            bb = _scale_2(b)
            new_brackets.append(str(bb))

    # Add last number
    new_brackets = new_brackets + extra

    # Convert brackets as string
    new_brackets = ";".join(new_brackets)
    return new_brackets
