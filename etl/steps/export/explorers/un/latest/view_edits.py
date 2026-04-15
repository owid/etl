"""
This module is a bit complex, and we should work towards simplifying this.

But in a nutshell, it contains utils to tweak the views of an explorer based on indicator names and dimensions. Note that it is specific to the population and demography explorer.

Example: We want to tweak the explorer view title when we are looking at the population indicator for sex="female", age="all" and variant="medium".

EXTRA: There is also the file map_brackets.yml, which contains relevant information for tweaking the map brackets of the views.
"""

import re
from pathlib import Path
from typing import Any

import yaml

from etl.collection.explorer import Explorer


class ViewEditor:
    """Edit views.

    There is a function `edit_views_*` for each different explorer.

    NOTE: There might be redundancy in functions.
    """

    def __init__(self, map_brackets_yaml: str | Path):
        self.map_brackets = self._load_map_brackets(map_brackets_yaml)

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

    def _add_map_brackets_display(self, age, sex, indicator_name, indicator):
        """Add map bracket information for a specific indicator and dimensions (age, sex)."""
        display = {
            "colorScaleScheme": self._get_color_scheme(indicator_name),
        }

        brackets = self._get_map_brackets(indicator_name, age, sex)

        if isinstance(brackets, dict):
            display["colorScaleNumericBins"] = brackets["brackets"]
            display["colorScaleNumericMinValue"] = brackets["min_value"]
        else:
            display["colorScaleNumericBins"] = brackets

        if indicator.display is None:
            indicator.display = display
        else:
            indicator.display = {**indicator.display, **display}

    def edit_views_pop(self, explorer: Explorer, ds_grapher=None):
        """Edit population explorer views.

        Views may have one y-indicator (estimates-only) or two (estimates + projection
        variant, grouped by `create_with_grouped_projections`). The same display edits
        apply to every indicator in the view.

        When ``ds_grapher`` is provided, grouped projection views also get their
        ``title`` / ``subtitle`` set explicitly from the indicator metadata. Without
        this override, the grapher falls back to the dataset origin title
        ("World Population Prospects") because the two y-indicators have different
        ``title`` fields, and the projection subtitle is dropped because only the
        first indicator's (estimates, empty) subtitle is considered.
        """
        # Cache of table_name -> grapher table (metadata only) to avoid re-reading.
        tables_cache: dict[str, Any] = {}

        def _tb(table_name: str):
            if ds_grapher is None:
                return None
            if table_name not in tables_cache:
                tables_cache[table_name] = ds_grapher.read(table_name, load_data=False)
            return tables_cache[table_name]

        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            variant = v.dimensions.get("variant", "estimates")

            # Edit display
            assert v.indicators.y is not None

            for indicator in v.indicators.y:
                # Add map colorscheme
                self._add_map_brackets_display(age, sex, indicator_name, indicator)

                # Add legend name
                if indicator_name != "population_density":
                    if indicator.display is None:
                        indicator.display = {}
                    indicator.display["name"] = f"{age} years"

            # Set title/subtitle for grouped projection views (estimates + projection).
            # The single-indicator estimates views are handled correctly by grapher's
            # indicator-metadata fallback, so we leave them untouched.
            if variant != "estimates" and ds_grapher is not None and len(v.indicators.y) == 2:
                self._set_pop_grouped_view_title_subtitle(v, _tb)

    def _set_pop_grouped_view_title_subtitle(self, view, get_table):
        """Populate view.config title/subtitle from grapher-step indicator metadata.

        Expects ``view.indicators.y`` to be ``[estimates_indicator, projection_indicator]``
        (the order produced by ``create_with_grouped_projections``). Reads
        ``title_public`` (identical across variants) and the projection indicator's
        ``grapher_config.subtitle`` (which already includes the age-specific base text
        and the "Future projections..." sentence).
        """
        estimates_col = view.indicators.y[0].catalogPath.split("#")[-1]
        projection_col = view.indicators.y[1].catalogPath.split("#")[-1]

        # Sanity-check the indicator order: y[0] must be the estimates variant
        # (solid line) and y[1] the projection (dashed). If this invariant breaks,
        # ``create_with_grouped_projections`` changed its concatenation order and
        # the subtitle we pick here would be wrong (empty estimates subtitle would
        # overwrite the projection one).
        assert "variant_estimates" in estimates_col, (
            f"Expected first y-indicator to be the estimates variant, got {estimates_col!r}"
        )
        assert "variant_estimates" not in projection_col, (
            f"Expected second y-indicator to be a projection variant, got {projection_col!r}"
        )

        # population, population_change, and population_density all live in the
        # `population` table in grapher.
        tb = get_table("population")
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

    def edit_views_manual(self, explorer: Explorer):
        """Edit explorer views of manual explorer."""
        pattern = re.compile(r".*/population#population__sex_(?:[a-z]+)__age_([\d_+(plus)]+)__variant_(?:[a-z]+)$")
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]

            # Append projection text to subtitles for non-estimate views
            variant = v.dimensions.get("variant", "estimates")
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
                    display = {
                        "name": f"{age} years",
                    }
                    if indicator.display is None:
                        indicator.display = display
                    else:
                        indicator.display = {**indicator.display, **display}
            elif indicator_name in {"growth_rate", "natural_change_rate"}:
                # Edit display
                assert v.indicators.y is not None
                for indicator in v.indicators.y:
                    self._add_map_brackets_display("all", "all", indicator_name, indicator)
                    # display = {
                    #     "colorScaleScheme": "RdBu",
                    #     "colorScaleNumericBins": "-5;-2;-1;-0.5;0;0.5;1;2;5;1",
                    # }
                    # if indicator.display is None:
                    #     indicator.display = display
                    # else:
                    #     indicator.display = {**indicator.display, **display}
            elif indicator_name in {"child_deaths", "infant_deaths"}:
                # Edit display
                assert v.indicators.y is not None
                assert len(v.indicators.y) == 1

                # Get dimensions
                sex = v.dimensions["sex"]
                age = v.dimensions["age"]

                # Add map colorscheme
                self._add_map_brackets_display(age, sex, indicator_name, v.indicators.y[0])

    def edit_views_fr(self, explorer):
        """Edit fertility rate explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator_name == "fertility_rate":
                # Edit display
                assert v.indicators.y is not None
                for indicator in v.indicators.y:
                    self._add_map_brackets_display(
                        age,
                        sex,
                        indicator_name,
                        indicator,
                    )

    def edit_views_b(self, explorer):
        """Edit births explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            sex = v.dimensions["sex"]

            # Edit display
            assert v.indicators.y is not None
            for indicator in v.indicators.y:
                self._add_map_brackets_display(
                    age,
                    sex,
                    indicator_name,
                    indicator,
                )

    def edit_views_ma(self, explorer):
        """Edit median age explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            sex = v.dimensions["sex"]
            if indicator_name == "median_age":
                for indicator in v.indicators.y:
                    self._add_map_brackets_display(age, sex, indicator_name, indicator)

    def edit_views_mig(self, explorer):
        """Edit migration explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            sex = v.dimensions["sex"]
            if indicator_name in {"net_migration", "net_migration_rate"}:
                for indicator in v.indicators.y:
                    self._add_map_brackets_display(age, sex, indicator_name, indicator)

    def edit_views_deaths(self, explorer):
        """Edit deaths explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]

            for indicator in v.indicators.y:
                self._add_map_brackets_display(age, sex, indicator_name, indicator)

    def edit_views_le(self, explorer):
        """Edit life expectancy explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator_name == "life_expectancy":
                if v.config is None:
                    v.config = {"yAxisMin": int(age)}
                else:
                    v.config["yAxisMin"] = int(age)

            for indicator in v.indicators.y:
                self._add_map_brackets_display(age, sex, indicator_name, indicator)

    def edit_views_sr(self, explorer):
        """Edit sex ratio explorer views"""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            if indicator_name == "sex_ratio":
                for indicator in v.indicators.y:
                    self._add_map_brackets_display(age, "all", indicator_name, indicator)

    def edit_views_dr(self, explorer):
        """Edit dependency ratio explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            for indicator in v.indicators.y:
                self._add_map_brackets_display(age, sex, indicator_name, indicator)


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
