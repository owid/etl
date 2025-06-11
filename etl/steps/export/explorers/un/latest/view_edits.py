"""
This module is a bit complex, and we should work towards simplifying this.

But in a nutshell, it contains utils to tweak the views of an explorer based on indicator names and dimensions. Note that it is specific to the population and demography explorer.

Example: We want to tweak the explorer view title when we are looking at the population indicator for sex="female", age="all" and variant="medium".

EXTRA: There is also the file map_brackets.yml, which contains relevant information for tweaking the map brackets of the views.
"""

import re
from pathlib import Path
from typing import Union

import yaml

from etl.collection.explorer import Explorer


class ViewEditor:
    """Edit views.

    There is a function `edit_views_*` for each different explorer.

    NOTE: There might be redundancy in functions.
    """

    def __init__(self, map_brackets_yaml: Union[str, Path]):
        self.map_brackets = self._load_map_brackets(map_brackets_yaml)

    def _load_map_brackets(self, map_brackets_yaml: Union[str, Path]):
        """Load map brackets.

        sex="female" and "male" are filled based on the "all" brackets.
        """
        with open(map_brackets_yaml, "r") as f:
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

    def edit_views_pop(self, explorer: Explorer):
        """Edit population explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            # Edit title
            if indicator_name != "population_density":
                if sex == "all":
                    title = "Population"
                else:
                    title = f"{sex.title()} population"

                if age != "all":
                    if age == "0":
                        title += " of children under the age of 1"
                    elif age == "0-4":
                        title += " of children under the age of 5"
                    elif age == "0-14":
                        title += " of children under the age of 15"
                    elif age == "0-24":
                        title += " under the age of 25"
                    elif age == "1":
                        title += " age 1"
                    elif "-" in age:
                        title += f" aged {age.replace('-', ' to ')} years"
                    elif "+" in age:
                        title += f" older than {age.replace('+', '')} years"
                    else:
                        title += f" at age {age} years"

                if indicator_name == "population_change":
                    title = f"Annual change in {title.lower()}"

                if v.config is None:
                    v.config = {"title": title}
                else:
                    v.config["title"] = title

            # Edit display
            assert v.indicators.y is not None
            assert len(v.indicators.y) == 1

            # Add map colorscheme
            self._add_map_brackets_display(age, sex, indicator_name, v.indicators.y[0])

            # Add legend name
            if indicator_name != "population_density":
                assert v.indicators.y[0].display is not None
                v.indicators.y[0].display["name"] = f"{age} years"

    def edit_views_manual(self, explorer: Explorer):
        """Edit explorer views of manual explorer."""
        pattern = re.compile(r".*/population#population__sex_(?:[a-z]+)__age_([\d_+(plus)]+)__variant_(?:[a-z]+)$")
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
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
                if age == "all":
                    title = "Fertility rate: children per woman"
                else:
                    title = f"Fertility rate from mothers aged {age.capitalize().replace('-', ' to ')}"

                if v.config is None:
                    v.config = {"title": title}
                else:
                    v.config["title"] = title

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
            if indicator_name == "births":
                if age == "all":
                    title = "Births"
                else:
                    title = f"Births from mothers aged {age.capitalize().replace('-', ' to ')}"

                if v.config is None:
                    v.config = {"title": title}
                else:
                    v.config["title"] = title

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
            if indicator_name == "deaths":
                if sex == "all":
                    title = "Deaths"
                else:
                    title = f"{sex.title()} deaths"

                if age != "all":
                    if age == "0":
                        title += " of children under the age of 1"
                    elif age == "0-4":
                        title += " of children under the age of 5"
                    elif age == "0-14":
                        title += " of children under the age of 15"
                    elif age == "0-24":
                        title += " under the age of 25"
                    elif "-" in age:
                        title += f" aged {age.replace('-', ' to ')} years"
                    elif "+" in age:
                        title += f" older than {age.replace('+', '')} years"
                    else:
                        title += f" at age {age} years"

                if v.config is None:
                    v.config = {"title": title}
                else:
                    v.config["title"] = title

            for indicator in v.indicators.y:
                self._add_map_brackets_display(age, sex, indicator_name, indicator)

    def edit_views_le(self, explorer):
        """Edit life expectancy explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator_name == "life_expectancy":
                if sex == "all":
                    title = "Life expectancy"
                else:
                    title = f"{sex.title()} life expectancy"

                if age == "0":
                    title += " at birth"
                else:
                    title += f" at age {age}"

                if v.config is None:
                    v.config = {
                        "title": title,
                        "yAxisMin": int(age),
                    }
                else:
                    v.config["title"] = title
                    v.config["yAxisMin"] = int(age)

            for indicator in v.indicators.y:
                self._add_map_brackets_display(age, sex, indicator_name, indicator)

    def edit_views_sr(self, explorer):
        """Edit sex ratio explorer views"""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            age = v.dimensions["age"]
            if indicator_name == "sex_ratio":
                title = "Sex ratio"

                if age == "0":
                    title += " at birth"
                elif age == "100+":
                    title += " at age 100 and over"
                elif age != "all":
                    title += f" at age {age}"

                if v.config is None:
                    v.config = {
                        "title": title,
                    }
                else:
                    v.config["title"] = title

                for indicator in v.indicators.y:
                    self._add_map_brackets_display(age, "all", indicator_name, indicator)

    def edit_views_dr(self, explorer):
        """Edit dependency ratio explorer views."""
        for v in explorer.views:
            indicator_name = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator_name == "dependency_ratio":
                sex_label = sex + " " if sex != "all" else ""
                if age == "total":
                    title = f"total {sex_label}dependency ratio"
                elif age == "youth":
                    title = f"{sex_label}youth dependency ratio"
                elif age == "old":
                    title = f"{sex_label}old-age dependency ratio"
                else:
                    raise ValueError(f"Unknown age: {age}")

                if v.config is None:
                    v.config = {
                        "title": title.capitalize(),
                    }
                else:
                    v.config["title"] = title.capitalize()

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
