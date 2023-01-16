import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import simplejson as json
from structlog import get_logger

log = get_logger()


@dataclass
class Chart:
    id: str
    config: Dict[str, Any]

    @property
    def variable_ids(self) -> List[int]:
        """IDs of variables used in the chart."""
        ids = [dim["variableId"] for dim in self.config["dimensions"]]
        if "map" in self.config and "variableId" in self.config["map"]:
            ids.append(self.config["map"]["variableId"])
        ids = list(set(ids))
        return ids

    @property
    def variable_id_map(self) -> Optional[int]:
        """Get ID of the variable used in map tab."""
        id_ = None
        if "map" in self.config:
            if "variableId" in self.config["map"]:
                id_ = self.config["map"]["variableId"]
            elif len(self.config["dimensions"]) == 1:
                id_ = self.config["dimensions"][0]["variableId"]
        return id_

    @property
    def is_min_year_hardcoded(self):
        """Check if the minimum year is hardcoded in the title or subtitle."""
        return _is_field_hardcoded_in_title(self.config, "minTime")

    @property
    def is_max_year_hardcoded(self):
        """Check if the maximum year is hardcoded in the title or subtitle."""
        return _is_field_hardcoded_in_title(self.config, "maxTime")

    @property
    def config_as_str(self) -> str:
        """Return chart config as string."""
        return json.dumps(self.config, ignore_nan=True)

    def increase_version(self, by: int = 1) -> None:
        """Increase chart version by 1."""
        self.config["version"] += by

    def update_map_time(self, old_range: List[int], new_range: List[int]) -> None:
        """Update the time of the map tab.

        Logic:
            - 'time' and/or 'targetYear' are updated only if they were set to minimum or maximum years. Otherwise,
                they are left untouched.
        """
        if "targetYear" in self.config["map"]:
            if pd.notnull(min(new_range)) and self.config["map"]["targetYear"] == min(old_range):
                self.config["map"]["targetYear"] = min(new_range)
            elif pd.notnull(max(new_range)) and self.config["map"]["time"] == max(old_range):
                self.config["map"]["targetYear"] = max(new_range)

        # update time
        if "time" in self.config["map"]:
            if pd.notnull(min(new_range)) and self.config["map"]["time"] == min(old_range):
                self.config["map"]["time"] = min(new_range)
            elif pd.notnull(max(new_range)) and self.config["map"]["time"] == max(old_range):
                self.config["map"]["time"] = max(new_range)

    def is_single_time(self, var_min_time: int, var_max_time) -> bool:
        """Check if time slide is anchored at a specific year."""
        min_time = self.config.get("minTime")
        max_time = self.config.get("maxTime")
        times_are_eq = (
            min_time is not None
            and max_time is not None
            and (
                (min_time == max_time)
                or (min_time == "earliest" and max_time == var_min_time)
                or (min_time == var_min_time and max_time == "earliest")
                or (min_time == var_max_time and max_time == "latest")
                or (min_time == "latest" and max_time == var_max_time)
            )
        )
        return times_are_eq

    def single_time_earliest(self, var_min_year: int, var_max_year: int):
        """Check if time slide is anchored at the earliest year."""
        use_min_year = (
            self.config["minTime"] == "earliest"
            or self.config["maxTime"] == "earliest"
            or (
                pd.api.types.is_numeric_dtype(self.config["minTime"])  # type: ignore
                and pd.api.types.is_numeric_dtype(self.config["maxTime"])  # type: ignore
                and abs(self.config["minTime"] - var_min_year) < abs(self.config["maxTime"] - var_max_year)
            )
        )
        return use_min_year

    def check_fastt(self) -> Optional[str]:
        """Checks FASTT in chart.

        update/check text fields: slug, note, title, subtitle, sourceDesc.
        """
        hardcoded_fields = []
        fields = ["title", "subtitle", "note", "slug"]
        for field in fields:
            if field in self.config and re.search(r"\b\d{4}\b", self.config[field]):
                hardcoded_fields.append(f"`{field}`")
        if hardcoded_fields:
            text = (
                f"Chart {self.id} may have fields with hard-coded year in it. Please check field(s)"
                f" {', '.join(hardcoded_fields)}."
            )
            return text


def _is_field_hardcoded_in_title(config: Dict[str, Any], field: str) -> bool:
    """Check if the value of config['field'] is hardcoded in the title or subtitle."""
    min_year_hardcoded = (
        field in config and "title" in config and bool(re.search(rf"{config[field]}", config["title"]))
    ) or (field in config and "subtitle" in config and bool(re.search(rf"{config[field]}", config["subtitle"])))
    return min_year_hardcoded
