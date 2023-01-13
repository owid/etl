"""Chart objects.

For more details on the schema of Grapher charts, please see: https://github.com/owid/owid-grapher/blob/master/packages/%40ourworldindata/grapher/src/schema/grapher-schema.002.yaml
"""
from dataclasses import dataclass
import pandas as pd
from typing import Dict, Any, Optional, List
import re


@dataclass
class Chart:
    id: str
    config: Dict[Any, Any]

    @property
    def variable_ids(self) -> List[int]:
        ids = set([dim["variableId"] for dim in self.config["dimensions"]])
        if "map" in self.config and "variableId" in self.config["map"]:
            ids.add(self.config["map"]["variableId"])
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
        return self._is_field_hardcoded_in_title("minTime")

    @property
    def is_max_year_hardcoded(self):
        """Check if the maximum year is hardcoded in the title or subtitle."""
        return self._is_field_hardcoded_in_title("maxTime")

    def is_single_time(self, var_min_year: int, var_max_year) -> bool:
        """Check if time slide is anchored at a specific year."""
        min_time = self.config.get("minTime")
        max_time = self.config.get("maxTime")
        times_are_eq = (
            min_time is not None
            and max_time is not None
            and (
                (min_time == max_time)
                or (min_time == "earliest" and max_time == var_min_year)
                or (min_time == var_min_year and max_time == "earliest")
                or (min_time == var_max_year and max_time == "latest")
                or (min_time == "latest" and max_time == var_max_year)
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

    def update_map_time(self, old_range: List[int], new_range: List[int]) -> None:
        """Update the time of the map tab."""
        if "targetYear" in self.config["map"]:
            if pd.notnull(min(new_range)) and self.config["map"]["targetYear"] == min(old_range):
                self.config["map"]["targetYear"] = min(new_range)
            elif pd.notnull(max(new_range)):
                self.config["map"]["targetYear"] = max(new_range)

        # update time
        if "time" in self.config["map"]:
            if pd.notnull(min(new_range)) and self.config["map"]["time"] == min(old_range):
                self.config["map"]["time"] = min(new_range)
            elif pd.notnull(max(new_range)):
                self.config["map"]["time"] = max(new_range)

    def check_fastt(self) -> List[str]:
        """modifies chart config FASTT.

        update/check text fields: slug, note, title, subtitle, sourceDesc.
        """
        report = []
        if "title" in self.config and re.search(r"\b\d{4}\b", self.config["title"]):
            report.append(
                f"Chart {self.id} title may have a hard-coded year in it that "
                f'will not be updated: "{self.config["title"]}"'
            )
        if "subtitle" in self.config and re.search(r"\b\d{4}\b", self.config["subtitle"]):
            report.append(
                f"Chart {self.id} subtitle may have a hard-coded year in it "
                f'that will not be updated: "{self.config["subtitle"]}"'
            )
        if "note" in self.config and re.search(r"\b\d{4}\b", self.config["note"]):
            report.append(
                f"Chart {self.id} note may have a hard-coded year in it that "
                f'will not be updated: "{self.config["note"]}"'
            )
        if re.search(r"\b\d{4}\b", self.config["slug"]):
            report.append(
                f"Chart {self.id} slug may have a hard-coded year in it that "
                f'will not be updated: "{self.config["slug"]}"'
            )
        return report

    def _is_field_hardcoded_in_title(self, field: str) -> bool:
        """Check if the value of config['field'] is hardcoded in the title or subtitle."""
        min_year_hardcoded = (
            field in self.config
            and "title" in self.config
            and bool(re.search(rf"{self.config[field]}", self.config["title"]))
        ) or (
            field in self.config
            and "subtitle" in self.config
            and bool(re.search(rf"{self.config[field]}", self.config["subtitle"]))
        )
        return min_year_hardcoded


class ReviewedChart(Chart):
    reason: str

    def __init__(self, id, config, reason):
        Chart.__init__(self, id, config)
        self.reason = reason

    @classmethod
    def from_chart(cls, chart, reason):
        return cls(id=chart.id, config=chart.config, dimensions=chart.dimensions, reason=reason)
