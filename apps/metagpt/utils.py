"""Auxiliary utils for metagpt app."""
from pathlib import Path
from typing import Dict, List


class Channels:
    """Channels for metadata files.

    Using this to avoid hardcoding strings.
    """

    SNAPSHOT = "snapshot"
    GRAPHER = "grapher"
    GARDEN = "garden"


def read_metadata_file(path_to_file: str | Path) -> str:
    """Read a metadata file and returns its content."""
    with open(path_to_file, "r") as file:
        return file.read()


def convert_list_to_dict(data_list: List[str]) -> Dict[str, str]:
    """Convert a list of string elements in the format "'key': 'value'" into a dictionary."""
    data_dict = {}
    for item in data_list:
        # Removing leading and trailing single quotes, then splitting the string by ':'
        key, value = item.strip("'").split(": ", 1)
        data_dict[key.strip("'")] = value.strip("'\"")
    return data_dict
