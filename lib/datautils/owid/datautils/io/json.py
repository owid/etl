"""Input/Output functions for local files."""

import json
from pathlib import Path
from typing import Any, Hashable, List, Tuple, Union

from owid.datautils.common import warn_on_list_of_entities
from owid.datautils.decorators import enable_file_download


def _load_json_data_and_duplicated_keys(ordered_pairs: List[Tuple[Hashable, Any]]) -> Any:
    clean_dict = {}
    duplicated_keys = []
    for key, value in ordered_pairs:
        if key in clean_dict:
            duplicated_keys.append(key)
        clean_dict[key] = value
    if len(duplicated_keys) > 0:
        warn_on_list_of_entities(
            list_of_entities=duplicated_keys,
            warning_message="Duplicated entities found.",
            show_list=True,
        )

    return clean_dict


@enable_file_download(path_arg_name="json_file")
def load_json(json_file: Union[str, Path], warn_on_duplicated_keys: bool = True) -> Any:
    """Load data from JSON file with optional duplicate key detection.

    If the JSON file contains duplicated keys, a warning is optionally raised,
    and only the value of the latest duplicated key is kept.

    Args:
        json_file: Path to JSON file. Supports local files and URLs (via decorator).
        warn_on_duplicated_keys: If True, warn about duplicate keys in JSON file.

    Returns:
        Data loaded from JSON file (typically a dict or list).

    Example:
        ```python
        from owid.datautils.io.json import load_json

        # Load JSON file
        data = load_json("data.json")

        # Disable duplicate key warnings
        data = load_json("data.json", warn_on_duplicated_keys=False)
        ```
    """
    with open(json_file, "r") as _json_file:
        json_content = _json_file.read()
        if warn_on_duplicated_keys:
            data = json.loads(json_content, object_pairs_hook=_load_json_data_and_duplicated_keys)
        else:
            data = json.loads(json_content)

    return data


def save_json(data: Any, json_file: Union[str, Path], **kwargs: Any) -> None:
    """Save data to a JSON file.

    Args:
        data: Data to be stored in JSON file (typically a dict or list).
        json_file: Path to output JSON file. Parent directories are created if needed.
        **kwargs: Additional keyword arguments for `json.dump()` (e.g., `indent=4`, `sort_keys=True`).

    Example:
        ```python
        from owid.datautils.io.json import save_json

        data = {"name": "John", "age": 30}

        # Save JSON file
        save_json(data, "output.json")

        # Save with formatting
        save_json(data, "output.json", indent=4, sort_keys=True)
        ```
    """
    # Ensure json_file is a path.
    json_file = Path(json_file)

    # Ensure output directory exists.
    json_file.parent.mkdir(parents=True, exist_ok=True)

    with open(json_file, "w") as _json_file:
        json.dump(data, _json_file, **kwargs)
