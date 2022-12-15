from collections import OrderedDict
from typing import Any

import yaml


def _str_presenter(dumper: Any, data: Any) -> Any:
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


# dump multi-line strings correctly in YAML and add support for OrderedDict
yaml.add_representer(str, _str_presenter)
yaml.add_representer(
    OrderedDict,
    lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items()),
)

# to use with safe_dump:
yaml.representer.SafeRepresenter.add_representer(str, _str_presenter)
