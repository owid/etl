import datetime as dt
import re
from pathlib import Path
from typing import Optional, TypeVar, Union, overload

import dynamic_yaml
import pytz
import yaml
from unidecode import unidecode

T = TypeVar("T")


def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # make sure to call `to_dict` of nested objects as well
    cls.to_dict = lambda self, **kwargs: {  # type: ignore
        k: getattr(self, k).to_dict(**kwargs) if hasattr(getattr(self, k), "to_dict") else v
        for k, v in orig(self, **kwargs).items()
        if not k.startswith("_") and v not in [None, [], {}]
    }

    return cls


@overload
def underscore(name: str, validate: bool = True, camel_to_snake: bool = False) -> str:
    ...


@overload
def underscore(name: None, validate: bool = True, camel_to_snake: bool = False) -> None:
    ...


def underscore(name: Optional[str], validate: bool = True, camel_to_snake: bool = False) -> Optional[str]:
    """Convert arbitrary string to under_score. This was fine tuned on WDI bank column names.
    This function might evolve in the future, so make sure to have your use cases in tests
    or rather underscore your columns yourself.

    Parameters
    ----------
    name : str
        String to format.
    validate: bool, optional
        Whether to validate that the string is under_score. Defaults to True.
    camel_to_snake: bool, optional
        Whether to convert camelCase to snake_case. Defaults to False.

    Returns
    -------
    str:
        String using snake_case formatting.
    """
    if name is None:
        return None

    orig_name = name

    # camelCase to snake_case
    if camel_to_snake:
        name = _camel_to_snake(name)

    name = (
        name.replace(" ", "_")
        .replace("-", "_")
        .replace("—", "_")
        .replace("–", "_")
        .replace("‑", "_")
        .replace(",", "_")
        .replace(".", "_")
        .replace("\t", "_")
        .replace("?", "_")
        .replace('"', "")
        .replace("‘", "")
        .replace("\xa0", "_")
        .replace("’", "")
        .replace("`", "")
        .replace("−", "_")
        .replace("*", "_")
        .replace("“", "")
        .replace("”", "")
        .replace("#", "")
        .replace("^", "")
        .lower()
    )

    # replace special separators
    name = (
        name.replace("(", "__")
        .replace(")", "__")
        .replace(":", "__")
        .replace(";", "__")
        .replace("[", "__")
        .replace("]", "__")
    )

    # replace special symbols
    name = name.replace("/", "_")
    name = name.replace("|", "_")
    name = name.replace("=", "_")
    name = name.replace("%", "pct")
    name = name.replace("+", "plus")
    name = name.replace("us$", "usd")
    name = name.replace("$", "dollar")
    name = name.replace("&", "_and_")
    name = name.replace("<", "_lt_")
    name = name.replace(">", "_gt_")
    name = name.replace("≥", "_gte_")
    name = name.replace("≤", "_lte_")

    # replace quotes
    name = name.replace("'", "")

    # shrink triple underscore
    name = re.sub("__+", "__", name)

    # convert special characters to ASCII
    name = unidecode(name).lower()

    # strip leading and trailing underscores
    name = name.strip("_")

    # if the first letter is number, prefix it with underscore
    if re.match("^[0-9]", name):
        name = f"_{name}"

    # make sure it's under_score now, if not then raise NameError
    if validate:
        validate_underscore(name, f"`{orig_name}`")

    return name


def _camel_to_snake(name: str) -> str:
    """Convert string camelCase to snake_case.

    Reference: https://stackoverflow.com/a/1176023/5056599 CC BY-SA 4.0

    Example:
    >>> _camel_to_snake('camelCase')
    'camel_case'

    Parameters
    ----------
    name : str
        String using camelCase formatting.

    Returns
    -------
    str:
        String using snake_case formatting.
    """
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def underscore_table(t, *args, **kwargs):
    """Convert column and index names to underscore. Only for backwards compatibility.
    Using table.underscore() method is preferred."""
    return t.underscore(*args, **kwargs)


def validate_underscore(name: Optional[str], object_name: str = "Name") -> None:
    """Raise error if name is not snake_case."""
    if name is not None and not re.match("^[a-z_][a-z0-9_]*$", name):
        raise NameError(f"{object_name} must be snake_case. Change `{name}` to `{underscore(name, validate=False)}`")


def dynamic_yaml_load(path: Union[Path, str], params: dict = {}, return_dict=False) -> dict:
    with open(path) as istream:
        yd = dynamic_yaml.load(istream)

    yd.update(params)

    # additional parameters
    yd["TODAY"] = dt.datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B %Y")

    if return_dict:
        yd = yaml.safe_load(dynamic_yaml.dump(yd))

    return yd
