import dataclasses
import datetime as dt
import hashlib
import re
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Type, TypeVar, Union, overload

import dynamic_yaml
import pytz
import yaml
from unidecode import unidecode

T = TypeVar("T")


def prune_dict(d: dict) -> dict:
    """Remove all keys starting with underscore and all empty values from a dictionary."""
    out = {}
    for k, v in d.items():
        if not k.startswith("_") and v not in [None, [], {}]:
            if isinstance(v, dict):
                out[k] = prune_dict(v)
            elif isinstance(v, list):
                out[k] = [prune_dict(x) if isinstance(x, dict) else x for x in v if x not in [None, [], {}]]
            else:
                out[k] = v
    return out


def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # calling original to_dict returns dictionaries, not objects
    cls.to_dict = lambda self, **kwargs: prune_dict(orig(self, **kwargs))  # type: ignore

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
        .replace("ˆ", "")
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


def dynamic_yaml_load(path: Union[Path, str], params: dict = {}) -> dict:
    with open(path) as istream:
        yd = dynamic_yaml.load(istream)

    yd.update(params)

    # additional parameters
    # NOTE: TODAY is dynamic and can depend on the time of creation. This goes against our
    #   philosophy of having deterministic outputs from snapshots and its use is therefore
    #   discouraged. You should use origin.date_accessed instead if possible.
    #   We only keep it here because of its convenience for COVID and AI automatic updates.
    yd["TODAY"] = dt.datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B %Y")

    return yd


def dynamic_yaml_to_dict(yd: Any) -> dict:
    """Convert dynamic yaml to dict. Using dynamic yaml can cause problems when you
    try to run e.g. Origin(**yd). It's safer to run Origin(**dynamic_yaml_to_dict(yd)) instead."""
    return yaml.safe_load(dynamic_yaml.dump(yd))


def hash_any(x: Any) -> int:
    """Return a unique, deterministic hash for an arbitrary object.

    This function is especially useful when working with mutable objects, such as dataclasses that
    can't be made frozen, but where you still need to use operations like `set`, `dict` keys, or
    deduplication with `unique`. A standard Python `hash()` is not suitable in such cases because Python's
    `hash()` function for strings is randomized across different interpreter sessions for security reasons
    (via `PYTHONHASHSEED`), which can result in non-deterministic hash values.

    This function handles common Python data structures, such as dataclasses, lists, dicts, strings, and `None`,
    and ensures that the returned hash is always deterministic across different runs. For strings, it uses an MD5
    hash truncated to 64 bits to maintain consistent behavior across different runs of the program.

    The function is recursive, so it can handle nested objects like lists of dataclasses, dicts with list values, etc.

    Args:
        x (Any): The object to be hashed. It can be of any type: dataclass, list, dict, string, or other.

    Returns:
        int: A deterministic integer hash value for the object.

    Special cases:
    - **Dataclasses**: It recursively hashes each field of the dataclass by generating a tuple of (field_name_hash, field_value_hash)
      and then hashes that tuple.
    - **Lists**: It recursively hashes each element in the list, converts the list to a tuple (because tuples are hashable),
      and then hashes the tuple.
    - **Dictionaries**: It hashes the keys and values of the dictionary, sorting them by key to ensure consistency, then
      generates a tuple of (key_hash, value_hash) pairs and hashes that tuple.
    - **Strings**: Instead of the built-in `hash()`, it uses the MD5 hash algorithm to generate a consistent 64-bit hash
      (by truncating the result) that remains the same across interpreter runs.
    - **None**: Always returns `0` as the hash for `None`.
    - **Other types**: Falls back on Python's built-in `hash()` function for all other types of objects.

    Example:
    >>> @dataclass
    ... class Person:
    ...     name: str
    ...     age: int
    >>> p1 = Person(name="Alice", age=30)
    >>> p2 = Person(name="Alice", age=30)
    >>> hash_any(p1) == hash_any(p2)
    True
    """

    if is_dataclass(x):
        # Handle dataclass: sort fields by name and hash a tuple of (field_name_hash, field_value_hash) for each field
        return hash(
            tuple([(hash_any(f.name), hash_any(getattr(x, f.name))) for f in sorted(fields(x), key=lambda f: f.name)])
        )
    elif isinstance(x, list):
        # Handle lists: recursively hash each element in the list and hash the result as a tuple
        return hash(tuple([hash_any(y) for y in x]))
    elif isinstance(x, dict):
        # Handle dicts: sort by key, then recursively hash each key-value pair as a tuple of (key_hash, value_hash)
        return hash(tuple([(hash_any(k), hash_any(v)) for k, v in sorted(x.items())]))
    elif isinstance(x, str):
        # Handle strings: compute the MD5 hash, truncate to 64 bits for consistent results across runs
        return int(hashlib.md5(x.encode()).hexdigest(), 16) & ((1 << 64) - 1)
    elif x is None:
        # Handle None: return a fixed hash value for None
        return 0
    else:
        # Fallback for other types: use the built-in hash() function
        return hash(x)


def dataclass_from_dict(cls: Optional[Type[T]], d: Dict[str, Any]) -> T:
    """Recursively create an instance of a dataclass from a dictionary."""
    if cls is None or not dataclasses.is_dataclass(cls):
        return d  # type: ignore

    field_types = {f.name: f.type for f in dataclasses.fields(cls)}

    init_args = {}
    for field_name, v in d.items():
        # Skip values in a dictionary that are not in the dataclass. We do this to stay backwards compatible
        if field_name not in field_types:
            continue

        field_type = field_types[field_name]

        if isinstance(field_type, list):
            init_args[field_name] = [dataclass_from_dict(None, item) for item in v]
        elif isinstance(field_type, dict):
            init_args[field_name] = {k: dataclass_from_dict(None, item) for k, item in v.items()}
        elif dataclasses.is_dataclass(field_type):
            init_args[field_name] = dataclass_from_dict(field_type, v)  # type: ignore
        elif isinstance(field_type, type):
            init_args[field_name] = field_type(v)
        else:
            init_args[field_name] = v

    return cls(**init_args)
