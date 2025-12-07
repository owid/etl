import dataclasses
import hashlib
import re
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, TextIO, TypeVar, get_args, get_origin, overload

import dynamic_yaml
import structlog
import yaml
from unidecode import unidecode

T = TypeVar("T")

log = structlog.get_logger()


def prune_dict(d: dict) -> dict:
    """Remove private keys and empty values from a dictionary recursively.

    Removes all keys starting with underscore (private fields) and all empty
    values (None, empty lists, empty dicts) from a dictionary and its nested
    structures.

    Args:
        d: Dictionary to prune.

    Returns:
        New dictionary with private keys and empty values removed.

    Example:
        ```python
        d = {
            "title": "Dataset",
            "_internal": "hidden",
            "count": 0,  # Kept (not empty)
            "empty_list": [],
            "nested": {"value": 1, "null": None}
        }
        result = prune_dict(d)
        # Returns: {"title": "Dataset", "count": 0, "nested": {"value": 1}}
        ```
    """
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
    """Decorator that modifies a class's to_dict method to prune empty values.

    Wraps a dataclass's `to_dict` method to automatically remove private fields
    (starting with underscore) and empty values when serializing to JSON.

    Args:
        cls: Dataclass to decorate.

    Returns:
        The same class with modified `to_dict` method.

    Example:
        ```python
        from dataclasses import dataclass
        from owid.catalog.utils import pruned_json

        @pruned_json
        @dataclass
        class Config:
            name: str
            _internal: str = "hidden"
            optional: str | None = None

        config = Config(name="test", _internal="secret", optional=None)
        d = config.to_dict()
        # Returns: {"name": "test"}  (no _internal or optional)
        ```

    Note:
        This decorator is commonly used with metadata classes to keep JSON
        output clean by removing None values and private fields.
    """
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # calling original to_dict returns dictionaries, not objects
    cls.to_dict = lambda self, **kwargs: prune_dict(orig(self, **kwargs))  # type: ignore

    return cls


@overload
def underscore(name: str, validate: bool = True, camel_to_snake: bool = False) -> str: ...


@overload
def underscore(name: None, validate: bool = True, camel_to_snake: bool = False) -> None: ...


def underscore(name: str | None, validate: bool = True, camel_to_snake: bool = False) -> str | None:
    """Convert arbitrary string to snake_case format.

    Transforms strings into valid Python identifiers using snake_case convention.
    Handles special characters, punctuation, and optionally converts camelCase.
    Originally fine-tuned for World Bank WDI column names.

    Args:
        name: String to format. Returns None if input is None.
        validate: If True, validates the result is valid snake_case and raises
            NameError if not. Defaults to True.
        camel_to_snake: If True, converts camelCase to snake_case before other
            transformations. Defaults to False.

    Returns:
        String in snake_case format, or None if input was None.

    Raises:
        NameError: If validate is True and the result is not valid snake_case.

    Example:
        ```python
        # Basic usage
        underscore("GDP (constant 2015 US$)")
        # Returns: "gdp__constant_2015_usdollar__"

        # Handle camelCase
        underscore("myVariableName", camel_to_snake=True)
        # Returns: "my_variable_name"

        # Skip validation
        underscore("123invalid", validate=False)
        # Returns: "_123invalid"
        ```

    Warning:
        This function may evolve in the future. For critical use cases, either
        add tests or manually underscore your column names.
    """
    if name is None:
        return None

    orig_name = name

    # camelCase to snake_case
    if camel_to_snake:
        name = _camel_to_snake(name)

    # convert special characters to ASCII first, then work with clean ASCII
    name = unidecode(name).lower()

    # replace basic whitespace and punctuation
    name = (
        name.replace(" ", "_")
        .replace("-", "_")
        .replace(",", "_")
        .replace(".", "_")
        .replace("\t", "_")
        .replace("?", "_")
        .replace("!", "_")
        .replace('"', "")
        .replace("'", "")
        .replace("\xa0", "_")
        .replace("`", "")
        .replace("*", "_")
        .replace("#", "")
        .replace("^", "")
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

    # shrink triple underscore
    name = re.sub("__+", "__", name)

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
    """Convert camelCase string to snake_case.

    Args:
        name: String using camelCase formatting.

    Returns:
        String converted to snake_case formatting.

    Example:
        ```python
        _camel_to_snake('camelCase')
        # Returns: 'camel_case'

        _camel_to_snake('HTTPResponseCode')
        # Returns: 'http_response_code'
        ```

    Note:
        Implementation based on https://stackoverflow.com/a/1176023/5056599 (CC BY-SA 4.0)
    """
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def underscore_table(t, *args, **kwargs):
    """Convert column and index names to underscore format.

    Warning:
        **DEPRECATED**: Use `table.underscore()` method instead. This function
        exists only for backward compatibility.

    Args:
        t: Table object to underscore.
        *args: Positional arguments passed to `table.underscore()`.
        **kwargs: Keyword arguments passed to `table.underscore()`.

    Returns:
        Table with underscored column and index names.

    Example:
        Deprecated usage
        ```python
        underscored = underscore_table(my_table)
        ```

        Preferred usage
        ```python
        underscored = my_table.underscore()
        ```
    """
    return t.underscore(*args, **kwargs)


def validate_underscore(name: str | None, object_name: str = "Name") -> None:
    """Validate that a name follows snake_case convention.

    Args:
        name: String to validate. If None, validation is skipped.
        object_name: Name of the object being validated, used in error messages.
            Defaults to "Name".

    Raises:
        NameError: If name is not valid snake_case (lowercase letters, digits,
            and underscores only, must start with letter or underscore).

    Example:
        Valid names pass silently
        ```python
        validate_underscore("my_variable")
        validate_underscore("_private_var")
        ```

        Invalid names raise NameError
        ```python
        try:
            validate_underscore("MyVariable", "Variable")
        except NameError as e:
            print(e)
            # Prints: Variable must be snake_case. Change `MyVariable` to `my_variable`
        ```
    """
    if name is not None and not re.match("^[a-z_][a-z0-9_]*$", name):
        raise NameError(f"{object_name} must be snake_case. Change `{name}` to `{underscore(name, validate=False)}`")


def dynamic_yaml_load(source: Path | str | TextIO, params: dict = {}) -> dict:
    """Load YAML file with dynamic parameter substitution.

    Loads a YAML file and updates it with provided parameters for dynamic
    interpolation. Supports loading from file paths, path strings, or file-like objects.

    Args:
        source: File path (Path or str) or file-like object (e.g., StringIO).
        params: Dictionary of parameters to substitute in the YAML. Defaults to empty dict.

    Returns:
        Parsed YAML data as dictionary with parameters applied.

    Example:
        ```python
        from pathlib import Path

        # Load from file path
        data = dynamic_yaml_load("config.yaml", {"year": 2024})

        # Load from StringIO
        from io import StringIO
        yaml_str = StringIO("title: Dataset {{year}}")
        data = dynamic_yaml_load(yaml_str, {"year": 2024})
        ```
    """
    if isinstance(source, (str, Path)):
        with open(source) as istream:
            yd = dynamic_yaml.load(istream)
    else:  # Assume it's a file-like object (StringIO, BytesIO, etc.)
        yd = dynamic_yaml.load(source)

    yd.update(params)

    return yd


def dynamic_yaml_to_dict(yd: Any) -> dict:
    """Convert dynamic YAML object to plain dictionary.

    Dynamic YAML objects can cause issues when unpacking into dataclass constructors.
    This function converts them to standard Python dictionaries for safe usage.

    Args:
        yd: Dynamic YAML object to convert.

    Returns:
        Plain Python dictionary.

    Example:
        Problem: Dynamic YAML can cause errors
        ```python
        # origin = Origin(**dynamic_yaml_obj)  # May fail
        ```

        Solution: Convert to dict first
        ```python
        origin = Origin(**dynamic_yaml_to_dict(dynamic_yaml_obj))  # Safe
        ```

    Note:
        Always use this conversion before unpacking into dataclass constructors
        to avoid unexpected behavior with dynamic YAML objects.
    """
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
        ```python
        >>> @dataclass
        >>> class Person:
        ...    name: str
        ...    age: int

        >>> p1 = Person(name="Alice", age=30)
        >>> p2 = Person(name="Alice", age=30)
        >>> hash_any(p1) == hash_any(p2)
        True
        ```
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


def dataclass_from_dict(cls: type[T] | None, d: dict[str, Any]) -> T:
    """Recursively create an instance of a dataclass from a dictionary. We've implemented custom
    method because original dataclasses_json.from_dict was too slow (this gives us more than 2x
    speedup). See https://github.com/owid/etl/pull/3517#issuecomment-2468084380 for more details.
    """
    if d is None or not dataclasses.is_dataclass(cls) or not isinstance(d, dict):
        return d  # type: ignore

    field_types = {f.name: f.type for f in dataclasses.fields(cls)}

    init_args = {}
    for field_name, v in d.items():
        # Skip values in a dictionary that are not in the dataclass
        if field_name not in field_types:
            continue

        # Handle None values right away
        if v is None:
            init_args[field_name] = None
            continue

        field_type = field_types[field_name]
        origin = get_origin(field_type)
        args = get_args(field_type)

        # unwrap  (e.g. License | None -> License)
        if type(None) in args:
            filtered_args = tuple(a for a in args if a is not type(None))
            if len(filtered_args) == 1:
                # Save the original field_type for List[...] | None case
                field_type = filtered_args[0]
                # For List[...] | None case, update the origin and args
                if get_origin(field_type) is list:
                    origin = list
                    args = get_args(field_type)

        if origin is list:
            # Check if we have type arguments (e.g. List[str])
            if args:
                item_type = args[0]
                init_args[field_name] = [dataclass_from_dict(item_type, item) for item in v]
            else:
                # No type arguments, just use the values as-is
                init_args[field_name] = v
        elif origin is dict:
            key_type, value_type = args
            init_args[field_name] = {k: dataclass_from_dict(value_type, item) for k, item in v.items()}
        elif dataclasses.is_dataclass(field_type):
            init_args[field_name] = field_type.from_dict(v)  # type: ignore
        elif isinstance(field_type, type) and field_type not in (Any,):
            try:
                init_args[field_name] = field_type(v)
            except ValueError as e:
                log.error(
                    "conversion.failed",
                    field_name=field_name,
                    field_type=field_type,
                    path=f"{d.get('channel')}/{d.get('namespace')}/{d.get('version')}/{d.get('short_name')}",
                    error=str(e),
                )
                continue
        else:
            init_args[field_name] = v

    return cls(**init_args)


def remove_details_on_demand(text: str) -> str:
    """Remove details-on-demand references from markdown text.

    Strips out special markdown links that reference details-on-demand content,
    keeping only the link text. This is useful for generating plain text versions
    of content that contains interactive elements.

    Args:
        text: Markdown text containing details-on-demand references.

    Returns:
        Text with details-on-demand references removed, keeping only link text.

    Example:
        ```python
        text = "This is a [description](#dod:something) of the data."
        result = remove_details_on_demand(text)
        # Returns: "This is a description of the data."
        ```

        Multiple references
        ```python
        text = "See [mortality](#dod:mort) and [fertility](#dod:fert) data."
        result = remove_details_on_demand(text)
        # Returns: "See mortality and fertility data."
        ```

    Note:
        The regex pattern matches `[text](#dod:keyword)` and replaces it with just `text`.
    """
    # The regex matches the entire markdown link syntax [text](#dod:keyword) and replaces it with just the text
    regex = r"\[([^\]]+)\]\(#dod:[^\)]+\)"
    text = re.sub(regex, r"\1", text)

    return text


def parse_numeric_list(val: list | str) -> list[float | int]:
    """Parse a string representation of a numeric list.

    Converts a comma-separated string of numbers (optionally wrapped in brackets)
    into a Python list of integers and floats.

    Args:
        val: String representation of a numeric list or an existing list.
            If already a list, returns it unchanged.

    Returns:
        List of integers and floats parsed from the input string.

    Example:
        ```python
        # String with brackets
        parse_numeric_list("[10, 20, 30]")
        # Returns: [10, 20, 30]

        # String without brackets
        parse_numeric_list("1.5, 2.5, 3.0")
        # Returns: [1.5, 2.5, 3.0]

        # Mixed integers and floats
        parse_numeric_list("10, 20.5, 30")
        # Returns: [10, 20.5, 30]

        # Already a list (no-op)
        parse_numeric_list([1, 2, 3])
        # Returns: [1, 2, 3]
        ```

    Note:
        Numbers with decimal points are parsed as floats, others as integers.
    """
    if isinstance(val, list):
        return val
    stripped = val.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        stripped = stripped[1:-1]

    return [float(x) if "." in x else int(x) for x in stripped.split(",") if x.strip()]
