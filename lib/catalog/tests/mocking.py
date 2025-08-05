#
#  mocking.py
#

import datetime as dt
import random
from typing import Any, Literal, Union

_MOCK_STRINGS = [
    "alpha",
    "beta",
    "gamma" "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "kappa",
    "lambda",
    "mu",
    "nu",
]


def is_optional_type(_type: type) -> bool:
    # Handle both old Union[str, None] and new str | None syntax
    origin = getattr(_type, "__origin__", None)
    args = getattr(_type, "__args__", ())

    # For the new union syntax (str | None), check if it's a UnionType or has the right structure
    import types

    # Check if it's the new union type (str | None)
    if isinstance(_type, types.UnionType):
        return len(args) == 2 and type(None) in args

    # Check for old Union syntax
    if origin == Union and len(args) == 2 and type(None) in args:
        return True

    # Check if it has args but no origin (new union syntax)
    if origin is None and len(args) == 2 and type(None) in args:
        return True

    return False


def strip_option(_type: type) -> type:
    # Return the non-None type from the union
    args = getattr(_type, "__args__", ())
    return next(arg for arg in args if arg is not type(None))


def mock(_type: type) -> Any:
    if is_optional_type(_type):
        _type = strip_option(_type)

    if hasattr(_type, "__forward_arg__"):
        raise ValueError(_type)

    if _type is int:
        return random.randint(0, 1000)

    elif _type is bool:
        return random.choice([True, False])

    elif _type is float:
        return 10 * random.random() / random.random()

    elif _type is dt.date:
        return _random_date()

    elif _type is str:
        # some strings in the frictionless standard must be lowercase with no spaces
        return random.choice(_MOCK_STRINGS).lower()

    elif getattr(_type, "_name", None) == "List" or getattr(_type, "__origin__", None) is list:
        args = getattr(_type, "__args__", ())
        if args and args[0].__name__ == "TableDimension":
            return None

        # e.g. List[int] or list[int]
        if args:
            return [mock(args[0]) for i in range(random.randint(1, 4))]  # type: ignore
        else:
            return []

    elif getattr(_type, "_name", None) == "Dict" or getattr(_type, "__origin__", None) is dict:
        # e.g. Dict[str, int] or dict[str, int]
        args = getattr(_type, "__args__", ())
        if len(args) >= 2:
            _from, _to = args[0], args[1]
            return {mock(_from): mock(_to) for i in range(random.randint(1, 8))}
        else:
            return {}

    elif hasattr(_type, "__dataclass_fields__"):
        # all dataclasses
        return _type(**{f.name: mock(f.type) for f in _type.__dataclass_fields__.values()})  # type: ignore

    elif getattr(_type, "__name__", None) == "ProcessingLog":
        return _type([])

    elif _type is Any:
        return mock(random.choice([str, int, float]))

    elif getattr(_type, "__name__", None) == "YearDateLatest":
        return str(_random_date())

    elif getattr(_type, "__origin__", None) == Literal:
        return random.choice(_type.__args__)  # type: ignore

    elif getattr(_type, "__origin__", None) == Union:
        return mock(random.choice(_type.__args__))

    elif _type is type(None):
        return None

    raise ValueError(f"don't know how to mock type: {_type}")


def _random_date() -> dt.date:
    return dt.date.fromordinal(dt.date.today().toordinal() - random.randint(0, 1000))
