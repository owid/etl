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
    return (
        getattr(_type, "__origin__", None) == Union
        and len(getattr(_type, "__args__", ())) == 2
        and getattr(_type, "__args__")[1] == type(None)  # noqa
    )


def strip_option(_type: type) -> type:
    return _type.__args__[0]  # type: ignore


def mock(_type: type) -> Any:
    if is_optional_type(_type):
        _type = strip_option(_type)

    if hasattr(_type, "__forward_arg__"):
        raise ValueError(_type)

    if _type == int:
        return random.randint(0, 1000)

    elif _type == bool:
        return random.choice([True, False])

    elif _type == float:
        return 10 * random.random() / random.random()

    elif _type == dt.date:
        return _random_date()

    elif _type == str:
        # some strings in the frictionless standard must be lowercase with no spaces
        return random.choice(_MOCK_STRINGS).lower()

    elif getattr(_type, "_name", None) == "List":
        # e.g. List[int]
        return [mock(_type.__args__[0]) for i in range(random.randint(1, 4))]  # type: ignore

    elif getattr(_type, "_name", None) == "Dict":
        # e.g. Dict[str, int]
        _from, _to = _type.__args__  # type: ignore
        return {mock(_from): mock(_to) for i in range(random.randint(1, 8))}

    elif hasattr(_type, "__dataclass_fields__"):
        # all dataclasses
        return _type(**{f.name: mock(f.type) for f in _type.__dataclass_fields__.values()})  # type: ignore

    elif getattr(_type, "__name__", None) == "ProcessingLog":
        return _type([])

    elif _type == Any:
        return mock(random.choice([str, int, float]))

    elif getattr(_type, "__name__", None) == "YearDateLatest":
        return str(_random_date())

    elif getattr(_type, "__origin__", None) == Literal:
        return random.choice(_type.__args__)  # type: ignore

    raise ValueError(f"don't know how to mock type: {_type}")


def _random_date() -> dt.date:
    return dt.date.fromordinal(dt.date.today().toordinal() - random.randint(0, 1000))
