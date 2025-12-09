import json
from pathlib import Path
from typing import TypeVar, Union

from owid.catalog.meta import MetaBase

T = TypeVar("T")


class MDIMBase(MetaBase):
    def save_file(self, filename: Union[str, Path], force_create: bool = False) -> None:
        path = Path(filename)
        if force_create:
            path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as ostream:
            json.dump(self.to_dict(), ostream, indent=2, default=str)


# .pruned_json
def prune_dict(d: dict) -> dict:
    """Remove all keys starting with underscore and all empty values from a dictionary.

    NOTE: This method was copied from owid.catalog.utils. It is slightly different in the sense that it does not remove fields with empty lists! This is because there are some fields which are mandatory and can be empty! (TODO: should probably fix the schema / engineering side)

    """
    out = {}
    for k, v in d.items():
        if not k.startswith("_") and v not in [None, {}]:
            if isinstance(v, dict):
                out[k] = prune_dict(v)
            elif isinstance(v, list):
                out[k] = [prune_dict(x) if isinstance(x, dict) else x for x in v if x not in [None, {}]]
            else:
                out[k] = v
    return out


# explorer.core, model.core, model.dimensions, model.view
def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # calling original to_dict returns dictionaries, not objects
    cls.to_dict = lambda self, **kwargs: prune_dict(orig(self, **kwargs))  # type: ignore

    return cls
