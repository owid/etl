from .catalog import Catalog, Dataset  # noqa
from .ingest import add_to_catalog  # noqa

_cache = {}


def __getattr__(name: str) -> Catalog:
    if name == "CATALOG":
        # cached walden catalog instance to avoid repeated slow loading, call
        # `refresh` to force reload
        if "CATALOG" not in _cache:
            _cache["CATALOG"] = Catalog()
        return _cache["CATALOG"]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
