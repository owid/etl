"""Should not import from any other submodule in etl.collection."""


class DuplicateCollectionViews(Exception):
    pass


class ParamKeyError(KeyError):
    """Raised when a placeholder is not found in `params`."""

    pass
