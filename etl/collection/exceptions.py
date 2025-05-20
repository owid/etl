"""Should not import from any other submodule in etl.collection."""


class DuplicateCollectionViews(Exception):
    pass


class PlaceholderError(KeyError):
    """Raised when a placeholder is not found in view_params."""

    pass
