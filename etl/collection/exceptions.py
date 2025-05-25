"""Should not import from any other submodule in etl.collection."""


class DuplicateCollectionViews(Exception):
    pass


class ParamKeyError(KeyError):
    """Raised when a placeholder is not found in `params`."""

    pass


class MissingChoiceError(Exception):
    """Raised when a choice is missing in the dimension choices."""

    pass


class DuplicateValuesError(Exception):
    """Raised when duplicate values are found in a List of dimensions or choices."""

    pass


class ExtraIndicatorsInUseError(Exception):
    """Raised when "extra" indicators are used in some of the dimension fields."""

    pass
