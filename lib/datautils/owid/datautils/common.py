"""Common objects shared by other modules."""

import warnings
from typing import Any, List, Set, Union


class ExceptionFromDocstring(Exception):
    """Exception that returns its own docstring, if no message is explicitly given."""

    def __init__(self, exception_message: Union[str, None] = None, *args: Any):
        super().__init__(exception_message or self.__doc__, *args)


class ExceptionFromDocstringWithKwargs(Exception):
    """Exception that returns its own docstring, if no message is explicitly given."""

    def __init__(self, exception_message: Union[str, None] = None, *args: Any, **kwargs: Any):
        text = exception_message or self.__doc__
        if kwargs:
            additional_text = ", ".join([f"{key}: {value}" for key, value in kwargs.items()])
            text += additional_text
        super().__init__(text, *args)


def warn_on_list_of_entities(
    list_of_entities: Union[List[Any], Set[Any]], warning_message: str, show_list: bool
) -> None:
    """Raise a warning with a custom message, and optionally print a list of affected elements.

    Parameters
    ----------
    list_of_entities : list or set
        Elements to optionally print one by one (only relevant if show_list is True).
    warning_message : str
        Warning message.
    show_list : bool
        True to print a list of affected entities.

    """
    warnings.warn(warning_message)
    if show_list:
        print(warning_message)
        print("\n".join(["* " + str(entity) for entity in list_of_entities]))
