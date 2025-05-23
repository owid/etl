import contextlib
import warnings
from typing import Iterable
from warnings import catch_warnings, simplefilter, warn  # noqa: F401

import structlog

log = structlog.get_logger()


def warn_with_structlog(message, category, filename, lineno, file=None, line=None):
    log.warning(message, category=category.__name__, filename=filename, lineno=lineno)


# Replace the default showwarning with structlog warnings
warnings.showwarning = warn_with_structlog


class MetadataWarning(Warning):
    pass


class StepWarning(Warning):
    pass


class DifferentValuesWarning(MetadataWarning):
    pass


class DisplayNameWarning(MetadataWarning):
    pass


class NoOriginsWarning(MetadataWarning):
    pass


class GroupingByCategoricalWarning(StepWarning):
    pass


@contextlib.contextmanager
def ignore_warnings(ignore_warnings: Iterable[type] = (Warning,)):
    """Ignore warnings. You can pass a list of specific warnings to ignore like MetadataWarning or StepWarning.

    Usage:
        with ignore_warnings():
            ds_garden = create_dataset(...)
    """
    with warnings.catch_warnings():
        for w in ignore_warnings:
            warnings.filterwarnings("ignore", category=w)
        yield
