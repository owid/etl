#
#  properties.py
#

from typing import Any, Protocol


class MetadataClass(Protocol):
    metadata: Any


def metadata_property(k: str) -> property:
    """
    Make metadata fields available directly on a base class.
    """

    def getter(self: MetadataClass) -> Any:
        return getattr(self.metadata, k)

    def setter(self: MetadataClass, v: Any) -> None:
        return setattr(self.metadata, k, v)

    return property(getter, setter)
