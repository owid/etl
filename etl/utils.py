import re

from typing import Any, List
from dataclasses import dataclass, field


def import_from(module: str, name: str) -> Any:
    module = __import__(re.sub("/", ".", module), fromlist=[name])
    return getattr(module, name)


@dataclass
class IntRange:
    min: int  # type: ignore
    _min: int = field(init=False, repr=False)
    max: int  # type: ignore
    _max: int = field(init=False, repr=False)

    @property  # type: ignore
    def min(self) -> int:
        return self._min

    @min.setter
    def min(self, x: int) -> None:
        self._min = int(x)

    @property  # type: ignore
    def max(self) -> int:
        return self._max

    @max.setter
    def max(self, x: int) -> None:
        self._max = int(x)

    @staticmethod
    def from_values(xs: List[int]):
        return IntRange(min(xs), max(xs))

    def to_values(self):
        return [self.min, self.max]
