#
#  owid.catalog.charts
#
#
#  Access to data in OWID charts.
#

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from .internal import (
    ChartNotFoundError,  # noqa
    LicenseError,  # noqa
    _fetch_bundle,
    _GrapherBundle,
    _list_charts,
)


@dataclass
class Chart:
    """
    A chart published on Our World in Data, for example:

    https://ourworldindata.org/grapher/life-expectancy
    """

    slug: str

    _bundle: Optional[_GrapherBundle] = None

    @property
    def bundle(self) -> _GrapherBundle:
        # LARS: give a nice error if the chart does not exist
        if self._bundle is None:
            self._bundle = _fetch_bundle(self.slug)

        return self._bundle

    @property
    def config(self) -> dict:
        return self.bundle.config  # type: ignore

    def get_data(self) -> pd.DataFrame:
        return self.bundle.to_frame()

    def __lt__(self, other):
        return self.slug < other.slug

    def __eq__(self, value: object) -> bool:
        return isinstance(value, Chart) and value.slug == self.slug


def list_charts() -> List[str]:
    """
    List all available charts published on Our World in Data.
    """
    return sorted(_list_charts())


def get_data(slug: str) -> pd.DataFrame:
    """
    Fetch the data for a chart by its slug.
    """
    return Chart(slug).get_data()
