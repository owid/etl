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
    List all available charts published on Our World in Data, representing each via
    a short slug that you can use with `get_data()`.
    """
    return sorted(_list_charts())


def get_data(slug_or_url: str) -> pd.DataFrame:
    """
    Fetch the data for a chart by its slug or by the URL of the chart.

    Additional metadata about the chart is available in the DataFrame's `attrs` attribute.
    """
    if slug_or_url.startswith("https://ourworldindata.org/grapher/"):
        slug = slug_or_url.split("/")[-1]

    elif slug_or_url.startswith("https://"):
        raise ValueError("URL must be a Grapher URL, e.g. https://ourworldindata.org/grapher/life-expectancy")

    else:
        slug = slug_or_url

    return Chart(slug).get_data()
