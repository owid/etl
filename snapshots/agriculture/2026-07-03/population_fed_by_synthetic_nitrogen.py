"""Script to create a snapshot of the share of the world population fed by synthetic nitrogen fertilizers.

The data is the share of the world population sustained by synthetic nitrogen fertilizers (produced via the
Haber-Bosch process), digitized by Our World in Data in 2017 from Figure 1 of Erisman et al. (2008),
"How a century of ammonia synthesis changed the world" (https://www.nature.com/articles/ngeo325).

The digitized values are hardcoded below (they were lifted from the legacy grapher dataset
"Population fed by Haber-Bosch fertilizers - FAO (2017)", where each value was stored as
share x world population, with population frozen at 2015).
"""

import pandas as pd

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Share of the world population fed by synthetic nitrogen fertilizers (%), digitized from Figure 1 of
# Erisman et al. (2008).
SHARE_FED_BY_YEAR = {
    1900: 0.0,
    1910: 0.5,
    1930: 5.0,
    1940: 7.0,
    1950: 8.0,
    1955: 11.0,
    1960: 13.0,
    1970: 24.0,
    1980: 30.0,
    1990: 40.0,
    2000: 44.0,
    2008: 48.0,
}


def run(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    snap = paths.init_snapshot()

    df = pd.DataFrame(
        {
            "year": list(SHARE_FED_BY_YEAR.keys()),
            "share_of_population_fed_by_synthetic_nitrogen": list(SHARE_FED_BY_YEAR.values()),
        }
    )

    snap.create_snapshot(data=df, upload=upload)
