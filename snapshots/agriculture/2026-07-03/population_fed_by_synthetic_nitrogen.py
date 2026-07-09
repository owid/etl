"""Script to create a snapshot of the share of the world population fed by synthetic nitrogen fertilizers.

The data is the share of the world population sustained by synthetic nitrogen fertilizers (produced via the
Haber-Bosch process), extracted from Figure 1 of Erisman et al. (2008).

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
