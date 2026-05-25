"""Build the Conflict Data Source explorer from a single consolidated config.

The companion `conflict_data_source.config.yml` carries all five dropdowns
(`data_source`, `conflict_type`, `measure`, `conflict_sub_type`, `sub_measure`)
and every view across the seven underlying data sources (UCDP, UCDP+PRIO, MARS,
MIE, COW, COW-MID, PRIO).
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    collection = paths.create_collection(
        config=paths.load_collection_config(),
        explorer=True,
    )
    collection.save(tolerate_extra_indicators=True)
