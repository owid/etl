"""Build the fertilizers explorer.

15 single-indicator views span 4 upstream grapher datasets (faostat_rfn, additional_variables,
excess_fertilizers__west_et_al__2014, nitrogen_efficiency__lassaletta_et_al__2014). Chart text,
map color scales, and chart-level config live in each indicator's `presentation.grapher_config`
in the corresponding garden meta.yml — the explorer view inherits all of that at chart render
time, so this YAML is just dimensions + per-view catalogPath.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    config = paths.load_collection_config()

    c = paths.create_collection(
        config=config,
        short_name="fertilizers",
        explorer=True,
    )

    c.save(tolerate_extra_indicators=True)
