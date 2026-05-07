"""Garden step for Clark et al. (2022) — Environmental impacts of food (211-product roll-up).

The snapshot CSV ships with `Entity` (food product) and `Year` columns plus 25 numeric
impact columns (5 metrics × 4 functional units + 5 biodiversity columns). We rename
`Entity` → `country` to match the OWID convention even though the entities here are food
products rather than geographic units (the explorer sets `entityType: food` to render the
picker correctly downstream).
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot()
    tb = snap.read(safe_types=False)

    tb = tb.rename(columns={"Entity": "country", "Year": "year"})

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_garden.save()
