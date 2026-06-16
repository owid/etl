"""Build the Energy explorer.

99 single-indicator views split across four upstream grapher datasets:
- energy/2025-06-27/energy_mix — primary-energy breakdowns by source.
- energy/2026-04-24/electricity_mix — electricity-generation breakdowns by source.
- energy/2025-06-27/primary_energy_consumption — top-level primary-energy totals.
- energy_institute/2025-06-27/statistical_review_of_world_energy — carbon
  intensity and a few totals.

Each chart's text (title, subtitle, note, default tab) lives upstream in the
indicator's `presentation.grapher_config` (set in the relevant garden meta
YAML); this step's only job is to tag each column with `m.dimensions` so
`paths.create_collection(tb=[...], ...)` auto-expands one view per indicator.

The column → dimensions map lives in the sidecar `energy.dims.yaml`, scoped
per upstream table because `primary_energy_consumption__twh` exists in both
`electricity_mix` and `primary_energy_consumption` (we want the latter, which
is what the legacy explorer referenced). The `source` dimension has an "na"
slot for the `view=total` rows that don't show a source breakdown (matching
the legacy explorer's empty Dropdown cell).
"""

from pathlib import Path

import yaml

from etl.helpers import PathFinder

paths = PathFinder(__file__)

COLUMN_DIMENSIONS = yaml.safe_load((Path(__file__).parent / "energy.dims.yaml").read_text())


def _tag(tb, table_key):
    """Set `m.dimensions` and `m.original_short_name` on every column whose name
    appears in COLUMN_DIMENSIONS[table_key]. Columns not in the per-table map
    are silently ignored by `create_collection`'s expander.
    """
    cols_for_table = COLUMN_DIMENSIONS[table_key]
    for col in tb.columns:
        if col not in cols_for_table:
            continue
        tb[col].metadata.dimensions = cols_for_table[col]
        tb[col].metadata.original_short_name = "energy"
    return tb


def run() -> None:
    config = paths.load_collection_config()

    ds_em = paths.load_dataset("energy_mix")
    tb_em = ds_em.read("energy_mix", load_data=False)
    _tag(tb_em, "energy_mix")

    ds_elx = paths.load_dataset("electricity_mix")
    tb_elx = ds_elx.read("electricity_mix", load_data=False)
    _tag(tb_elx, "electricity_mix")

    ds_pec = paths.load_dataset("primary_energy_consumption")
    tb_pec = ds_pec.read("primary_energy_consumption", load_data=False)
    _tag(tb_pec, "primary_energy_consumption")

    ds_sr = paths.load_dataset("statistical_review_of_world_energy")
    tb_sr = ds_sr.read("statistical_review_of_world_energy", load_data=False)
    _tag(tb_sr, "statistical_review_of_world_energy")

    c = paths.create_collection(
        config=config,
        tb=[tb_em, tb_elx, tb_pec, tb_sr],
        indicator_names="energy",
        short_name="energy",
        explorer=True,
    )

    # Universal chart-level config so every view exposes table + map + line + bar
    # tabs. Plus the default view: per-capita primary-energy consumption, total
    # (matches the legacy explorer's defaultView=true row).
    c.edit_views(
        [
            {
                "config": {
                    "type": "LineChart DiscreteBar",
                    "hasMapTab": True,
                },
            },
            {
                "dimensions": {
                    "view": "total",
                    "source": "na",
                    "energy_or_electricity": "primary_energy",
                    "metric": "per_capita_consumption",
                },
                "config": {
                    "defaultView": "true",
                },
            },
        ]
    )

    c.save(tolerate_extra_indicators=True)
