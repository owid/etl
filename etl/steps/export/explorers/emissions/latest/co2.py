"""Build the CO₂ and Greenhouse Gas Emissions explorer.

54 single-indicator views split across two upstream grapher datasets:
- gcp/2025-11-13/global_carbon_budget — fossil emissions, cumulative totals,
  per-fuel breakdowns.
- emissions/2025-12-04/national_contributions — non-CO₂ gases (CH₄, N₂O,
  all-GHG combined) plus warming-impact temperature responses.

Each chart's text (title, subtitle, note, default tab) lives upstream in the
indicator's `presentation.grapher_config` (set in the relevant garden meta YAML);
this step's only job is to tag each column with `m.dimensions` so
`paths.create_collection(tb=[...], ...)` auto-expands one view per indicator.

The column → dimensions map lives in the sidecar `co2.dims.yaml` — bulk data
out of the way of the step logic. The `fuel` dimension has an "na" slot for
views without a fuel breakdown (consumption-based emissions, non-CO₂ gases).
`relative_to_world` is a binary checkbox — for views without a "relative"
variant (per-capita, per-kWh, etc.) the corresponding `relative` view simply
doesn't exist and the toggle has no effect in those states.
"""

from pathlib import Path

import yaml

from etl.helpers import PathFinder

paths = PathFinder(__file__)

COLUMN_DIMENSIONS = yaml.safe_load((Path(__file__).parent / "co2.dims.yaml").read_text())


def _tag(tb):
    """Set `m.dimensions` on every column whose name appears in COLUMN_DIMENSIONS.

    The two upstream tables don't share column names, so we iterate `tb.columns`
    blindly — columns from the other table simply aren't here, and untagged
    columns are silently ignored by `create_collection`'s expander.
    """
    for col in tb.columns:
        if col not in COLUMN_DIMENSIONS:
            continue
        tb[col].metadata.dimensions = COLUMN_DIMENSIONS[col]
        tb[col].metadata.original_short_name = "value"
    return tb


def run() -> None:
    config = paths.load_collection_config()

    ds_gcb = paths.load_dataset("global_carbon_budget")
    tb_gcb = ds_gcb.read("global_carbon_budget", load_data=False)
    _tag(tb_gcb)

    ds_nc = paths.load_dataset("national_contributions")
    tb_nc = ds_nc.read("national_contributions", load_data=False)
    _tag(tb_nc)

    c = paths.create_collection(
        config=config,
        tb=[tb_gcb, tb_nc],
        indicator_names="value",
        short_name="co2",
        explorer=True,
    )

    # Universal chart-level config; per-view title/subtitle/tab inherit from
    # each indicator's upstream `presentation.grapher_config`.
    c.set_global_config(
        {
            "type": "LineChart",
            "hasMapTab": True,
        }
    )

    c.save(tolerate_extra_indicators=True)
