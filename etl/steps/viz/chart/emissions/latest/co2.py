"""CO₂ and greenhouse gas emissions data explorer, as a multidim. It replaces the `viz://explorer/emissions/latest/co2` step.

54 single-indicator views split across two upstream grapher datasets:
- gcp/2026-11-10/global_carbon_budget — fossil emissions, cumulative totals,
  per-fuel breakdowns.
- emissions/2025-12-04/national_contributions — non-CO₂ gases (CH₄, N₂O,
  all-GHG combined) plus warming-impact temperature responses.

Each chart's text (title, subtitle, note, default tab) lives upstream in the
indicator's `presentation.grapher_config` (set in the relevant garden meta YAML);
this step tags each column with `m.dimensions` so `paths.create_chart(tb=[...], ...)`
auto-expands one view per indicator.

The column → dimensions map lives in the sidecar `co2.dims.yaml`. The `fuel` dimension
has an "na" slot for views without a fuel breakdown (consumption-based emissions,
non-CO₂ gases). For views without a "relative to world total" variant (per capita,
per kWh, etc.) the `relative` view doesn't exist, so that toggle only offers "Total".
"""

from pathlib import Path

import yaml

from etl.helpers import PathFinder
from etl.viz.chart.explorer_text import add_explorer_text

paths = PathFinder(__file__)

COLUMN_DIMENSIONS = yaml.safe_load((Path(__file__).parent / "co2.dims.yaml").read_text())


def _tag(tb):
    """Set `m.dimensions` on every column whose name appears in COLUMN_DIMENSIONS.

    The two upstream tables don't share column names, so we iterate `tb.columns`
    blindly: columns from the other table simply aren't here, and untagged
    columns are ignored by the expander.
    """
    for col in tb.columns:
        if col not in COLUMN_DIMENSIONS:
            continue
        tb[col].metadata.dimensions = COLUMN_DIMENSIONS[col]
        tb[col].metadata.original_short_name = "emissions"
    return tb


def run() -> None:
    config = paths.load_config()

    # Load the two upstream tables and tag their columns with dimensions.
    ds_gcb = paths.load_dataset("global_carbon_budget")
    tb_gcb = ds_gcb.read("global_carbon_budget", load_data=False)
    _tag(tb_gcb)

    ds_nc = paths.load_dataset("national_contributions")
    tb_nc = ds_nc.read("national_contributions", load_data=False)
    _tag(tb_nc)

    # Expand one view per tagged indicator.
    c = paths.create_chart(
        config=config,
        tb=[tb_gcb, tb_nc],
        indicator_names="emissions",
        short_name="co2",
    )

    # Universal chart-level config, plus a Slope tab on the flagship per-capita CO₂ territorial view.
    # Per-view title/subtitle inherit from each indicator's upstream `presentation.grapher_config`.
    # The explorer opened every view on the chart tab (overriding the indicators' `tab: map`), hid the time
    # span in titles, and linked its footer to the topic page; keep all three.
    c.edit_views(
        [
            {
                "config": {
                    "chartTypes": ["LineChart", "DiscreteBar"],
                    "hasMapTab": True,
                    "tab": "chart",
                    "originUrl": "/co2-and-greenhouse-gas-emissions",
                    "hideAnnotationFieldsInTitle": {"entity": True, "time": True, "changeInPrefix": True},
                },
            },
            {
                "dimensions": {
                    "gas": "co2",
                    "accounting": "territorial",
                    "fuel": "all_fossil",
                    "count": "per_capita",
                },
                "config": {
                    "chartTypes": ["LineChart", "SlopeChart", "DiscreteBar"],
                },
            },
        ]
    )

    _apply_decimal_overrides(c)

    # The explorer shows a long text below the chart (data sources, derived metrics, FAQs) and a download
    # link. Append them to every view's key points (co2.explorer.md), until grapher can show multidim-level
    # text (see etl/viz/chart/explorer_text.py).
    add_explorer_text(c, paths)

    c.save()


def _apply_decimal_overrides(c) -> None:
    """Override the default 2-decimal rendering for views where it isn't right.

    `numDecimalPlaces` lives on the indicator's `display` block, not on view-level
    config, so `edit_views` doesn't reach it and we set it via a per-view loop.
    """
    for view in c.views:
        if view.matches(gas=["methane", "all_ghg"], count="per_capita"):
            decimals = 1
        elif view.matches(gas="warming_impact", fuel=["all_fossil", "land_use"]):
            decimals = 3
        else:
            continue
        if not view.indicators.y:
            continue
        for indicator in view.indicators.y:
            indicator.display = {**(indicator.display or {}), "numDecimalPlaces": decimals}
