"""Grapher step for the Electricity Mix (Energy Institute & Ember) dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.grapher.helpers import add_columns_for_multiindicator_chart
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# The nine sources every stacked electricity chart decomposes generation into. The clean pair is used
# (bioenergy, and other renewables excluding it); before Ember's split exists those two are filled with
# zeros, like the other late-starting renewables.
STACKED_SOURCES = [
    "coal",
    "gas",
    "oil",
    "nuclear",
    "hydro",
    "wind",
    "solar",
    "bioenergy",
    "other_renewables_excluding_bioenergy",
]
STACKED_SOURCES_FILLED_WITH_ZEROS = ["hydro", "wind", "solar", "bioenergy", "other_renewables_excluding_bioenergy"]

# A complete stack can still misrepresent the mix when its sources add up to far less than the entity's own
# reported total: before 1985 the United Kingdom's sources reach as little as 77% of its total generation.
# Rows whose complete stack deviates from the total by more than this fraction are blanked in the
# chart-specific columns (never in the standalone ones).
STACK_TOLERANCE = 0.05


def censor_and_check_stack(
    tb: Table, columns: list[str], denominator: str | None, chart_slug: str, filled_with_zeros: list[str]
) -> Table:
    """Leave the chart-specific columns of a stacked chart unable to misstate the mix.

    Three cases, applied to the chart-specific columns only (never to the standalone ones):

    - A row missing some sources whose reported total leaves no room for them gets those sources set to
      zero: Sri Lanka reports no nuclear at all, but its other sources add up to its total, which is the
      producer confirming the gap is zero rather than unknown.
    - Any other row missing a source is blanked entirely, so a stacked chart cannot show part of a mix
      as though it were the whole: Oceania in 1985 reports only a fifth of its total by source.
    - A row reporting every source whose sum still deviates from the total beyond STACK_TOLERANCE is
      blanked too: the United Kingdom's sources before 1985 reach as little as 77% of its own total.

    `denominator` is the column holding the entity's total, or None for shares, which must add up to 100.
    Asserts the two resulting invariants: whole mix or nothing, and complete stacks match the total.
    """
    if denominator is None:
        total = pd.Series(100.0, index=tb.index)
    else:
        total = tb[denominator]

    # Rebuild the values from the standalone columns: add_columns_for_multiindicator_chart has already
    # blanked every partial row, before the reported total could confirm any of its gaps as zeros.
    for column in columns:
        metadata = tb[column].metadata
        tb[column] = tb[column.rsplit("_chart_", 1)[0]].copy()
        tb[column].metadata = metadata
    for column in filled_with_zeros:
        tb[column] = tb[column].fillna(0)

    # Fill the remaining gaps the total confirms as zero.
    stack = tb[columns]
    accounted = stack.sum(axis=1, min_count=1)
    some_missing = stack.isna().any(axis=1) & stack.notna().any(axis=1)
    confirmed_zero = some_missing & total.notna() & ((total - accounted).abs() <= STACK_TOLERANCE * total)
    for column in columns:
        tb.loc[confirmed_zero & tb[column].isna(), column] = 0

    # Blank rows that still miss a source, and complete rows whose sum misstates the total.
    stack = tb[columns]
    complete = stack.notna().all(axis=1)
    incomplete = stack.notna().any(axis=1) & ~complete
    deviation = stack.sum(axis=1) / total - 1
    off = complete & total.notna() & (total > 0) & (deviation.abs() > STACK_TOLERANCE)
    tb.loc[incomplete | off, columns] = np.nan

    # The invariants a stacked chart needs.
    stack = tb[columns]
    complete = stack.notna().all(axis=1)
    assert not (stack.notna().any(axis=1) & ~complete).any(), f"{chart_slug}: rows must show the whole mix or none."
    comparable = complete & total.notna() & (total > 0)
    deviation = stack.loc[comparable].sum(axis=1) / total[comparable] - 1
    error = f"{chart_slug}: complete stacks deviate from the total beyond tolerance."
    assert (deviation.abs() <= STACK_TOLERANCE).all(), error
    assert complete.any(), f"{chart_slug}: censoring left no complete rows at all."
    paths.log.info(
        f"{chart_slug}: zero-filled {int(confirmed_zero.sum())} rows confirmed by the total, "
        f"blanked {int(incomplete.sum())} incomplete and {int(off.sum())} misstated rows."
    )
    return tb


def run() -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("electricity_mix")
    tb_garden = ds_garden.read("electricity_mix", reset_index=False)

    #
    # Process data.
    #
    # Drop unnecessary columns.
    tb = tb_garden.drop(columns=["population"], errors="raise")

    # Create columns for specific multi-indicator charts, to handle issues with missing data. Stacked
    # charts must use these instead of the standalone columns: the standalone ones keep every accurate
    # value of a single source, while these blank any row that would show part of a mix as the whole.
    # Add columns for the stacked chart with slug "electricity-production-by-source".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[f"{source}_generation__twh" for source in STACKED_SOURCES],
        chart_slug="electricity-production-by-source",
        columns_to_fill_with_zeros=[f"{source}_generation__twh" for source in STACKED_SOURCES_FILLED_WITH_ZEROS],
    )
    tb = censor_and_check_stack(
        tb,
        columns=[f"{source}_generation__twh_chart_electricity_production_by_source" for source in STACKED_SOURCES],
        denominator="total_generation__twh",
        chart_slug="electricity-production-by-source",
        filled_with_zeros=[
            f"{source}_generation__twh_chart_electricity_production_by_source"
            for source in STACKED_SOURCES_FILLED_WITH_ZEROS
        ],
    )

    # Add columns for the stacked share chart with slug "share-elec-by-source".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[f"{source}_share_of_electricity__pct" for source in STACKED_SOURCES],
        chart_slug="share-elec-by-source",
        columns_to_fill_with_zeros=[
            f"{source}_share_of_electricity__pct" for source in STACKED_SOURCES_FILLED_WITH_ZEROS
        ],
    )
    tb = censor_and_check_stack(
        tb,
        columns=[f"{source}_share_of_electricity__pct_chart_share_elec_by_source" for source in STACKED_SOURCES],
        denominator=None,
        chart_slug="share-elec-by-source",
        filled_with_zeros=[
            f"{source}_share_of_electricity__pct_chart_share_elec_by_source"
            for source in STACKED_SOURCES_FILLED_WITH_ZEROS
        ],
    )

    # Add columns for chart with slug "elec-fossil-nuclear-renewables".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[
            "renewable_generation__twh",
            "nuclear_generation__twh",
            "fossil_generation__twh",
        ],
        chart_slug="elec-fossil-nuclear-renewables",
        columns_to_fill_with_zeros=[],
    )

    # Add columns for chart with slug "elec-mix-bar".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[
            "renewable_generation__twh",
            "nuclear_generation__twh",
            "fossil_generation__twh",
        ],
        chart_slug="elec-mix-bar",
        columns_to_fill_with_zeros=[],
    )

    # Add columns for chart with slug "per-capita-electricity-source-stacked".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[f"per_capita_{source}_generation__kwh" for source in STACKED_SOURCES],
        chart_slug="per-capita-electricity-source-stacked",
        columns_to_fill_with_zeros=[
            f"per_capita_{source}_generation__kwh" for source in STACKED_SOURCES_FILLED_WITH_ZEROS
        ],
    )
    tb = censor_and_check_stack(
        tb,
        columns=[
            f"per_capita_{source}_generation__kwh_chart_per_capita_electricity_source_stacked"
            for source in STACKED_SOURCES
        ],
        denominator="per_capita_total_generation__kwh",
        chart_slug="per-capita-electricity-source-stacked",
        filled_with_zeros=[
            f"per_capita_{source}_generation__kwh_chart_per_capita_electricity_source_stacked"
            for source in STACKED_SOURCES_FILLED_WITH_ZEROS
        ],
    )

    # Add columns for chart with slug "per-capita-electricity-fossil-nuclear-renewables".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[
            "per_capita_fossil_generation__kwh",
            "per_capita_nuclear_generation__kwh",
            "per_capita_renewable_generation__kwh",
        ],
        chart_slug="per-capita-electricity-fossil-nuclear-renewables",
        columns_to_fill_with_zeros=[],
    )

    # Also expose the Ember-only monthly table (date-indexed) as a second table in this dataset.
    tb_monthly = ds_garden.read("electricity_mix_monthly", reset_index=False)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb, tb_monthly], default_metadata=ds_garden.metadata)
    ds_grapher.save()
