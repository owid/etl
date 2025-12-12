#
#  plotting.py
#
"""OWID chart plotting utilities for Table and Variable."""

from typing import Any, Literal

import pandas as pd

from owid.grapher import Chart

# Common column names for auto-detection
ENTITY_COLUMNS = ["country", "entity", "entities", "location", "region"]
TIME_COLUMNS = ["year", "years", "date", "time"]


def create_owid_chart(
    df: pd.DataFrame,
    y: str,
    x: str | None = None,
    entity: str | None = None,
    kind: Literal["line", "bar", "scatter"] = "line",
    stacked: bool = False,
    title: str | None = None,
    unit: str | None = None,
    max_entities: int = 10,
    entities: list[str] | None = None,
    enable_map: bool = False,
) -> Chart:
    """Create an OWID grapher chart from a DataFrame.

    This is the core charting function used by both Variable.plot_owid and Table.plot_owid.

    Args:
        df: DataFrame with the data to plot (should have entity/time columns if needed).
        y: Column name for y-axis values.
        x: Column name for x-axis. If None, auto-detected from TIME_COLUMNS.
        entity: Column name for entity grouping. If None, auto-detected from ENTITY_COLUMNS.
        kind: Type of plot ("line", "bar", "scatter"). Defaults to "line".
        stacked: Whether to stack bars in bar charts. Defaults to False.
        title: Chart title.
        unit: Unit for y-axis label.
        max_entities: Maximum entities to show initially. Defaults to 10.
        entities: Specific entities to show. Overrides max_entities auto-selection.
        enable_map: Whether to enable the map tab in the chart. Defaults to False.

    Returns:
        owid.grapher.Chart object.
    """
    all_cols = list(df.columns)

    # Auto-detect entity column
    if entity is None:
        for col in ENTITY_COLUMNS:
            if col in all_cols:
                entity = col
                break

    # Auto-detect time column
    if x is None:
        for col in TIME_COLUMNS:
            if col in all_cols:
                x = col
                break

    # Determine which entities to select
    selected_entities: list[str] | None = None
    if entities is not None:
        selected_entities = entities
    elif entity and entity in df.columns:
        unique_entities = df[entity].unique()
        if len(unique_entities) > max_entities:
            if kind == "scatter":
                # For scatter, pick entities with most data points
                entity_counts = df[entity].value_counts()
                selected_entities = entity_counts.head(max_entities).index.tolist()
            else:
                # For line/bar, pick top entities by last value
                last_values = df.groupby(entity)[y].last().abs().sort_values(ascending=False)
                selected_entities = last_values.head(max_entities).index.tolist()

    # Create the chart
    chart = Chart(df)

    # Apply the appropriate mark type
    if kind == "line":
        chart = chart.mark_line()
    elif kind == "bar":
        chart = chart.mark_bar(stacked=stacked)
    elif kind == "scatter":
        chart = chart.mark_scatter()
    else:
        raise ValueError(f"Unknown chart kind: {kind}. Use 'line', 'bar', or 'scatter'.")

    # Build encode parameters
    encode_params: dict[str, Any] = {"y": y}
    if x:
        encode_params["x"] = x
    if entity:
        encode_params["entity"] = entity

    chart = chart.encode(**encode_params)

    # Select entities and enable picker if needed
    if selected_entities is not None:
        chart = chart.select(entities=selected_entities)
        chart = chart.interact(entity_control=True, enable_map=enable_map)
    elif enable_map:
        chart = chart.interact(enable_map=True)

    # Set title
    if title:
        chart = chart.label(title=title)

    # Set y-axis unit
    if unit:
        chart = chart.yaxis(unit=unit)

    return chart
