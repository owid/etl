"""Stage 3: Dimension Mapping.

This is the core feature - an interactive table that maps indicators to dimensions.
Users can:
- See all columns from the dataset
- Assign indicator names
- Map columns to dimension values
- Add/remove dimensions
"""

from typing import Any

import pandas as pd
import streamlit as st


def get_column_dimensions(tb: Any, col: str) -> dict[str, str]:
    """Extract dimension metadata from a column if available."""
    try:
        if hasattr(tb[col], "m") and hasattr(tb[col].m, "dimensions"):
            dims = tb[col].m.dimensions
            if dims:
                return dims
    except Exception:
        pass
    return {}


def get_indicator_name(tb: Any, col: str) -> str:
    """Get the indicator name from column metadata or use column name."""
    try:
        if hasattr(tb[col], "m"):
            if hasattr(tb[col].m, "original_short_name") and tb[col].m.original_short_name:
                return tb[col].m.original_short_name
            if hasattr(tb[col].m, "presentation"):
                pres = tb[col].m.presentation
                if hasattr(pres, "title_short") and pres.title_short:
                    return pres.title_short
    except Exception:
        pass
    return col


def extract_dimensions_from_columns(tb: Any) -> dict[str, set[str]]:
    """Extract all unique dimensions and their values from column metadata."""
    dimensions: dict[str, set[str]] = {}

    for col in tb.columns:
        if col in ["country", "year", "entity_id", "entity_name"]:
            continue
        col_dims = get_column_dimensions(tb, col)
        for dim_name, dim_value in col_dims.items():
            if dim_name not in dimensions:
                dimensions[dim_name] = set()
            dimensions[dim_name].add(dim_value)

    return dimensions


def infer_indicator_name_from_column(col_name: str) -> str:
    """Infer indicator name from column name by removing common suffixes/patterns."""
    # Common patterns: column_name_low, column_name_high, column_name_per_capita, etc.
    suffixes_to_remove = [
        "_low",
        "_high",
        "_best",
        "_median",
        "_mean",
        "_per_capita",
        "_per_million",
        "_per_100k",
        "_absolute",
        "_relative",
        "_pct",
        "_rate",
        "_male",
        "_female",
        "_total",
        "_all",
    ]

    result = col_name
    for suffix in suffixes_to_remove:
        if result.endswith(suffix):
            result = result[: -len(suffix)]
            break

    return result if result else col_name


def build_initial_mapping(tb: Any) -> list[dict[str, Any]]:
    """Build initial dimension mapping from table columns."""
    mapping = []

    # Get index columns to exclude
    index_cols = {"country", "year", "entity_id", "entity_name"}
    if hasattr(tb, "index") and hasattr(tb.index, "names"):
        index_cols.update(name for name in tb.index.names if name)

    for col in tb.columns:
        if col in index_cols:
            continue

        # Get existing dimensions
        col_dims = get_column_dimensions(tb, col)

        # Get indicator name
        indicator_name = get_indicator_name(tb, col)
        if indicator_name == col:
            indicator_name = infer_indicator_name_from_column(col)

        row = {
            "column": col,
            "indicator_name": indicator_name,
            "include": True,
            **col_dims,
        }
        mapping.append(row)

    return mapping


def render() -> None:
    """Render the dimension mapper stage."""
    tb = st.session_state.get("mdim_table")

    if tb is None:
        st.error("No table loaded. Please go back to Step 1 and select a dataset.")
        return

    # Initialize or get existing mapping
    if not st.session_state.mdim_dimension_mapping:
        st.session_state.mdim_dimension_mapping = build_initial_mapping(tb)

    mapping = st.session_state.mdim_dimension_mapping

    # Extract existing dimensions
    existing_dims = extract_dimensions_from_columns(tb)
    all_dim_names = list(existing_dims.keys())

    # Toolbar row: filter + add dimension + bulk actions
    col1, col2, col3, col4 = st.columns([2, 2, 1, 1])

    with col1:
        filter_text = st.text_input(
            "Filter columns",
            placeholder="Type to filter...",
            key="dim_filter",
            label_visibility="collapsed",
        )

    with col2:
        new_dim = st.text_input(
            "Add dimension",
            placeholder="New dimension name",
            key="new_dimension_input",
            label_visibility="collapsed",
        )

    with col3:
        if st.button("Add dim", disabled=not new_dim, use_container_width=True):
            if new_dim and new_dim not in all_dim_names:
                all_dim_names.append(new_dim)
                for row in mapping:
                    row[new_dim] = ""
                st.rerun()

    with col4:
        # Bulk toggle for filtered rows
        if st.button("Toggle all", use_container_width=True, help="Toggle include for visible rows"):
            for row in mapping:
                if not filter_text or filter_text.lower() in row["column"].lower():
                    row["include"] = not row.get("include", True)
            st.session_state.mdim_dimension_mapping = mapping
            st.rerun()

    # Convert mapping to DataFrame for editing
    df = pd.DataFrame(mapping)

    # Ensure all dimension columns exist
    for dim in all_dim_names:
        if dim not in df.columns:
            df[dim] = ""

    # Reorder columns: column, indicator_name, include, then dimensions
    base_cols = ["column", "indicator_name", "include"]
    dim_cols = [c for c in all_dim_names if c in df.columns]
    other_cols = [c for c in df.columns if c not in base_cols and c not in dim_cols]
    ordered_cols = base_cols + dim_cols + other_cols
    df = df[[c for c in ordered_cols if c in df.columns]]

    # Apply filter
    if filter_text:
        mask = df["column"].str.contains(filter_text, case=False, na=False)
        display_df = df[mask].copy()
    else:
        display_df = df

    # Configure column settings
    column_config = {
        "column": st.column_config.TextColumn("Column", disabled=True, width="medium"),
        "indicator_name": st.column_config.TextColumn("Indicator", width="medium"),
        "include": st.column_config.CheckboxColumn("Inc", default=True, width="small"),
    }

    # Add dimension columns with known choices
    for dim in dim_cols:
        known_choices = list(existing_dims.get(dim, set()))
        if known_choices:
            column_config[dim] = st.column_config.SelectboxColumn(
                dim.replace("_", " ").title(),
                options=[""] + sorted(known_choices),
                width="small",
            )
        else:
            column_config[dim] = st.column_config.TextColumn(
                dim.replace("_", " ").title(),
                width="small",
            )

    # Show count if filtered
    if filter_text:
        st.caption(f"Showing {len(display_df)} of {len(df)} columns")

    edited_df = st.data_editor(
        display_df,
        column_config=column_config,
        use_container_width=True,
        num_rows="fixed",
        hide_index=True,
        key="dimension_mapping_editor",
        height=400,
    )

    # Merge edits back into full dataframe
    if filter_text:
        df.update(edited_df)
        st.session_state.mdim_dimension_mapping = df.to_dict("records")
    else:
        st.session_state.mdim_dimension_mapping = edited_df.to_dict("records")

    # Compact summary
    full_df = pd.DataFrame(st.session_state.mdim_dimension_mapping)
    included = full_df["include"].sum() if "include" in full_df.columns else len(full_df)
    indicators = full_df[full_df["include"]]["indicator_name"].nunique() if "include" in full_df.columns else 0

    dim_parts = []
    for dim in dim_cols:
        if dim in full_df.columns:
            values = full_df[full_df["include"]][dim].dropna().unique()
            values = [v for v in values if v]
            if values:
                dim_parts.append(f"{dim}: {len(values)}")

    # Calculate views
    if dim_parts:
        from functools import reduce
        from operator import mul

        n_views = reduce(
            mul,
            [
                len([v for v in full_df[full_df["include"]][d].dropna().unique() if v])
                for d in dim_cols
                if d in full_df.columns
            ],
            1,
        )
        n_views = max(n_views, 1) * indicators
    else:
        n_views = included

    summary = f"**{included}** columns, **{indicators}** indicators → **{n_views}** views"
    if dim_parts:
        summary += f"  ·  Dimensions: {', '.join(dim_parts)}"
    st.caption(summary)

    # Validation
    is_valid = included > 0 and indicators > 0
    missing = full_df[full_df["include"] & (full_df["indicator_name"] == "")]["column"].tolist()
    if missing:
        st.warning(f"Missing indicator names: {', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}")
        is_valid = False

    st.session_state.mdim_dimensions_complete = is_valid
