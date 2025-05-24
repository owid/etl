import random

import streamlit as st

from apps.wizard.utils.components import url_persist


def truncate_lines(s: str, max_lines: int) -> str:
    """
    Truncate a string to a maximum number of lines.
    """
    lines = s.splitlines()
    if len(lines) > max_lines:
        st.warning(f"The diff is too long to display in full. Showing only the first {max_lines} lines.")
        return "\n".join(lines[:max_lines]) + "\n... (truncated)"
    return s


def _extract_all_dimensions(explorer_views: list[dict]) -> dict[str, list]:
    dim_names = list(explorer_views[0].keys())

    # Extract all unique dimensions across views
    all_dimensions = {dim: set() for dim in dim_names}
    for view in explorer_views:
        for dim in dim_names:
            all_dimensions[dim].add(view[dim])

    # Convert sets to lists for selectboxes
    return {dim: sorted(list(values)) for dim, values in all_dimensions.items()}


def _display_view_options(slug: str, views: list[dict]) -> dict:
    """Display explorer view options UI and return the selected view."""
    all_dimensions = _extract_all_dimensions(views)

    st.subheader("Select Explorer View Options")

    # Create random view button
    if st.button(f"🎲 Random view ({len(views)} views available)"):
        # Select a random view from views
        if views:
            random_view = random.choice(views)
            # Update session state with the random view values
            for dim, val in random_view.items():
                st.session_state[f"{slug}_{dim}"] = val
            # Rerun to apply the changes
            st.rerun(scope="fragment")

    # Arrange selectboxes horizontally using columns
    cols = st.columns(len(all_dimensions)) if all_dimensions else []

    selected_options = {}
    for i, (dim, values) in enumerate(all_dimensions.items()):
        selected_options[dim] = url_persist(cols[i].selectbox)(f"{dim}", options=values, key=f"{slug}_{dim}")

    view = selected_options if selected_options else (views[0] if views else {})

    # Check if the selected combination exists in any of the views
    combination_exists = False
    for explorer_view in views:
        if all(dim in explorer_view and explorer_view[dim] == val for dim, val in view.items()):
            combination_exists = True
            break

    # Display warning if combination doesn't exist
    if not combination_exists and view:
        st.warning(
            "⚠️ This specific combination of options does not exist in the explorer views. The explorer may show unexpected results."
        )

    return view


def _fill_missing_dimensions(views: list[dict]) -> list[dict]:
    """
    Fill missing dimensions in views with '-'.
    This is to ensure that all views have the same dimensions for comparison.
    """
    # If view doesn't have all dimensions, use '-'
    dim_names = {n for v in views for n in v.keys()}
    for view in views:
        for dim in dim_names:
            # If dimension is missing in a view, use '-'
            if dim not in view:
                view[dim] = "-"
    return views


def _set_page_config(title: str):
    """Set the page config."""
    # Config
    st.set_page_config(
        page_title=f"Wizard: {title}",
        layout="wide",
        page_icon="🪄",
        initial_sidebar_state="collapsed",
        menu_items={
            "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
        },
    )
