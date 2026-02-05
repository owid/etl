"""MDIM Creator - Create MDIMs with a guided 4-stage wizard.

This wizard guides users through creating MDIM collections:
1. Data Source Selection - Pick a grapher step
2. Metadata Configuration - Set title, tags, defaults
3. Dimension Mapping - Map columns to dimensions
4. Preview & Generate - Review and generate files
"""

import streamlit as st

from apps.wizard.app_pages.mdim_creator.stages import (
    dataset_picker,
    dimension_mapper,
    metadata_form,
    preview,
)
from apps.wizard.utils import set_states
from apps.wizard.utils.components import st_title_with_expert

st.set_page_config(
    page_title="Wizard: MDIM Creator",
    page_icon=":material/grid_view:",
    layout="wide",
)

# Initialize session state
STAGES = ["dataset", "metadata", "dimensions", "preview"]
STAGE_NAMES = {
    "dataset": "1. Data Source",
    "metadata": "2. Metadata",
    "dimensions": "3. Dimensions",
    "preview": "4. Preview & Generate",
}


def init_session_state() -> None:
    """Initialize session state variables."""
    set_states(
        {
            "mdim_stage": "dataset",
            "mdim_dataset_loaded": False,
            "mdim_metadata_complete": False,
            "mdim_dimensions_complete": False,
            "mdim_generated": False,
            # Data storage
            "mdim_selected_step": None,
            "mdim_table": None,
            "mdim_columns": [],
            "mdim_metadata": {},
            "mdim_dimension_mapping": [],
            "mdim_common_config": {},
        },
        also_if_not_exists=True,
    )


def render_stage_indicator() -> None:
    """Render a compact stage progress indicator."""
    current_stage = st.session_state.mdim_stage
    current_idx = STAGES.index(current_stage)

    # Build compact breadcrumb: "✓ Data Source → ✓ Metadata → **Dimensions**"
    parts = []
    for i, name in enumerate(STAGE_NAMES.values()):
        # Remove number prefix for cleaner look
        clean_name = name.split(". ", 1)[-1]
        if i < current_idx:
            parts.append(f":green[✓ {clean_name}]")
        elif i == current_idx:
            parts.append(f"**{clean_name}**")
        # Don't show future stages
    st.caption(" → ".join(parts) + f"  ·  Step {current_idx + 1} of {len(STAGES)}")


def can_proceed_to_stage(stage: str) -> bool:
    """Check if we can proceed to a given stage."""
    if stage == "dataset":
        return True
    elif stage == "metadata":
        return st.session_state.mdim_dataset_loaded
    elif stage == "dimensions":
        return st.session_state.mdim_metadata_complete
    elif stage == "preview":
        return st.session_state.mdim_dimensions_complete
    return False


def go_to_stage(stage: str) -> None:
    """Navigate to a specific stage."""
    if can_proceed_to_stage(stage):
        st.session_state.mdim_stage = stage


def render_navigation() -> None:
    """Render navigation buttons."""
    current_stage = st.session_state.mdim_stage
    current_idx = STAGES.index(current_stage)

    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        if current_idx > 0:
            if st.button(
                f":material/arrow_back: {STAGE_NAMES[STAGES[current_idx - 1]]}",
                use_container_width=True,
            ):
                go_to_stage(STAGES[current_idx - 1])
                st.rerun()

    with col3:
        if current_idx < len(STAGES) - 1:
            next_stage = STAGES[current_idx + 1]
            can_proceed = can_proceed_to_stage(next_stage)
            if st.button(
                f"{STAGE_NAMES[next_stage]} :material/arrow_forward:",
                use_container_width=True,
                disabled=not can_proceed,
                type="primary" if can_proceed else "secondary",
            ):
                go_to_stage(next_stage)
                st.rerun()


# Initialize session state
init_session_state()

# Title
st_title_with_expert("MDIM Creator", icon=":material/grid_view:")

# Stage indicator at the top
render_stage_indicator()

# Render current stage
current_stage = st.session_state.mdim_stage

if current_stage == "dataset":
    dataset_picker.render()
elif current_stage == "metadata":
    metadata_form.render()
elif current_stage == "dimensions":
    dimension_mapper.render()
elif current_stage == "preview":
    preview.render()

# Navigation
render_navigation()
