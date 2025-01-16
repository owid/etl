"""First part of the app, where user is asked to fill in a short search form."""

import streamlit as st

from apps.wizard.app_pages.producer_analytics.utils import AUXILIARY_STEPS, MIN_DATE, TODAY
from apps.wizard.utils.components import st_horizontal


def render_selection():
    with st.container(border=True):
        st.markdown(
            f"Select a custom date range (note that this metric started to be recorded on {MIN_DATE.strftime('%Y-%m-%d')})."
        )

        with st_horizontal(vertical_alignment="center"):
            # Create input fields for minimum and maximum dates.
            min_date = st.date_input(
                "Select minimum date",
                value=MIN_DATE,
                key="min_date",
                format="YYYY-MM-DD",
                min_value=MIN_DATE,
            ).strftime(  # type: ignore
                "%Y-%m-%d"
            )
            max_date = st.date_input(
                "Select maximum date",
                value=TODAY,
                key="max_date",
                format="YYYY-MM-DD",
                min_value=min_date,
            ).strftime(  # type: ignore
                "%Y-%m-%d"
            )
            exclude_auxiliary_steps = st.checkbox(
                "Exclude auxiliary steps (e.g. population)",
                False,
                help="Exclude steps that are commonly used as auxiliary data, so they do not skew the analytics in favor of a few producers. But note that this will exclude all uses of these steps, even when they are the main datasets (not auxiliary). Auxiliary steps are:\n- "
                + "\n- ".join(sorted(f"`{s}`" for s in AUXILIARY_STEPS)),
            )

    if exclude_auxiliary_steps:
        # If the user wants to exclude auxiliary steps, take the default list of excluded steps.
        excluded_steps = AUXILIARY_STEPS
    else:
        # Otherwise, do not exclude any steps.
        excluded_steps = []

    return min_date, max_date, excluded_steps
