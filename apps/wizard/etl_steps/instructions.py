import streamlit as st

from apps.wizard.app_pages.harmonizer.utils import render as render_harmonizer
from apps.wizard.etl_steps.utils import STEP_NAME_PRESENT


@st.dialog("Harmonize country names", width="large")
def render_harm(form):
    meadow_step = form.step_uri("meadow")
    render_harmonizer(meadow_step)


def render_instructions(form):
    for channel in ["meadow", "garden", "grapher"]:
        if channel in form.steps_to_create:
            with st.container(border=True):
                st.markdown(f"##### **{STEP_NAME_PRESENT.get(channel, channel)}**")
                render_instructions_step(channel, form)


def render_instructions_step(channel, form=None):
    if channel == "meadow":
        render_instructions_meadow(form)
    elif channel == "garden":
        render_instructions_garden(form)
    elif channel == "grapher":
        render_instructions_grapher(form)


def render_instructions_meadow(form=None):
    ## Run step
    st.markdown("**1) Run Meadow step**")
    if form is None:
        st.code(
            "uv run etl run data://meadow/namespace/version/short_name",
            language="shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
        st.markdown("Use `--private` if the dataset is private.")
    else:
        st.code(
            f"uv run etl run {form.meadow_step_uri} {'--private' if form.is_private else ''}",
            language="shellSession",
            wrap_lines=True,
            line_numbers=True,
        )


def render_instructions_garden(form=None):
    ## 1/ Run etl step
    st.markdown("**1) Harmonize country names**")
    st.button("Harmonize", on_click=lambda form=form: render_harm(form))
    st.markdown("You can also run it in your terminal:")
    if form is None:
        st.code(
            "uv run etl harmonize data/meadow/version/short_name/table_name.feather country etl/steps/data/garden/version/short_name.countries.json",
            "shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
    else:
        st.code(
            f"uv run etl harmonize data/meadow/{form.base_step_name}/{form.short_name}.feather country etl/steps/data/garden/{form.base_step_name}.countries.json",
            "shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
    st.markdown("**2) Run Garden step**")
    st.markdown("After editing the code of your Garden step, run the following command:")
    if form is None:
        st.code(
            "uv run etl run data://garden/namespace/version/short_name",
            "shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
        st.markdown("Use `--private` if the dataset is private.")
    else:
        st.code(
            f"uv run etl run {form.garden_step_uri} {'--private' if form.is_private else ''}",
            "shellSession",
            wrap_lines=True,
            line_numbers=True,
        )


def render_instructions_grapher(form=None):
    st.markdown("**1) Run Grapher step**")
    if form is None:
        st.code(
            "uv run etl run data://meadow/namespace/version/short_name",
            language="shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
        st.markdown("Use `--private` if the dataset is private.")
    else:
        st.code(
            f"uv run etl run {form.grapher_step_uri} {'--private' if form.is_private else ''}",
            language="shellSession",
            wrap_lines=True,
            line_numbers=True,
        )
    st.markdown("**2) Pull request**")
    st.markdown("Create a pull request in [ETL](https://github.com/owid/etl), get it reviewed and merged.")
