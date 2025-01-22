import streamlit as st

from apps.wizard.app_pages.harmonizer.utils import render as render_harmonizer
from apps.wizard.etl_steps.utils import STEP_NAME_PRESENT


def render_instructions(form=None, key: str = ""):
    Instructions(form, key).render()


class Instructions:
    def __init__(self, form=None, key=""):
        self.form = form
        self.key = key

    def render(self):
        for channel in ["meadow", "garden", "grapher"]:
            if (self.form is None) or (channel in self.form.steps_to_create):
                with st.container(border=True):
                    st.markdown(f"##### **{STEP_NAME_PRESENT.get(channel, channel)}**")
                    if channel == "meadow":
                        self.render_instructions_meadow()
                    elif channel == "garden":
                        self.render_instructions_garden()
                    elif channel == "grapher":
                        self.render_instructions_grapher()

    def render_instructions_meadow(self):
        ## Run step
        st.markdown("**1) Run Meadow step**")
        if self.form is None:
            st.code(
                "uv run etl run data://meadow/namespace/version/short_name",
                language="shellSession",
                wrap_lines=True,
                line_numbers=True,
            )
            st.markdown("Use `--private` if the dataset is private.")
        else:
            st.code(
                f"uv run etl run {self.form.meadow_step_uri} {'--private' if self.form.is_private else ''}",
                language="shellSession",
                wrap_lines=True,
                line_numbers=True,
            )

    def render_instructions_garden(self):
        ## 1/ Run etl step
        st.markdown("**1) Harmonize country names**")
        self._render_harmonizer_btn()

        st.markdown("You can also run it in your terminal:")
        if self.form is None:
            st.code(
                "uv run etl harmonize data/meadow/version/short_name/table_name.feather country etl/steps/data/garden/version/short_name.countries.json",
                "shellSession",
                wrap_lines=True,
                line_numbers=True,
            )
        else:
            st.code(
                f"uv run etl harmonize data/meadow/{self.form.base_step_name}/{self.form.short_name}.feather country etl/steps/data/garden/{self.form.base_step_name}.countries.json",
                "shellSession",
                wrap_lines=True,
                line_numbers=True,
            )
        st.markdown("**2) Run Garden step**")
        st.markdown("After editing the code of your Garden step, run the following command:")
        if self.form is None:
            st.code(
                "uv run etl run data://garden/namespace/version/short_name",
                "shellSession",
                wrap_lines=True,
                line_numbers=True,
            )
            st.markdown("Use `--private` if the dataset is private.")
        else:
            st.code(
                f"uv run etl run {self.form.garden_step_uri} {'--private' if self.form.is_private else ''}",
                "shellSession",
                wrap_lines=True,
                line_numbers=True,
            )

    def render_instructions_grapher(self):
        st.markdown("**1) Run Grapher step**")
        if self.form is None:
            st.code(
                "uv run etl run data://meadow/namespace/version/short_name",
                language="shellSession",
                wrap_lines=True,
                line_numbers=True,
            )
            st.markdown("Use `--private` if the dataset is private.")
        else:
            st.code(
                f"uv run etl run {self.form.grapher_step_uri} {'--private' if self.form.is_private else ''}",
                language="shellSession",
                wrap_lines=True,
                line_numbers=True,
            )
        st.markdown("**2) Pull request**")
        st.markdown("Create a pull request in [ETL](https://github.com/owid/etl), get it reviewed and merged.")

    @st.fragment
    def _render_harmonizer_btn(self):
        key = "data_step_harmonize"
        if self.key != "":
            key = f"{key}_{self.key}"
        st.button(
            "Harmonize",
            on_click=self._render_harmonizer_dialog,
            key=key,
        )

    @st.dialog("Harmonize country names", width="large")
    def _render_harmonizer_dialog(self):
        if self.form is None:
            meadow_step = None
        else:
            meadow_step = self.form.step_uri("meadow")
        render_harmonizer(step_uri=meadow_step)
