import streamlit as st

from apps.wizard.app_pages.chart_diff.utils import compare_chart_configs


def st_show_conflict_resolver(config_1, config_2):
    """Conflict resolver."""
    st.warning(
        "This is under development! For now, please resolve the conflict manually by integrating the changes in production into the chart in staging server."
    )
    config_compare = compare_chart_configs(
        config_1,  # type: ignore
        config_2,
    )

    if config_compare:
        # with st.form("conflict-resolve-form"):
        # st.markdown("## Conflict resolver")
        st.markdown(
            "Find below the chart config fields that do not match. Choose the value you want to keep for each of the fields (or introduce a new one)."
        )
        # st.write(config_compare)
        for field in config_compare:
            st.markdown(f"### {field['key']}")
            col1, col2 = st.columns(2)

            with col1:
                with st.container(border=True):
                    st.write(field["value1"])
                st.button(
                    "Choose `PRODUCTION`",
                    key=f"conflict-prod-{field['key']}",
                )
            with col2:
                with st.container(border=True):
                    st.write(field["value2"])
                st.button(
                    "Choose `STAGING`",
                    key=f"conflict-stag-{field['key']}",
                )

            from streamlit_ace import st_ace

            st_ace(
                # label="Resolution",
                value=str(field["value1"]),
                language="json",
                wrap=True,
            )

            # with col2:
            #     pass
            # st.radio(
            #     f"**{field['key']}**",
            #     options=[field["value1"], field["value2"]],
            #     format_func=lambda x: f"`PROD`: {field['value1']}"
            #     if x == field["value1"]
            #     else f"`STAG`: {field['value2']}",
            #     key=f"conflict-{field['key']}",
            #     # horizontal=True,
            # )
            # st.text_input(
            #     "Custom value",
            #     label_visibility="collapsed",
            #     placeholder="Enter a custom value",
            #     key=f"conflict-custom-{field['key']}",
            # )
            # st.form_submit_button("Resolve", help="This will update the chart in the staging server.")
