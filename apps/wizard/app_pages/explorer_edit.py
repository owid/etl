"""Helper tool to create map brackets for all indicators in an indicator-based explorer."""

from io import StringIO

import streamlit as st

from etl.collections.explorer_legacy import ExplorerLegacy

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Explorer editor",
    page_icon="🪄",
)
st.title(":material/explore: Explorer Editor")


with st.container(border=True):
    st.subheader("IDs to Paths")
    st.markdown("Migrate all references to indicator IDs for their corresponding indicator paths.")

    uploaded_file = st.file_uploader(
        label="Upload Explorer config file",
        type=["csv", "tsv"],
    )

    if uploaded_file:
        stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
        string_data = stringio.read()

        if uploaded_file.name.endswith("csv"):
            sep = ","
        else:
            sep = "\t"

        explorer = ExplorerLegacy.from_raw_string(string_data, sep=sep)

        # explorer.convert_ids_to_etl_paths()

        # with st.popover("config"):
        #     st.dataframe(pd.DataFrame(explorer.config).T.rename(columns={0: "value"}))

        # with st.popover("graphers"):
        #     st.dataframe(explorer.df_graphers)

        # with st.popover("columns"):
        #     st.dataframe(explorer.df_columns)

        # Update
        explorer.convert_ids_to_etl_paths()

        # Downloads
        st.download_button(
            "Download `graphers` (CSV)",
            explorer._df_graphers_output.to_csv(sep=",", index=False),  # type: ignore
            file_name="graphers.csv",
        )
        if not explorer.df_columns.empty:
            st.download_button(
                "Download `columns` (CSV)",
                explorer._df_columns_output.to_csv(sep=",", index=False),  # type: ignore
                file_name="columns.csv",
            )

        filename = f"modified-{uploaded_file.name.replace('.csv', '.tsv')}"
        st.download_button("Download new config (TSV)", explorer.content, file_name=filename)
