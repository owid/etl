"""Code to generate the table with producer analytics."""

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode

from apps.wizard.app_pages.producer_analytics.utils import columns_producer, make_grid
from apps.wizard.utils.components import st_cache_data


class UIProducerAnalytics:
    """UI handler for producer section."""

    def __init__(self, df):
        self.df = _process_df(df)
        self.producers_selection = []

    def show(self, min_date, max_date, **kwargs):
        """Render first section."""
        st.markdown(
            "Total number of charts and chart views for each producer. Producers selected in this table will be used to filter the producer-charts table below."
        )

        self.producers_selection = self.show_table(min_date, max_date)

    def show_table(self, min_date, max_date):  # -> list[Any] | Any:
        """Render the table with producers analytics."""
        # Define columns UI.
        COLUMNS_PRODUCERS = columns_producer(min_date, max_date)
        columns = COLUMNS_PRODUCERS | {
            "views_per_chart": {
                "headerName": "Views per chart",
                "headerTooltip": "Average number of views of a chart using data from this producer.",
            }
        }
        # Configure individual columns with specific settings.
        grid_options = make_grid(self.df, columns, selection=True)
        # grid_options["autoSizeStrategy"] = {
        #     "type": "fitCellContents",
        #     "skipHeader": False,
        # }

        # st.write(grid_options)
        # Custom CSS to ensure the table stretches across the page.
        custom_css = {
            ".ag-theme-streamlit": {
                "max-width": "100% !important",
                "width": "100% !important",
                "margin": "0 auto !important",  # Centers the grid horizontally.
            },
        }
        # Display the grid table with the updated grid options.
        grid_response = AgGrid(
            data=self.df,
            gridOptions=grid_options,
            # height=500,
            width="100%",
            update_mode=GridUpdateMode.MODEL_CHANGED,
            fit_columns_on_grid_load=True,  # Automatically adjust columns when the grid loads.
            allow_unsafe_jscode=True,
            theme="streamlit",
            custom_css=custom_css,
            # excel_export_mode=ExcelExportMode.MANUAL,  # Doesn't work?
            # columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        )

        # Get the selected producers from the first table.
        df = grid_response["selected_rows"]
        if df is None:
            return []

        return df["producer"].tolist()


@st_cache_data(
    custom_text="Loading producer data from various sources (Big Query, MySQL, etc.) This can take few seconds..."
)
def _process_df(df):
    # Group by producer and get the full list of chart slugs for each producer.
    df = df.groupby("producer", observed=True, as_index=False).agg(
        {
            "chart_url": lambda x: [item for item in x if pd.notna(item)],  # Filter out NaN values
            "views_365d": "sum",
            "views_30d": "sum",
            "views_custom": "sum",
        }
    )
    df["n_charts"] = df["chart_url"].apply(len)

    # Views per chart
    df["views_per_chart"] = (df["views_custom"] / df["n_charts"]).astype(int)

    # Check if lists are unique. If not, make them unique in the previous line.
    error = "Duplicated chart slugs found for a given producer."
    assert df["chart_url"].apply(lambda x: len(x) == len(set(x))).all(), error

    # Drop unnecessary columns.
    df = df.drop(columns=["chart_url"])

    # Sort conveniently.
    df = df.sort_values(["views_custom"], ascending=False).reset_index(drop=True)

    return df
