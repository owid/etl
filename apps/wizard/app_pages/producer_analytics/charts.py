"""Code to generate the chart analytics bit of the app."""

import pandas as pd
import plotly.express as px
import streamlit as st
from st_aggrid import AgGrid, JsCode

from apps.wizard.app_pages.producer_analytics.data_io import get_chart_views_from_bq
from apps.wizard.app_pages.producer_analytics.utils import (
    columns_producer,
    make_grid,
)


class UIChartProducerAnalytics:
    """UI handler for chart section."""

    def __init__(self, df, producers_selection):
        self.df = _process_df(df)
        self.producers_selection = producers_selection
        self.analytics = {}

    @property
    def df_filtered(self):
        """Only include charts from selected producers."""
        # Filter the data frame to only include the selected producers.
        if len(self.producers_selection) == 0:
            # If no producers are selected, show all producer-charts.
            return self.df
        else:
            # Filter producer-charts by selected producers.
            return self.df[self.df["producer"].isin(self.producers_selection)]

    def show(self, min_date, max_date, **kwargs):
        """Render first section."""
        st.subheader("Analytics by chart")
        st.markdown("Number of views for each chart that uses data by the selected producers.")

        # Get data to display
        with st.spinner("Getting producer-specific chart analytics..."):
            df_total_daily_views, df_top_10_daily_views = self.get_chart_analytics(min_date, max_date)

        # Show chart
        self.show_chart(df_total_daily_views, df_top_10_daily_views)

        # Show table
        self.show_table(min_date, max_date)

    def get_chart_analytics(self, min_date, max_date) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Get analytics on producer's charts.

        - Total daily views of selected producers.
        - Daily views of the top 10 charts.

        NOTE: These analytics are mostly used for plotting purposes, and later for the summary.
        """
        # Get data to display
        df = self.df_filtered

        # Get total daily views of selected producers.
        grapher_urls_selected = df["chart_url"].unique().tolist()  # type: ignore
        df_total_daily_views = get_chart_views_from_bq(
            date_start=min_date,
            date_end=max_date,
            groupby=["day"],
            grapher_urls=grapher_urls_selected,
        )

        # Get daily views of the top 10 charts.
        grapher_urls_top_10 = (
            df.sort_values("views_custom", ascending=False)["chart_url"].unique().tolist()[0:10]  # type: ignore
        )
        df_top_10_daily_views = get_chart_views_from_bq(
            date_start=min_date,
            date_end=max_date,
            groupby=["day", "grapher"],
            grapher_urls=grapher_urls_top_10,
        )

        # Add analytics to object
        ## Get total number of views and average daily views.
        self.analytics["total_views"] = df_total_daily_views["renders"].sum()
        self.analytics["average_daily_views"] = df_total_daily_views["renders"].mean()
        ## Get total views of the top 10 charts in the selected date range.
        self.analytics["df_top_10_total_views"] = df_top_10_daily_views.groupby("grapher", as_index=False).agg(
            {"renders": "sum"}
        )

        return df_total_daily_views, df_top_10_daily_views

    def show_table(self, min_date, max_date):
        """Show table with analytics on producer's charts."""
        # Configure and display the second table.
        # Create a JavaScript renderer for clickable slugs.
        grapher_slug_jscode = JsCode(
            r"""
            class UrlCellRenderer {
            init(params) {
                this.eGui = document.createElement('a');
                if (params.value) {
                    // Extract the slug from the full URL.
                    const url = new URL(params.value);
                    const slug = url.pathname.split('/').pop();  // Get the last part of the path as the slug.
                    this.eGui.innerText = slug;
                    this.eGui.setAttribute('href', params.value);
                } else {
                    this.eGui.innerText = '';
                }
                this.eGui.setAttribute('style', "text-decoration:none; color:blue");
                this.eGui.setAttribute('target', "_blank");
            }
            getGui() {
                return this.eGui;
            }
            }
            """
        )

        # Define columns to be shown, including the cell renderer for "grapher".
        COLUMNS_PRODUCERS = columns_producer(min_date, max_date)
        COLUMNS_PRODUCER_CHARTS = {
            column: (
                {
                    "headerName": "Chart URL",
                    "headerTooltip": "URL of the chart in the grapher.",
                    "cellRenderer": grapher_slug_jscode,
                    "filter": "agTextColumnFilter",
                }
                if column == "chart_url"
                else COLUMNS_PRODUCERS[column]
            )
            for column in ["views_custom", "producer", "views_365d", "views_30d", "chart_url"]
        }

        # Get data to display
        df = self.df_filtered

        # Prepare the grid options.
        grid_options = make_grid(df, COLUMNS_PRODUCER_CHARTS, selection=False)

        # Display the grid.
        AgGrid(
            data=df,
            gridOptions=grid_options,
            height=500,
            width="100%",
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True,
            theme="streamlit",
            # excel_export_mode=ExcelExportMode.MANUAL,  # Doesn't work?
        )

    def show_chart(self, df_total_daily_views: pd.DataFrame, df_top_10_daily_views: pd.DataFrame):
        """Show chart with analytics on producer's charts."""
        # Prepare dataframe to plot.
        df_plot = pd.concat(
            [
                df_total_daily_views.assign(**{"grapher": "Total"}),
                df_top_10_daily_views,
            ]
        ).rename(
            columns={"grapher": "Chart slug"},
        )
        df_plot["Chart slug"] = df_plot["Chart slug"].apply(lambda x: x.split("/")[-1])
        df_plot["day"] = pd.to_datetime(df_plot["day"]).dt.strftime("%a. %Y-%m-%d")

        # Create figure to plot
        fig = px.line(
            df_plot,
            x="day",
            y="renders",
            color="Chart slug",
            title="Top 10 charts: daily views",
        ).update_layout(xaxis_title=None, yaxis_title=None)

        # Display the chart.
        st.plotly_chart(fig, use_container_width=True)


@st.cache_data(show_spinner=False)
def _process_df(df):
    def _process(df):
        # Create an expanded table with number of views per chart.
        df = df.dropna(subset=["chart_url"]).fillna(0).reset_index(drop=True)
        df = df.sort_values("views_custom", ascending=False).reset_index(drop=True)

        return df

    with st.spinner("Loading chart data. This can take few seconds..."):
        return _process(df)
