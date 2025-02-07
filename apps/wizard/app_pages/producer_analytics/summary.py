"""Last bit, where a summary is prepared to be shared with the data producer."""

import streamlit as st


class UISummary:
    def __init__(self, ui_producer, ui_charts):
        self.ui_producer = ui_producer
        self.ui_charts = ui_charts

    def bake_summary(self, min_date, max_date):
        """Prepare summary at the end of the app."""
        # Prepare the total number of views.
        total_views_str = f"{self.ui_charts.analytics['total_views']:9,}"
        # Prepare the average daily views.
        average_views_str = f"{round(self.ui_charts.analytics['average_daily_views']):9,}"
        # Prepare a summary of the top 10 charts to be copy-pasted.
        if len(self.ui_producer.producers_selection) == 0:
            producers_selected_str = "all producers"
        elif len(self.ui_producer.producers_selection) == 1:
            producers_selected_str = self.ui_producer.producers_selection[0]
        else:
            producers_selected_str = (
                ", ".join(self.ui_producer.producers_selection[:-1])
                + " and "
                + self.ui_producer.producers_selection[-1]
            )
        # NOTE: I tried .to_string() and .to_markdown() and couldn't find a way to keep a meaningful format.
        df_summary_str = ""
        for _, row in (
            self.ui_charts.analytics["df_top_10_total_views"].sort_values("renders", ascending=False).iterrows()
        ):
            df_summary_str += f"{row['renders']:9,}" + " - " + row["grapher"] + "\n"

        # Define the content to copy.
        summary = f"""Analytics of charts using data by {producers_selected_str} between {min_date} and {max_date}:
- Total number of chart views: {total_views_str}
- Average daily chart views: {average_views_str}
- Views of top performing charts:
{df_summary_str}
        """
        return summary

    def show(self, min_date, max_date):
        # Display the content.
        st.subheader("Summary")
        st.markdown(
            """For now, to share analytics with a data producer you can so any of the following:
- **Table export**: Right-click on a cell in the above's table and export as a CSV or Excel file.
- **Chart export**: Click on the camera icon on the top right of the chart to download the chart as a PNG.
- **Copy summary**: Click on the upper right corner of the box below to copy the summary to the clipboard.
        """
        )

        summary = self.bake_summary(min_date, max_date)
        st.code(summary, language="text")
