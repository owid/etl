import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.producer_analytics.charts import (
    get_producer_analytics_per_chart,
    get_producer_charts_analytics,
    plot_chart_analytics,
    show_producer_charts_grid,
)
from apps.wizard.app_pages.producer_analytics.producer import get_producer_analytics_per_producer, show_producers_grid
from apps.wizard.app_pages.producer_analytics.selection import render_selection
from apps.wizard.app_pages.producer_analytics.summary import prepare_summary

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Producer analytics",
    layout="wide",
    page_icon="🪄",
)


########################################################################################################################
# 0/ TITLE, SELECTION BOX
########################################################################################################################

# Streamlit app layout.
st.title(":material/analytics: Producer analytics")
st.markdown("Explore analytics of data producers.")

# Selection box
min_date, max_date, excluded_steps = render_selection()

########################################################################################################################
# 1/ GET DATA
########################################################################################################################

df_expanded = get_producer_charts_analytics(
    min_date=min_date,
    max_date=max_date,
    excluded_steps=excluded_steps,
)

########################################################################################################################
# 2/ PRODUCER ANALYTICS: Display main table, with analytics per producer.
# Allow the user to select a subset of producers.
########################################################################################################################
st.header("Analytics by producer")
st.markdown(
    "Total number of charts and chart views for each producer. Producers selected in this table will be used to filter the producer-charts table below."
)

# Load table content and select only columns to be shown.
with st.spinner("Loading producer data. We are accessing various databases. This can take few seconds..."):
    df_producers = get_producer_analytics_per_producer(df_expanded)

# Prepare and display the grid table with producer analytics.
producers_selected = show_producers_grid(
    df_producers=df_producers,
    min_date=min_date,
    max_date=max_date,
)

########################################################################################################################
# 2/ CHART ANALYTICS: Display a chart with the total number of daily views, and the daily views of the top performing charts.
########################################################################################################################
st.header("Analytics by chart")
st.markdown("Number of views for each chart that uses data by the selected producers.")

# Load detailed analytics per producer-chart.
with st.spinner("Loading chart data. This can take few seconds..."):
    df_producer_charts = get_producer_analytics_per_chart(df_expanded)

# Get the selected producers from the first table.
if len(producers_selected) == 0:
    # If no producers are selected, show all producer-charts.
    df_producer_charts_filtered = df_producer_charts
else:
    # Filter producer-charts by selected producers.
    df_producer_charts_filtered = df_producer_charts[df_producer_charts["producer"].isin(producers_selected)]

# Show chart with chart analytics, and get some summary data.
total_views, average_daily_views, df_top_10_total_views = plot_chart_analytics(
    df_producer_charts_filtered, min_date, max_date
)

# Show table
show_producer_charts_grid(df_producer_charts_filtered, min_date, max_date)

########################################################################################################################
# 3/ SUMMARY: Display a summary to be shared with the data producer.
########################################################################################################################

# Prepare the summary to be copy-pasted.
summary = prepare_summary(
    df_top_10_total_views=df_top_10_total_views,
    producers_selected=producers_selected,
    total_views=total_views,
    average_daily_views=average_daily_views,
    min_date=min_date,
    max_date=max_date,
)

# Display the content.
st.markdown(
    """## Summary for data producers

For now, to share analytics with a data producer you can so any of the following:
- **Table export**: Right-click on a cell in the above's table and export as a CSV or Excel file.
- **Chart export**: Click on the camera icon on the top right of the chart to download the chart as a PNG.
- **Copy summary**: Click on the upper right corner of the box below to copy the summary to the clipboard.
"""
)
st.code(summary, language="text")
