import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.producer_analytics.charts import UIChartProducerAnalytics
from apps.wizard.app_pages.producer_analytics.data_io import get_analytics
from apps.wizard.app_pages.producer_analytics.producer import UIProducerAnalytics
from apps.wizard.app_pages.producer_analytics.selection import render_selection
from apps.wizard.app_pages.producer_analytics.summary import UISummary

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

df = get_analytics(
    min_date=min_date,
    max_date=max_date,
    excluded_steps=excluded_steps,
)

########################################################################################################################
# 2/ PRODUCER ANALYTICS: Display main table, with analytics per producer.
# Allow the user to select a subset of producers.
########################################################################################################################

ui_producer = UIProducerAnalytics(df)
ui_producer.show(min_date, max_date)

########################################################################################################################
# 3/ CHART ANALYTICS: Display a chart with the total number of daily views, and the daily views of the top performing charts.
########################################################################################################################
ui_charts = UIChartProducerAnalytics(df, ui_producer.producers_selection)
ui_charts.show(min_date, max_date)

########################################################################################################################
# 3/ SUMMARY: Display a summary to be shared with the data producer.
########################################################################################################################

# Prepare the summary to be copy-pasted.
ui_summary = UISummary(ui_producer, ui_charts)
ui_summary.show(min_date, max_date)
