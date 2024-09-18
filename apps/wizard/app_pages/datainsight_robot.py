from typing import cast

import streamlit as st
from owid.catalog.charts import Chart

from apps.utils.gpt import OpenAIWrapper
from apps.wizard.app_pages.insights import (
    fetch_chart_data,
    get_grapher_thumbnail,
    get_thumbnail_url,
    list_charts,
)
from etl.db import get_connection


class DataError(Exception):
    pass


MODEL = "gpt-4o"

# CONFIG
st.set_page_config(
    page_title="Data insight robot",
    page_icon="🪄",
)
st.title(":material/lightbulb: Data insighter")


def get_trajectory_prompt(base_prompt: str, slug: str) -> str:
    chart = Chart(slug)
    df = chart.get_data()

    date_col = "years" if "years" in df.columns else "dates"

    # shrink it
    df = df.round(1)
    if "years" in df.columns:
        st.warning("NOTE: We are only looking at data from the year 2000 onwards")
        df = df.query("years >= 2000")

    if len(df.columns) == 3:
        # shrink more via a pivot
        (value_col,) = df.columns.difference(["entities", date_col])
        df = df.pivot(index="entities", columns=date_col, values=value_col)

    df_s = df.to_csv()

    title = chart.config["title"]
    subtitle = chart.config["subtitle"]

    return f"{base_prompt}\n\n---\n\n## {title}\n\n{subtitle}\n\n{df_s}"


(tab1, tab2) = st.tabs(["Insight from chart", "Explain raw data"])

with tab1:
    st.markdown(
        f"Generate data insights from a chart view, using the `{MODEL}` model. Choose what to describe by selecting the chart and the countries and years you care about, then paste the link in here."
    )
    # PROMPT
    default_prompt = """This is a chart from Our World In Data.

I'd like you to write a data insight for me. Data insights are a short format that explains the main point of a chart. They are usually a title sentence and then three concise paragraphs that are written in a way that is easy to understand for a general audience.

Here is a recent example for a chart about primary education in Morocco

--
Primary education in Morocco: from less than half to nearly universal attendance

In the 1970s, less than half of Morocco's primary-age children attended school. Today, nearly every child is in school, with enrolment rates having soared to over 99%, according to data published by UNESCO.

Though this is a remarkable achievement, there's still room for improvement in education quality. Only about a third of these students achieve basic reading comprehension by the end of primary school.

Focusing on getting children into school has been crucial. The next step is to enhance the quality of education to ensure they not only attend but also learn and thrive.
--

Here is an example for maternal deaths where the chart shows the number of maternal deaths per 100,000 live births for the whole world that is falling over time:

--
Maternal deaths have halved in the last 35 years

A woman dying when she is giving birth to her child is one of the greatest tragedies imaginable.

Every year, 300,000 women die from pregnancy-related causes.

Fortunately, the world has made continuous progress, and such tragic deaths have become much rarer, as the chart shows. The WHO has published data since 1985. Since then, the number of maternal deaths has halved.
--

Please write a data insight for the given chart. Use simple language and short paragraphs. Provide the title as a markdown header-2.
    """
    # Ask user for URL & commit
    grapher_url = st.text_input(
        label="Grapher URL",
        placeholder="https://ourworldindata.org/grapher/life-expectancy?country=~CHN",
        help="Introduce the URL to a Grapher URL. Query parameters work!",
        key="url",
    )
    if grapher_url != "" and not grapher_url.startswith("https://ourworldindata.org/grapher/"):
        st.warning("Please introduce a valid Grapher URL")

    with st.expander("Edit the prompt"):
        prompt = st.text_area(
            label="Prompt",
            value=default_prompt,
            key="prompt",
        )
    confirmed = st.button(
        "Generate insight",
        disabled=grapher_url == "" or not grapher_url.startswith("https://ourworldindata.org/grapher/"),
    )

    # Action if user clicks on 'generate'
    if confirmed:
        # Opena AI (do first to catch possible errors in ENV)
        api = OpenAIWrapper()

        # Get thumbnail for chart
        thumb_url = get_thumbnail_url(grapher_url)
        hex = get_grapher_thumbnail(grapher_url)

        # Show image
        st.image(thumb_url)

        # Prepare messages for Insighter
        messages = [
            {
                "role": "system",
                "content": [
                    {"type": "text", "text": prompt},
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": hex},
                    }
                ],
            },
        ]

        with st.chat_message("assistant"):
            # Ask GPT (stream)
            stream = api.chat.completions.create(
                model=MODEL,
                messages=messages,  # type: ignore
                max_tokens=3000,
                stream=True,
            )
            response = cast(str, st.write_stream(stream))

with tab2:
    st.markdown(
        f"Generate insights from the raw data underlying a chart, using the `{MODEL}` model. In this case, ChatGPT is looking at all countries and all time periods at once."
    )
    conn = get_connection()
    default_prompt = """This is an indicator published by Our World In Data.

Explain the core insights present in this data, in plain, educational language.
"""
    all_charts = list_charts(conn)
    slugs = st.multiselect(
        label="Grapher slug",
        options=all_charts,
        help="Introduce the URL to a Grapher URL. Query parameters work!",
        key="tab2_url",
    )
    slug = None if len(slugs) == 0 else slugs[0]

    with st.expander("Edit the prompt"):
        prompt = st.text_area(
            label="Prompt",
            value=default_prompt,
            key="tab2_prompt",
        )
    confirmed = st.button(
        "Generate insight",
        disabled=slug is None,
        key="tab2_button",
    )

    # Action if user clicks on 'generate'
    if confirmed and slug is not None:
        # Opena AI (do first to catch possible errors in ENV)
        api = OpenAIWrapper()

        df = fetch_chart_data(conn, slug)
        prompt_with_data = get_trajectory_prompt(prompt, slug)  # type: ignore

        # Prepare messages for Insighter
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt_with_data},
                ],
            },
        ]

        with st.chat_message("assistant"):
            # Ask GPT (stream)
            stream = api.chat.completions.create(
                model=MODEL,
                messages=messages,  # type: ignore
                max_tokens=3000,
                stream=True,
            )
            response = cast(str, st.write_stream(stream))
