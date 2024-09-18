import base64
from pathlib import Path
from typing import cast
from urllib import parse

import requests
import streamlit as st
from owid.catalog.charts import Chart

from apps.utils.gpt import OpenAIWrapper
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
st.markdown(f"Generate data insights from a chart view, using the `{MODEL}` model.")


# FUNCTIONS
def get_thumbnail_url(grapher_url: str) -> str:
    """
    Turn https://ourworldindata.org/grapher/life-expectancy?country=~CHN"
    Into https://ourworldindata.org/grapher/thumbnail/life-expectancy.png?country=~CHN
    """
    assert grapher_url.startswith("https://ourworldindata.org/grapher/")
    parts = parse.urlparse(grapher_url)

    return f"{parts.scheme}://{parts.netloc}/grapher/thumbnail/{Path(parts.path).name}.png?{parts.query}"


def get_grapher_thumbnail(grapher_url: str) -> str:
    url = get_thumbnail_url(grapher_url)
    data = requests.get(url).content
    return f"data:image/png;base64,{base64.b64encode(data).decode('utf8')}"


def get_trajectory_prompt(base_prompt: str, slug: str) -> str:
    chart = Chart(slug)
    df = chart.get_data()
    st.warning(f"Chart has {len(df)} rows and {len(df.columns)} columns")
    if len(df.columns) > 3:
        raise DataError("This chart has more than 3 columns, which is not supported.")

    (value_col,) = df.columns.difference(["entities", "years"])
    df_s = df.round(1).query("years >= 2000").pivot(index="entities", columns="years", values=value_col).to_csv()

    title = chart.config["title"]
    subtitle = chart.config["subtitle"]

    return f"{base_prompt}\n\n---\n\n## {title}\n\n{subtitle}\n\n{df_s}"


def list_charts(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT slug FROM chart_configs WHERE JSON_EXTRACT(config, '$.isPublished')")
        return [slug for (slug,) in cur.fetchall()]


(tab1, tab2) = st.tabs(["Insight from chart", "Explain raw data"])

with tab1:
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
    conn = get_connection()
    default_prompt = """This is an indicator published by Our World In Data.

Explain the core insights present in this data, in plain, educational language.
"""
    all_charts = list_charts(conn)
    slug = st.multiselect(
        label="Grapher slug",
        options=all_charts,
        help="Introduce the URL to a Grapher URL. Query parameters work!",
        key="tab2_url",
    )

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
