import base64
from pathlib import Path
from typing import cast
from urllib import parse

import requests
import streamlit as st
from st_pages import add_indentation

from apps.utils.gpt import OpenAIWrapper

# CONFIG
st.set_page_config(
    page_title="Data insight robot",
    page_icon="🪄",
)
add_indentation()
st.title("💡 Data insighter")
st.markdown("Generate data insights from a chart view.")


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


# PROMPT
prompt = """This is a chart from Our World In Data.

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
confirmed = st.button("Generate insight")

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
            model="gpt-4-turbo",
            messages=messages,  # type: ignore
            max_tokens=3000,
            stream=True,
        )
        response = cast(str, st.write_stream(stream))
