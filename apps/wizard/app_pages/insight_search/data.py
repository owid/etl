import datetime as dt
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st

from apps.wizard.utils.embeddings import Doc
from etl.db import read_sql


@dataclass
class Insight(Doc):
    id: int
    title: str
    raw_text: str
    authors: list[str]
    url: Optional[str]
    url_img_desktop: Optional[str]
    url_img_mobile: Optional[str]
    url_vid: Optional[str]
    slug: str
    is_public: bool
    date_published: dt.date
    markdown: str

    def text(self) -> str:
        return self.title + " " + self.raw_text + " " + " ".join(self.authors)


def get_raw_data_insights() -> pd.DataFrame:
    """Get the content of data insights that exist in the database."""
    # Get all data insights from the database.
    query = """
        SELECT id, slug, content, published, publishedAt, markdown
        FROM posts_gdocs
        WHERE type = 'data-insight'
    """
    df = read_sql(query)

    return df


def extract_text_from_raw_data_insight(content: Dict[str, Any]) -> str:
    """Extract the text from the raw data insight, ignoring URLs and other fields."""
    texts = []

    # Iterate through each element in the 'body' field.
    for element in content.get("body", []):
        # Check if the element has a 'value' field that contains text.
        if "value" in element and isinstance(element["value"], list):
            for value_item in element["value"]:
                if "text" in value_item:
                    texts.append(value_item["text"])
                # Include text from children if present.
                if "children" in value_item and isinstance(value_item["children"], list):
                    for child in value_item["children"]:
                        if "text" in child:
                            texts.append(child["text"])

    # Join texts and do some minor cleaning.
    clean_text = " ".join(texts).replace(" .", ".").replace(" ,", ",").replace("  ", " ")

    return clean_text


def extract_image_urls_from_raw_data_insight(content) -> Tuple[str | None, str | None]:
    url_img_desktop = None
    url_img_mobile = None

    for element in content.get("body", []):
        if "type" in element and element["type"] == "image":
            if "filename" in element:
                fname = element["filename"]
                name, extension = os.path.splitext(fname)
                url_img_desktop = f"https://ourworldindata.org/images/published/{name}_1350{extension}"
            if "smallFilename" in element:
                fname = element["smallFilename"]
                name, extension = os.path.splitext(fname)
                url_img_mobile = f"https://ourworldindata.org/images/published/{name}_850{extension}"
            break
    return url_img_desktop, url_img_mobile


def extract_video_urls_from_raw_data_insight(content) -> str | None:
    url_video = None

    for element in content.get("body", []):
        if "type" in element and element["type"] == "video":
            if "url" in element:
                url_video = element["url"]
    return url_video


@st.cache_data(show_spinner=False)
def get_data_insights() -> list[Insight]:
    with st.spinner("Loading data insights..."):
        # Get the raw data insights from the database.
        df = get_raw_data_insights()

        # Parse data insights and construct a convenient dictionary.
        insights = []
        for _, di in df.iterrows():
            content = json.loads(di["content"])

            # Get multimedia urls
            url_img_desktop, url_img_mobile = extract_image_urls_from_raw_data_insight(content)
            url_vid = extract_video_urls_from_raw_data_insight(content)

            # Get markdown
            markdown = di["markdown"]
            pattern = r"<(Video|Image|Chart)\b[^>]*\/>"
            if markdown is not None:
                markdown = re.sub(pattern, "", markdown)
            else:
                markdown = extract_text_from_raw_data_insight(content)

            # Build DI dictionary
            di = Insight(
                id=di["id"],
                title=content["title"],
                raw_text=extract_text_from_raw_data_insight(content),
                authors=content["authors"],
                url=None,
                url_img_desktop=url_img_desktop,
                url_img_mobile=url_img_mobile,
                url_vid=url_vid,
                slug=di["slug"],
                is_public=bool(di["published"]),
                date_published=di["publishedAt"],
                markdown=markdown,
            )

            if di.is_public:
                di.url = f"https://ourworldindata.org/data-insights/{di.slug}"

            insights.append(di)

    return insights
