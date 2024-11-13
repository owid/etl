import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Tuple

import pandas as pd
import streamlit as st
from sentence_transformers import SentenceTransformer, util
from structlog import get_logger
from tqdm.auto import tqdm

from apps.wizard.utils.components import Pagination, tag_in_md
from etl.db import read_sql

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Insight Search",
    page_icon="🪄",
)


########################################################################################################################
# FUNCTIONS
########################################################################################################################
@st.cache_data(show_spinner=False)
def get_model():
    "Load the pre-trained model."
    with st.spinner("Loading model..."):
        model = SentenceTransformer("all-MiniLM-L6-v2")
    return model


MODEL = get_model()


def get_raw_data_insights() -> pd.DataFrame:
    """Get the content of data insights that exist in the database."""
    # Get all data insights from the database.
    # NOTE: Not all data insights have slugs, so for now identify them by id.
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
def get_data_insights() -> list[Dict[str, Any]]:
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
            di_dict = {
                "title": content["title"],
                "raw_text": extract_text_from_raw_data_insight(content),
                "authors": content["authors"],
                "url_img_desktop": url_img_desktop,
                "url_img_mobile": url_img_mobile,
                "url_vid": url_vid,
                "slug": di["slug"],
                "public": bool(di["published"]),
                "date_published": di["publishedAt"],
                "markdown": markdown,
            }

            if di_dict["public"]:
                di_dict["url"] = f"https://ourworldindata.org/data-insights/{di_dict['slug']}"

            insights.append(di_dict)

    return insights


# @st.cache_data
# def get_insights_embeddings(insights: list[Dict[str, Any]]) -> list:
#     # Combine the title and body of each insight into a single string.
#     insights_texts = [insight["title"] + " " + insight["body"] for insight in insights]
#     embeddings = [model.encode(doc, convert_to_tensor=True) for doc in tqdm(insights_texts)]

#     return embeddings


def _encode_text(text):
    return MODEL.encode(text, convert_to_tensor=True)


@st.cache_data(show_spinner=False)
def get_insights_embeddings(insights: list[Dict[str, Any]]) -> list:
    with st.spinner("Generating embeddings..."):
        # Combine the title, body and authors of each insight into a single string.
        insights_texts = [
            insight["title"] + " " + insight["raw_text"] + " " + " ".join(insight["authors"]) for insight in insights
        ]

        # Run embedding generation in parallel.
        with ThreadPoolExecutor() as executor:
            embeddings = list(tqdm(executor.map(_encode_text, insights_texts), total=len(insights_texts)))

    return embeddings


def get_sorted_documents_by_similarity(
    input_string: str, insights: list[Dict[str, str]], embeddings: list
) -> list[Dict[str, Any]]:
    """Ingests an input string and a list of documents, returning the list of documents sorted by their semantic similarity to the input string."""
    _insights = insights.copy()

    # Encode the input string and the document texts.
    input_embedding = MODEL.encode(input_string, convert_to_tensor=True)

    # Compute the cosine similarity between the input and each document.
    def _get_score(a, b):
        score = util.pytorch_cos_sim(a, b).item()
        score = (score + 1) / 2
        return score

    similarities = [_get_score(input_embedding, doc_embedding) for doc_embedding in embeddings]  # type: ignore

    # Attach the similarity scores to the documents.
    for i, doc in enumerate(_insights):
        doc["similarity"] = similarities[i]  # type: ignore

    # Sort the documents by descending similarity score.
    sorted_documents = sorted(_insights, key=lambda x: x["similarity"], reverse=True)

    return sorted_documents


def st_display_insight(insight):
    authors = ", ".join([tag_in_md(a, "gray", ":material/ink_pen") for a in insight["authors"]])
    score = round(insight["similarity"] * 100)
    with st.container(border=True):
        st.markdown(f"#### {insight['title']}")

        # If public, display special header (inc multimedia content if insight is public)
        if insight["public"]:
            # Display header 'Author | Date | Link'
            date_str = insight["date_published"].strftime("%B %d, %Y")
            date_str = tag_in_md(date_str, "green", ":material/calendar_month")
            # header = f"by **{authors}** | published **{date_str}** | [view]({insight['url']})"
            st.markdown(f"by {authors} | {date_str}")

            # Show multimedia content if available (image, video)
            if insight["url_img_desktop"] is not None:
                st.image(insight["url_img_desktop"], use_container_width=True)
            elif insight["url_vid"] is not None:
                st.video(insight["url_vid"])
        # Display only authors if not public
        else:
            st.write(f":red[(Draft)] {authors}")

        # Render text
        text = insight["markdown"].replace("$", "\$")
        st.caption(text)

        # Score
        st.write(f"**Similarity Score:** {score}%")


########################################################################################################################
# Fetch all data insights.
insights = get_data_insights()
# Create an embedding for each insight.
# TODO: This could also be stored in db.
embeddings = get_insights_embeddings(insights)
########################################################################################################################


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/search: DI search")

# Box for input text.
input_string = st.text_input(
    label="Enter a word or phrase to find the most similar insights.",
    placeholder="Type something...",
    help="Write any text to find the most similar data insights.",
)

if input_string:
    if len(input_string) < 3:
        st.warning("Please enter at least 3 characters.")
    else:
        # Get the sorted DIs.
        sorted_dis = get_sorted_documents_by_similarity(input_string, insights=insights, embeddings=embeddings)

        # Display the sorted documents.
        # TODO: This could be enhanced in different ways:
        #   * Add a color to similarity score.
        #   * Show the part of the text that justifies the score (this may also slow down the search).

        options = ["All", "Published", "Drafts"]
        selection = st.segmented_control(
            "Status",
            options,
            selection_mode="single",
            default="All",
            label_visibility="collapsed",
        )

        # Filter DIs
        st.write(selection)
        match selection:
            case "All":
                filtered_dis = sorted_dis
            case "Published":
                filtered_dis = [di for di in sorted_dis if di["public"]]
            case "Drafts":
                filtered_dis = [di for di in sorted_dis if not di["public"]]
            case _:
                filtered_dis = sorted_dis

        # Use pagination
        items_per_page = 2000
        pagination = Pagination(
            items=filtered_dis,
            items_per_page=items_per_page,
            pagination_key=f"pagination-di-search-{input_string}",
        )

        if len(filtered_dis) > items_per_page:
            pagination.show_controls(mode="bar")

        # Show items (only current page)
        for item in pagination.get_page_items():
            st_display_insight(item)
