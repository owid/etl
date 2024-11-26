from typing import Any, Dict

import streamlit as st
from sentence_transformers import util
from structlog import get_logger

from apps.wizard.utils.components import Pagination, st_horizontal, st_multiselect_wider, tag_in_md

from .data import get_data_insights
from .embeddings import get_insights_embeddings, get_model

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


def extract_video_urls_from_raw_data_insight(content) -> str | None:
    url_video = None

    for element in content.get("body", []):
        if "type" in element and element["type"] == "video":
            if "url" in element:
                url_video = element["url"]
    return url_video


def get_sorted_documents_by_similarity(
    model, input_string: str, insights: list[Dict[str, str]], embeddings: list
) -> list[Dict[str, Any]]:
    """Ingests an input string and a list of documents, returning the list of documents sorted by their semantic similarity to the input string."""
    _insights = insights.copy()

    # Encode the input string and the document texts.
    input_embedding = model.encode(input_string, convert_to_tensor=True)

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


def st_display_indicator(insight):
    # TODO: rework this to work with indicators
    # :material/person
    authors = ", ".join([tag_in_md(a, "gray", ":material/person") for a in insight["authors"]])
    score = round(insight["similarity"] * 100)

    # Get edit URLs
    # url_gdoc = f"https://docs.google.com/document/d/{insight['id']}/edit"
    url_admin = f"http://staging-site-master/admin/gdocs/{insight['id']}/preview"

    with st.container(border=True):
        # If public, display special header (inc multimedia content if insight is public)
        if insight["is_public"]:
            st.markdown(f"#### [{insight['title']}]({insight['url']})")

            # Display header 'Author | Date'
            date_str = insight["date_published"].strftime("%B %d, %Y")
            date_str = tag_in_md(date_str, "green", ":material/calendar_month")
            # header = f"by **{authors}** | published **{date_str}** | [view]({insight['url']})"
            st.markdown(f"by {authors} | {date_str} | [:material/edit: edit]({url_admin})")

            # Show multimedia content if available (image, video)
            if insight["url_img_desktop"] is not None:
                st.image(insight["url_img_desktop"], use_container_width=True)
            elif insight["url_vid"] is not None:
                st.video(insight["url_vid"])
        # Display only authors if not public
        else:
            st.markdown(f"#### {insight['title']}")
            st.write(f":red[(Draft)] {authors} | [:material/edit: edit]({url_admin})")

        # Render text
        text = insight["markdown"].replace("$", "\$")  # type: ignore
        st.caption(text)

        # Score
        st.write(f"**Similarity Score:** {score}%")


@st.cache_data(show_spinner=False)
def get_authors_with_DIs(insights):
    with st.spinner("Getting author names..."):
        return set(author for insight in insights for author in insight["authors"])


########################################################################################################################
# Fetch all data insights.
insights = get_data_insights()
# Available authors
authors = get_authors_with_DIs(insights)

# Load the pre-trained model.
MODEL = get_model()

# Create an embedding for each insight.
# TODO: This should be stored in DB.
embeddings = get_insights_embeddings(MODEL, insights)
########################################################################################################################


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/search: Indicator search")

# Box for input text.
input_string = st.text_input(
    label="Enter a word or phrase to find the most similar indicators.",
    placeholder="Type something...",
    help="Write any text to find the most similar data indicators.",
)

st_multiselect_wider()

if input_string:
    if len(input_string) < 3:
        st.warning("Please enter at least 3 characters.")
    else:
        # Get the sorted indicators.
        # TODO: limit this to N indicators
        sorted_dis = get_sorted_documents_by_similarity(MODEL, input_string, insights=insights, embeddings=embeddings)

        # Display the sorted documents.
        # TODO: This could be enhanced in different ways:
        #   * Add a color to similarity score.
        #   * Show the part of the text that justifies the score (this may also slow down the search).

        # Filter Indicators by used in charts
        options = ["All", "Published", "Drafts"]
        selection = st.segmented_control(
            "Publication status",
            options,
            selection_mode="single",
            default="All",
            label_visibility="collapsed",
        )

        # Filter DIs
        match selection:
            case "All":
                filtered_dis = sorted_dis
            case "Published":
                filtered_dis = [di for di in sorted_dis if di["is_public"]]
            case "Drafts":
                filtered_dis = [di for di in sorted_dis if not di["is_public"]]
            case _:
                filtered_dis = sorted_dis

        # Use pagination
        items_per_page = 100
        pagination = Pagination(
            items=filtered_dis,
            items_per_page=items_per_page,
            pagination_key=f"pagination-di-search-{input_string}",
        )

        if len(filtered_dis) > items_per_page:
            pagination.show_controls(mode="bar")

        # Show items (only current page)
        for item in pagination.get_page_items():
            st_display_indicator(item)
