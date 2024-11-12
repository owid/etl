import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import pandas as pd
import streamlit as st
from sentence_transformers import SentenceTransformer, util
from structlog import get_logger
from tqdm.auto import tqdm

from etl.db import read_sql

# Initialize log.
log = get_logger()

# Load the pre-trained model.
model = SentenceTransformer("all-MiniLM-L6-v2")


def get_raw_data_insights() -> pd.DataFrame:
    """Get the content of data insights that exist in the database."""
    # Get all data insights from the database.
    # NOTE: Not all data insights have slugs, so for now identify them by id.
    query = """
        SELECT id, slug, content
        FROM posts_gdocs
        WHERE type = 'data-insight'
    """
    df = read_sql(query)

    return df


def extract_text_from_raw_data_insight(content: Dict[str, Any]) -> str:
    """
    Extract the text from the raw data insight, ignoring URLs and other fields.

    """
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


@st.cache_data
def get_data_insights() -> list[Dict[str, Any]]:
    # Get the raw data insights from the database.
    df = get_raw_data_insights()

    # Parse data insights and construct a convenient dictionary.
    insights = []
    for _, di in df.iterrows():
        content = json.loads(di["content"])
        # For now, omit authors and other fields.
        di = {"title": content["title"], "body": extract_text_from_raw_data_insight(content)}
        insights.append(di)

    return insights


# @st.cache_data
# def get_insights_embeddings(insights: list[Dict[str, Any]]) -> list:
#     # Combine the title and body of each insight into a single string.
#     insights_texts = [insight["title"] + " " + insight["body"] for insight in insights]
#     embeddings = [model.encode(doc, convert_to_tensor=True) for doc in tqdm(insights_texts)]

#     return embeddings


def _encode_text(text):
    return model.encode(text, convert_to_tensor=True)


@st.cache_data
def get_insights_embeddings(insights: list[Dict[str, Any]]) -> list:
    # Combine the title and body of each insight into a single string.
    insights_texts = [insight["title"] + " " + insight["body"] for insight in insights]

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
    input_embedding = model.encode(input_string, convert_to_tensor=True)

    # Compute the cosine similarity between the input and each document.
    similarities = [util.pytorch_cos_sim(input_embedding, doc_embedding).item() for doc_embedding in embeddings]  # type: ignore

    # Attach the similarity scores to the documents.
    for i, doc in enumerate(_insights):
        doc["similarity"] = similarities[i]  # type: ignore

    # Sort the documents by descending similarity score.
    sorted_documents = sorted(_insights, key=lambda x: x["similarity"], reverse=True)

    return sorted_documents


########################################################################################################################
# Fetch all data insights.
insights = get_data_insights()
# Create an embedding for each insight.
# TODO: This could also be stored in db.
embeddings = get_insights_embeddings(insights)
########################################################################################################################


# Streamlit app layout.
st.title("Data insight finder")
st.write("Enter a word or phrase to find the most similar insights.")

# Box for input text.
input_string = st.text_input(
    label="Search query", placeholder="Type something...", help="Write any text to find the most similar data insights."
)

if input_string:
    # Get the sorted documents.
    sorted_docs = get_sorted_documents_by_similarity(input_string, insights=insights, embeddings=embeddings)

    # Display the sorted documents.
    # TODO: This could be enhanced in different ways:
    #   * Add a color to similarity score.
    #   * Add other fields (e.g. author).
    #   * Ideally, show a miniature of the chart (but that may be a bit more complicated).
    #   * Add a link to open the preview of the insight.
    #   * Show the part of the text that justifies the score (this may also slow down the search).
    st.subheader("Results")
    for doc in sorted_docs:
        st.markdown(f"### {doc['title']}")
        st.write(f"**Similarity Score:** {doc['similarity']:.4f}")

        st.write("---")
