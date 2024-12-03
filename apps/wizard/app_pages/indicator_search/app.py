import re

import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.indicator_search import data
from apps.wizard.app_pages.insight_search import embeddings as emb
from apps.wizard.utils.components import st_horizontal, st_multiselect_wider

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Indicator Search",
    page_icon="🪄",
    layout="wide",
)

########################################################################################################################
# FUNCTIONS
########################################################################################################################


def st_display_indicators(indicators: list[dict]):
    df = pd.DataFrame(indicators)

    df["link"] = df.apply(lambda x: f"http://staging-site-indicator-search/admin/variables/{x['variableId']}/", axis=1)

    df["catalogPath"] = df["catalogPath"].str.replace("grapher/", "")

    df = df.drop(columns=["variableId", "description"])

    def make_bold(value):
        return "font-weight: bold;"

    styled_df = df.style.map(make_bold, subset=["name"]).format("{:.0%}", subset=["similarity"])  # type: ignore

    # df.style.format({"name": markdown_to_html_link}, escape=False)

    column_config = {
        "link": st.column_config.LinkColumn("Open", display_text="Open"),
        # "name": st.column_config.TextColumn("Name"),
    }

    # html = styled_df.to_html(escape=False, index=False)
    # st.markdown(html, unsafe_allow_html=True)
    # st.markdown(df.to_html(escape=False), unsafe_allow_html=True)

    st.dataframe(
        styled_df,
        column_order=["link", "name", "catalogPath", "n_charts", "similarity"],
        use_container_width=True,
        hide_index=True,
        # height=int(len(indicators) * 35),
        column_config=column_config,
    )
    return


def split_input_string(input_string: str) -> tuple[str, list[str], list[str]]:
    """Break input string into query, includes and excludes."""
    # Break input string into query, includes and excludes
    query = []
    includes = []
    excludes = []
    for term in input_string.split():
        if term.startswith("+"):
            includes.append(term[1:].lower())
        elif term.startswith("-"):
            excludes.append(term[1:].lower())
        else:
            query.append(term)

    return " ".join(query), includes, excludes


def indicator_query(indicator: dict) -> str:
    return indicator["name"] + " " + indicator["description"] + " " + (indicator["catalogPath"] or "")


def indicator_text(indicator: dict) -> str:
    # Combine the name and description into a single string
    return indicator["name"] + " " + indicator["description"]


# TODO: memoization would be very expensive
@st.cache_data(show_spinner=False, max_entries=1)
def get_indicators_embeddings(_model, _indicators_texts: list[str]) -> list:
    with st.spinner("Generating embeddings..."):
        return emb.get_embeddings(_model, _indicators_texts)  # type: ignore


########################################################################################################################
# Get embedding model.
MODEL = emb.get_model()
# Fetch all data indicators.
indicators = data.get_data_indicators()

# Create an embedding for each indicator.
embeddings = get_indicators_embeddings(MODEL, [indicator_text(i) for i in indicators])
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
    help="Write any text to find the most similar indicators.",
)

st_multiselect_wider()
with st_horizontal():
    # Filter indicators
    selection = st.segmented_control(
        "Indicator status",
        ["All", "Used in charts"],
        selection_mode="single",
        default="All",
        label_visibility="collapsed",
    )

    # Add option to group by dataset
    group_by_dataset = st.checkbox("Group by dataset", value=True)

    dedup_dimensions = st.checkbox("Deduplicate dimensions", value=True)


if input_string:
    if len(input_string) < 3:
        st.warning("Please enter at least 3 characters.")
    else:
        # Break input string into query, includes and excludes
        query, includes, excludes = split_input_string(input_string)

        # Get the sorted indicators.
        sorted_inds = emb.get_sorted_documents_by_similarity(MODEL, query, docs=indicators, embeddings=embeddings)  # type: ignore

        # Display the sorted documents.
        # TODO: This could be enhanced in different ways:
        #   * Add a color to similarity score.
        #   * Show the part of the text that justifies the score (this may also slow down the search).

        # Filter indicators
        match selection:
            case "All":
                filtered_inds = sorted_inds
            case "Used in charts":
                filtered_inds = [di for di in sorted_inds if di["n_charts"] > 0]
            case _:
                filtered_inds = sorted_inds

        # Filter includes and excludes
        for include in includes:
            filtered_inds = [di for di in filtered_inds if include in indicator_query(di).lower()]

        for exclude in excludes:
            filtered_inds = [di for di in filtered_inds if exclude not in indicator_query(di).lower()]

        # TODO: display non-ETL indicators too
        filtered_inds = [ind for ind in filtered_inds if ind["catalogPath"] is not None]

        filtered_inds = sorted(filtered_inds, key=lambda k: k["similarity"], reverse=True)

        if dedup_dimensions:
            patterns = [
                re.compile(r"__sex.*?__"),
                re.compile(r"__age.*?__"),
            ]

            new_inds = []
            seen = set()
            for ind in filtered_inds:
                catalog_path = ind["catalogPath"]
                for pattern in patterns:
                    catalog_path = pattern.sub("__", catalog_path)

                if catalog_path in seen:
                    continue

                # Remove duplicate catalogPaths
                new_inds.append(ind)
                seen.add(catalog_path)

            filtered_inds = new_inds

        if group_by_dataset:
            # filtered_inds = [ind for ind in filtered_inds if ind["similarity"] > 0.6]

            # Group indicators by dataset
            for ind in filtered_inds:
                ind["dataset"] = "/".join(ind["catalogPath"].split("/")[:4])

                # Trim catalogPath
                ind["catalogPath"] = "/".join(ind["catalogPath"].split("/")[4:])

            MAX_DATASETS = 5

            MAX_ITEMS = 10

            filtered_inds = filtered_inds[:100]

            used_datasets = set()
            for ind in filtered_inds:
                if ind["dataset"] in used_datasets:
                    continue

                inds = [i for i in filtered_inds if i["dataset"] == ind["dataset"]]
                used_datasets.add(ind["dataset"])

                with st.container(border=True):
                    st.write(ind["dataset"])
                    st_display_indicators(sorted(inds, key=lambda ind: ind["similarity"], reverse=True)[:MAX_ITEMS])

                if len(used_datasets) >= MAX_DATASETS:
                    break

        else:
            # Show items (only current page)
            st_display_indicators(filtered_inds[:50])
