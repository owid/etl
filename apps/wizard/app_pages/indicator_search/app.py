import re

import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.indicator_search import data
from apps.wizard.utils import embeddings as emb
from apps.wizard.utils.components import st_horizontal, st_multiselect_wider, url_persist
from etl.config import OWID_ENV

# Initialize log.
log = get_logger()

# How many datasets to show
MAX_DATASETS = 5

# How many indicators in each dataset
MAX_INDICATORS_IN_DATASET = 10

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Indicator Search",
    page_icon="🪄",
    layout="wide",
)

########################################################################################################################
# FUNCTIONS
########################################################################################################################


def st_display_indicators(indicators: list[data.Indicator]):
    """Display a list of indicators as a dataframe."""
    df = pd.DataFrame([ind.to_dict() for ind in indicators])

    df["link"] = df.apply(lambda x: OWID_ENV.indicator_admin_site(x["variableId"]), axis=1)
    df["catalogPath"] = df["catalogPath"].str.replace("grapher/", "")
    df = df.drop(columns=["variableId", "description"])

    styled_df = df.style.format("{:.0%}", subset=["similarity"])

    column_config = {
        "link": st.column_config.LinkColumn("Open", display_text="Open"),
    }

    st.dataframe(
        styled_df,
        column_order=["name", "catalogPath", "n_charts", "similarity", "link"],
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
    )


def split_input_string(input_string: str) -> tuple[str, list[str], list[str]]:
    """Break input string into query, includes and excludes."""
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


def indicator_query(indicator: data.Indicator) -> str:
    return indicator.name + " " + indicator.catalogPath


def filter_include_exclude(
    indicators: list[data.Indicator], includes: list[str], excludes: list[str]
) -> list[data.Indicator]:
    new_indicators = []
    for ind in indicators:
        q = (ind.name + ind.catalogPath).lower()
        if all(include in q for include in includes) and all(exclude not in q for exclude in excludes):
            new_indicators.append(ind)
    return new_indicators


def deduplicate_dimensions(indicators: list[data.Indicator]) -> list[data.Indicator]:
    """Deduplicate identical indicators with different dimension values."""
    # Only deduplicate sex and age for now
    patterns = [
        re.compile(r"__sex.*?(?:__|$)"),
        re.compile(r"__age.*?years(?:__|$)"),
        re.compile(r"__age.*?(?:__|$)"),
    ]

    # Iterate over all indicators and remove duplicates
    new_inds = []
    seen = set()
    for ind in indicators:
        # Skip non-ETL indicators
        if ind.catalogPath is None:
            new_inds.append(ind)
            continue

        # Remove dimension from catalog path
        catalog_path = ind.catalogPath
        for pattern in patterns:
            catalog_path = pattern.sub("__", catalog_path)

        if catalog_path in seen:
            continue

        # Remove duplicate catalogPaths
        new_inds.append(ind)
        seen.add(catalog_path)

    return new_inds


# Don't memoize indicators, that would be computationally very expensive
@st.cache_data(show_spinner=False, max_entries=1)
def get_and_fit_model(_indicators: list[data.Indicator]) -> emb.EmbeddingsModel:
    # Get embedding model.
    model = emb.EmbeddingsModel(emb.get_model())
    # Create an embedding for each indicator.
    with st.spinner("Creating embeddings..."):
        model.fit(_indicators)
    return model


########################################################################################################################
# Fetch all data indicators.
indicators = data.get_data_indicators()
# Get embedding model.
model = get_and_fit_model(indicators)

########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/search: Indicator search")

# Box for input text.
input_string = url_persist(st.text_input)(
    key="input_string",
    label="Enter keywords or phrases to search for indicators.",
    placeholder="Use +term/-term to include/exclude exact match and the rest for semantic search. E.g. `depression +2024 -gbd`",
    help="+ and - also work on catalog paths.",
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

    # Add options
    group_by_dataset = st.checkbox("Group by dataset", value=True, help="Show indicators grouped by dataset.")
    dedup_dimensions = st.checkbox(
        "Deduplicate dimensions", value=True, help="Remove identical indicators with different dimension values."
    )


if input_string:
    if len(input_string) < 3:
        st.warning("Please enter at least 3 characters.")
    else:
        # Break input string into query, includes and excludes
        query, includes, excludes = split_input_string(input_string)

        # Get the sorted indicators.
        sorted_inds: list[data.Indicator] = model.get_sorted_documents_by_similarity(query)

        # Filter indicators
        match selection:
            case "All":
                filtered_inds = sorted_inds
            case "Used in charts":
                filtered_inds = [ind for ind in sorted_inds if ind.n_charts > 0]
            case _:
                filtered_inds = sorted_inds

        # Filter includes and excludes
        filtered_inds = filter_include_exclude(filtered_inds, includes, excludes)

        # Sort by similarity
        filtered_inds: list[data.Indicator] = sorted(filtered_inds, key=lambda k: k.similarity, reverse=True)  # type: ignore

        if dedup_dimensions:
            filtered_inds = deduplicate_dimensions(filtered_inds)

        if group_by_dataset:
            # Group indicators by dataset
            for ind in filtered_inds:
                ind.dataset = "/".join(ind.catalogPath.split("/")[:4])

                # Trim catalogPath
                ind.catalogPath = "/".join(ind.catalogPath.split("/")[4:])

            filtered_inds = filtered_inds[:100]

            used_datasets = set()
            for ind in filtered_inds:
                if ind.dataset in used_datasets:
                    continue

                inds = [i for i in filtered_inds if i.dataset == ind.dataset]
                used_datasets.add(ind.dataset)

                with st.container(border=True):
                    st.write(ind.dataset)
                    st_display_indicators(
                        sorted(inds, key=lambda ind: ind.similarity, reverse=True)[:MAX_INDICATORS_IN_DATASET]  # type: ignore
                    )

                if len(used_datasets) >= MAX_DATASETS:
                    break

        else:
            st_display_indicators(filtered_inds[:50])
