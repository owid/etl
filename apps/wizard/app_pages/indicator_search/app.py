import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests
import streamlit as st
from structlog import get_logger

from apps.wizard.utils.components import st_horizontal, st_multiselect_wider, st_title_with_expert, url_persist
from etl.config import OWID_ENV, SEARCH_API_URL

# Initialize log.
log = get_logger()

# How many datasets to show
MAX_DATASETS = 5

# How many indicators in each dataset
MAX_INDICATORS_IN_DATASET = 10

# Maximum number of results to fetch from API (API limit is 100)
MAX_RESULTS = 100


@dataclass
class Indicator:
    """Indicator from the Search API."""

    variableId: int
    name: str
    description: Optional[str]
    n_charts: int
    catalogPath: str
    similarity: float
    dataset: Optional[str] = None
    popularity: float = 0.0

    def to_dict(self):
        return {
            "variableId": self.variableId,
            "name": self.name,
            "description": self.description,
            "n_charts": self.n_charts,
            "catalogPath": self.catalogPath,
            "similarity": self.similarity,
            "popularity": self.popularity,
        }


# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Indicator Search",
    page_icon="🪄",
    layout="wide",
)

########################################################################################################################
# FUNCTIONS
########################################################################################################################


def search_indicators_api(query: str, limit: int = MAX_RESULTS) -> list[Indicator]:
    """Search indicators using the Search API."""
    response = requests.get(
        f"{SEARCH_API_URL}/indicators",
        params={"query": query, "limit": limit},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    indicators = []
    for result in data["results"]:
        indicators.append(
            Indicator(
                variableId=result["indicator_id"],
                name=result["title"],
                description=result["description"],
                n_charts=result["n_charts"],
                catalogPath=result["catalog_path"] or "",
                similarity=result["score"],
                popularity=result.get("popularity", 0.0),
            )
        )
    return indicators


def st_display_indicators(indicators: list[Indicator]):
    """Display a list of indicators as a dataframe."""
    df = pd.DataFrame([ind.to_dict() for ind in indicators])

    df["link"] = df.apply(lambda x: OWID_ENV.indicator_admin_site(x["variableId"]), axis=1)
    df["catalogPath"] = df["catalogPath"].str.replace("grapher/", "")
    df = df.drop(columns=["variableId", "description"])

    styled_df = df.style.format("{:.0%}", subset=["similarity", "popularity"])

    column_config = {
        "link": st.column_config.LinkColumn("Open", display_text="Open"),
    }

    st.dataframe(
        styled_df,
        column_order=["name", "catalogPath", "n_charts", "similarity", "popularity", "link"],
        width="stretch",
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


def filter_include_exclude(indicators: list[Indicator], includes: list[str], excludes: list[str]) -> list[Indicator]:
    new_indicators = []
    for ind in indicators:
        q = (ind.name + ind.catalogPath).lower()
        if all(include in q for include in includes) and all(exclude not in q for exclude in excludes):
            new_indicators.append(ind)
    return new_indicators


def deduplicate_dimensions(indicators: list[Indicator]) -> list[Indicator]:
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


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st_title_with_expert("Indicator search", icon=":material/search:")

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
        ["All", "Used in charts", "Has popularity"],
        selection_mode="single",
        default="Has popularity",
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

        # Search using the API
        with st.spinner("Searching..."):
            sorted_inds = search_indicators_api(query)

        if not sorted_inds:
            st.info("No results found.")
        else:
            # Filter indicators
            match selection:
                case "All":
                    filtered_inds = sorted_inds
                case "Used in charts":
                    filtered_inds = [ind for ind in sorted_inds if ind.n_charts > 0]
                case "Has popularity":
                    filtered_inds = [ind for ind in sorted_inds if ind.popularity > 0]
                case _:
                    filtered_inds = sorted_inds

            # Filter includes and excludes
            filtered_inds = filter_include_exclude(filtered_inds, includes, excludes)

            # Sort by similarity (already sorted by API, but re-sort to be sure)
            filtered_inds = sorted(filtered_inds, key=lambda k: k.similarity, reverse=True)

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
                            sorted(inds, key=lambda ind: ind.similarity, reverse=True)[:MAX_INDICATORS_IN_DATASET]
                        )

                    if len(used_datasets) >= MAX_DATASETS:
                        break

            else:
                st_display_indicators(filtered_inds[:50])
