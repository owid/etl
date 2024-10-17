"""This code contains the first demo for Anomalist.

This will be removed, but is kept alive since it may contain useful code snippets (e.g. streaming structured LLM outputs).
"""
from typing import cast

import streamlit as st
from pydantic import BaseModel, Field, ValidationError

from apps.utils.gpt import OpenAIWrapper, get_cost_and_tokens
from apps.wizard.utils import cached
from apps.wizard.utils.components import grapher_chart, st_horizontal
from etl.config import OWID_ENV
from etl.grapher_io import load_variables_in_dataset

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Anomalist",
    page_icon="🪄",
)

# SESSION STATE
st.session_state.register = st.session_state.get("register", {"by_dataset": {}})
st.session_state.datasets_selected = st.session_state.get("datasets_selected", [])
st.session_state.anomaly_revision = st.session_state.get("anomaly_revision", {})


# GPT
MODEL = "gpt-4o"
api = OpenAIWrapper()

# PAGE TITLE
st.title(":material/planner_review: Anomalist")
# st.markdown("Detect anomalies in your data!")


# SELECT DATASETS
st.markdown(
    """
    <style>
       .stMultiSelect [data-baseweb=select] span{
            max-width: 1000px;
        }
    </style>""",
    unsafe_allow_html=True,
)
st.session_state.datasets_selected = st.multiselect(
    "Select datasets",
    options=cached.load_dataset_uris(),
    max_selections=3,
)

for i in st.session_state:
    if i.startswith("check_anomaly_resolved_"):
        st.write(i, st.session_state[i])


# GET INDICATORS
if len(st.session_state.datasets_selected) > 0:
    # Get indicator uris for all selected datasets
    indicators = load_variables_in_dataset(st.session_state.datasets_selected)

    for indicator in indicators:
        catalog_path = cast(str, indicator.catalogPath)
        dataset_uri, indicator_slug = catalog_path.rsplit("/", 1)
        if dataset_uri not in st.session_state.register["by_dataset"]:
            st.session_state.register["by_dataset"][dataset_uri] = {}
        if indicator_slug in st.session_state.register["by_dataset"][dataset_uri]:
            continue
        st.session_state.register["by_dataset"][dataset_uri][indicator_slug] = {
            "anomalies": [],
            "id": indicator.id,
        }

################################################
# FUNCTIONS / CLASSES
################################################

NUM_MAX_ENTITIES = 10


@st.dialog("Vizualize the indicator", width="large")
def show_indicator(indicator_uri, indicator_id, selected_entities=None):
    """Plot the indicator in a modal window."""
    # Modal title
    st.markdown(f"[{indicator_slug}]({OWID_ENV.indicator_admin_site(indicator_id)})")

    # Plot indicator
    if (selected_entities is not None) and (len(selected_entities) > NUM_MAX_ENTITIES):
        st.warning(f"Too many entities. Showing only the first {NUM_MAX_ENTITIES}.")
    grapher_chart(catalog_path=indicator_uri, selected_entities=selected_entities, owid_env=OWID_ENV)


def show_anomaly(title, description, entities, indicator_id, indicator_uri, key):
    # check_value = st.session_state.register["by_dataset"][dataset_name][indicator_slug].get("resolved", False)
    check_value = st.session_state.get(key, False)

    if check_value:
        icon = "✅"
    else:
        icon = "⏳"

    with st.expander(title, icon=icon):
        st.checkbox(
            "Mark as resolved",
            value=check_value,
            key=f"check_anomaly_resolved_{key}",
        )
        st.write(description)
        # st.write(entities)

        if (entities is not None) & isinstance(entities, list) & (len(entities) > 0):
            if st.button(
                "Inspect anomaly",
                icon=":material/show_chart:",
                # use_container_width=True,
                key=f"btn_plot_{key}",
            ):
                show_indicator(indicator_uri, indicator_id, entities)


def get_anomaly_gpt(indicator_id: str, indicator_uri: str, dataset_name: str, indicator_slug: str):
    # Open AI (do first to catch possible errors in ENV)
    # Prepare messages for Insighter

    data = cached.load_variable_data(variable_id=int(indicator_id))
    data_1 = data.pivot(index="years", columns="entity", values="values")  # .head(20)
    data_1 = data_1.dropna(axis=1, how="all")
    data_1_str = cast(str, data_1.to_csv()).replace(".0,", ",")

    num_anomalies = 3

    messages = [
        {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": f"Provide {num_anomalies} anomalies in for the given time series.",
                },
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": data_1_str,
                },
            ],
        },
    ]
    kwargs = {
        "api": api,
        "model": MODEL,
        "messages": messages,
        "max_tokens": 3000,
        "response_format": AnomaliesModel,
    }
    latest_json = ""
    for anomaly_count, (anomaly, _, latest_json) in enumerate(openai_structured_outputs_stream(**kwargs)):
        # Show anomaly
        key = f"{indicator_id}_{anomaly_count}"
        show_anomaly(anomaly.title, anomaly.description, anomaly.entities, indicator_id, indicator_uri, key)

        # Save anomaly
        st.session_state.register["by_dataset"][dataset_name][indicator_slug]["anomalies"].append(anomaly.model_dump())

    # Get cost and tokens
    text_in = [mm["text"] for m in messages for mm in m["content"] if mm["type"] == "text"]
    text_in = "\n".join(text_in)
    cost, num_tokens = get_cost_and_tokens(text_in, latest_json, cast(str, MODEL))
    cost_msg = f"**Cost**: ≥{cost} USD.\n\n **Tokens**: ≥{num_tokens}."
    st.info(cost_msg)


@st.fragment
def show_indicator_block(dataset_name, indicator_slug):
    indicator_uri = f"{dataset_name}/{indicator_slug}"

    indicator_props = st.session_state.register["by_dataset"][dataset_name][indicator_slug]
    indicator_id = indicator_props["id"]
    indicator_anomalies = indicator_props["anomalies"]

    with st.container(border=True):
        # Title
        st.markdown(f"[{indicator_slug}]({OWID_ENV.indicator_admin_site(indicator_id)})")

        # Buttons
        with st_horizontal():
            # Find anomalies button
            btn_gpt = st.button(
                "Find anomalies",
                icon=":material/robot:",
                # use_container_width=True,
                type="primary",
                help="Use GPT to find anomalies in the indicator.",
                key=f"btn_gpt_{indicator_id}",
                # on_click=lambda: st.rerun(scope="fragment"),
            )
            # 'Plot indicator' button
            if st.button(
                "Plot indicator",
                icon=":material/show_chart:",
                # use_container_width=True,
                key=f"btn_plot_{indicator_id}",
            ):
                show_indicator(indicator_uri, indicator_id)

        # Show anomalies
        if btn_gpt:
            with st.spinner("Querying GPT for anomalies..."):
                get_anomaly_gpt(indicator_id, indicator_uri, dataset_name, indicator_slug)
        else:
            for anomaly_count, anomaly in enumerate(indicator_anomalies):
                key = f"resolved_{indicator_id}_{anomaly_count}"
                show_anomaly(
                    anomaly["title"], anomaly["description"], anomaly["entities"], indicator_id, indicator_uri, key
                )


class AnomalyModel(BaseModel):
    title: str = Field(description="Title of the anomaly.")
    description: str = Field(description="Short description of the anomaly.")
    entities: list[str] = Field(
        description="List of entities affected by the anomaly. Entities are given as columns in the input data (excluding column 'years' or 'date')."
    )
    finished: bool = Field(description="True if the obtention of a particular anomaly has been finalized.")


class AnomaliesModel(BaseModel):
    anomalies: list[AnomalyModel] = Field(description="List of anomalies detected in the data.")


def openai_structured_outputs_stream(api, **kwargs):
    """Stream structured outputs from OpenAI API.

    References:
        - https://community.openai.com/t/streaming-using-structured-outputs/925799/13
    """
    parsed_latest = None
    with api.beta.chat.completions.stream(**kwargs, stream_options={"include_usage": True}) as stream:
        # Check each chunk in stream (new chunk appears whenever a new character is added to the completion)
        for chunk in stream:
            # Only consider those of type "chunk"
            if chunk.type == "chunk":
                # Get latest snapshot
                latest_snapshot = chunk.to_dict()["snapshot"]

                # Get latest choice
                choice = latest_snapshot["choices"][0]
                parsed_cumulative = choice["message"].get("parsed", {})

                # Note that usage is not available until the final chunk
                latest_usage = latest_snapshot.get("usage", {})
                latest_json = choice["message"]["content"]

                # Checks:
                # 1. Check if "anomalies" is in the returned object
                # 2. Check if "anomalies" is a list
                # 3. Check if "anomalies" is not empty
                # 4. Check if the latest parsed object is different from the previous one
                if "anomalies" in parsed_cumulative:
                    anomalies = parsed_cumulative["anomalies"]
                    if isinstance(anomalies, list) & (len(anomalies) > 0):
                        parsed_latest_ = anomalies[-1]

                        # Check if parsed_latest_ is a valid AnomalyModel (i.e. if it is a complete object!)
                        try:
                            anomaly = AnomalyModel(**parsed_latest_)
                            if (parsed_latest is None) | (parsed_latest != parsed_latest_):
                                # st.write(choice)
                                parsed_latest = parsed_latest_
                                yield anomaly, latest_usage, latest_json
                        except ValidationError as _:
                            continue
                    # yield latest_parsed["anomalies"], latest_usage, latest_json


# SHOW INDICATORS
if len(st.session_state.datasets_selected) > 0:
    num_tabs = len(st.session_state.datasets_selected)
    tabs = st.tabs(st.session_state.datasets_selected)

    # Block per dataset
    for dataset_name, tab in zip(st.session_state.datasets_selected, tabs):
        with tab:
            # Block per indicator in dataset
            for indicator_slug in st.session_state.register["by_dataset"][dataset_name].keys():
                # Indicator block
                show_indicator_block(dataset_name, indicator_slug)

                # my_fragment(indicator_uri)
            # Anomalies detected
            # anomalies = indicator["anomalies"]
            # st.markdown(f"{len(anomalies)} anomalies detected.")

            # for anomaly_index, a in enumerate(anomalies):
            #     # Review icon
            #     if a["resolved"]:
            #         icon = "✅"
            #     else:
            #         icon = "⏳"

            #     # Anomaly explained (expander)
            #     with st.expander(f'{anomaly_index+1}/ {a["title"]}', expanded=False, icon=icon):
            #         # Check if resolved
            #         key = f"resolved_{dataset_index}_{indicator_index}_{anomaly_index}"

            #         # Checkbox (if resolved)
            #         st.checkbox(
            #             "Mark as resolved",
            #             value=a["resolved"],
            #             key=key,
            #         )

            #         # Anomaly description
            #         st.markdown(a["description"])
