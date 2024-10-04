from typing import cast

import streamlit as st

from apps.utils.gpt import OpenAIWrapper, get_cost_and_tokens
from apps.wizard.utils.chart import grapher_chart
from etl.config import OWID_ENV

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Anomalist",
    page_icon="🪄",
)

# INDICATOR SPECS (INPUT)
DATASETS = [
    {
        "dataset": "grapher/climate_watch/2023-10-31/emissions_by_sector",
        "indicators": [
            {
                "slug": "greenhouse_gas_emissions_by_sector#land_use_change_and_forestry_ghg_emissions",
                "anomalies": [
                    {
                        "title": "Afghanistan's Early Internet Usage",
                        "description": "Between 2001 and 2006, Afghanistan showed extremely low internet usage, remaining at approximately 0% for several years before a gradual increase began.",
                    },
                    {
                        "title": "Significant Jump in Angola's Internet Usage",
                        "description": "In 2012, Angola witnessed a significant jump in the share of individuals using the internet from 4.7% in 2011 to 7.7%, indicating a rapid growth phase.",
                    },
                    {
                        "title": "Explosive Growth in United Arab Emirates",
                        "description": "From 2007 to 2008, the United Arab Emirates saw an explosive growth in internet usage, rising from 61% to 63%, continuing its trend towards universal access.",
                    },
                ],
            },
        ],
    }
]
st.session_state.datasets = st.session_state.get("datasets", DATASETS)

# GPT
MODEL = "gpt-4o"


# ANOMALY STATUS
# Initialise/update anomaly-review status
for d_i, d in enumerate(st.session_state.datasets):
    # print(f"dataset {d_i}")
    for i_i, i in enumerate(d["indicators"]):
        # print(f"indicator {i_i}")
        for a_i, a in enumerate(i["anomalies"]):
            print(f"anomaly {a_i}")
            if "resolved" not in a:
                # print("> initialising")
                a["resolved"] = False
            else:
                # print("> updating")
                a["resolved"] = st.session_state[f"resolved_{d_i}_{i_i}_{a_i}"]


# PAGE TITLE
st.title(":material/planner_review: Anomalist")
st.markdown("Detect anomalies in your data!")
# st.write(st.session_state.datasets)
st.divider()

################################################
# FUNCTIONS
################################################


@st.dialog("Vizualize the indicator", width="large")
def show_indicator(indicator_uri):
    """Plot the indicator in a modal window."""
    # Modal title
    st.markdown(f"`{indicator_uri}`")

    # Get data and metadata from catalog
    # st.write(metadata)
    # Get list of entities available
    grapher_chart(catalog_path=indicator_uri, owid_env=OWID_ENV)
    # st.line_chart(data=data_, x="years", y="values", color="entity")


from pydantic import BaseModel


class AnomalyModel(BaseModel):
    title: str
    description: str


def openai_structured_outputs_stream(api, **kwargs):
    with api.beta.chat.completions.stream(**kwargs, stream_options={"include_usage": True}) as stream:
        for chunk in stream:
            # st.write(chunk)
            # st.write("---")
            if chunk.type == "chunk":
                latest_snapshot = chunk.to_dict()["snapshot"]
                # The first chunk doesn't have the 'parsed' key, so using .get to prevent raising an exception
                latest_parsed = latest_snapshot["choices"][0]["message"].get("parsed", {})
                # Note that usage is not available until the final chunk
                latest_usage = latest_snapshot.get("usage", {})
                latest_json = latest_snapshot["choices"][0]["message"]["content"]

                yield latest_parsed, latest_usage, latest_json


# Block per dataset
for dataset_index, d in enumerate(st.session_state.datasets):
    st.markdown(f'##### :material/dataset: {d["dataset"]}')
    indicators = d["indicators"]

    # Block per indicator in dataset
    for indicator_index, i in enumerate(indicators):
        indicator_uri = f"{d['dataset']}/{i['slug']}"
        with st.container(border=True):
            # Title
            st.markdown(f"`{i['slug']}`")

            col1, col2 = st.columns(2)

            with col1:
                btn_gpt = st.button(
                    "Find anomalies", icon=":material/planner_review:", use_container_width=True, type="primary"
                )

            with col2:
                # Show indicator button
                if st.button("Plot indicator", icon=":material/show_chart:", use_container_width=True):
                    show_indicator(indicator_uri)

            if btn_gpt:
                # Open AI (do first to catch possible errors in ENV)
                api = OpenAIWrapper()

                # Prepare messages for Insighter
                DIVIDER = "---"
                messages = [
                    {
                        "role": "system",
                        "content": [
                            {
                                "type": "text",
                                "text": """
                                    Provide a list of 3 data anomalies in a given topic in different countries. Your output should be in format:
                                    [
                                        {
                                            "title": "Title of anomaly 1",
                                            "description": "Description of the anomaly."
                                        },
                                        {
                                            "title": "Title of anomaly 2",
                                            "description": "Description of the anomaly."
                                        },
                                        {
                                            "title": "Title of anomaly 3",
                                            "description": "Description of the anomaly."
                                        }
                                    ]
                                """,
                            },
                        ],
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "life expectancy",
                            },
                        ],
                    },
                ]

                # {DIVIDER}
                # title: Title of anomaly 1
                # description: Description of the anomaly.
                # {DIVIDER}
                # title: Title of anomaly 2
                # description: Description of the anomaly.
                kwargs = {
                    "model": MODEL,
                    "messages": messages,
                    "max_tokens": 3000,
                    # "response_format": {"type": "json_object"},
                }
                # TODO: https://community.openai.com/t/streaming-using-structured-outputs/925799/13
                for parsed_completion, *_ in openai_structured_outputs_stream(
                    api, model="gpt-4o", temperature=0, messages=messages, response_format=AnomalyModel
                ):
                    st.write(parsed_completion)

                # with st.chat_message("assistant"):
                #     # Ask GPT (stream)
                #     stream = api.chat.completions.create(
                #         model=MODEL,
                #         messages=messages,  # type: ignore
                #         max_tokens=3000,
                #         stream=True,
                #         response_format={"type": "json_object"},
                #         # stream_options={"include_usage": True},  # retrieving token usage for stream response
                #     )
                #     # chunks = [c for c in stream]
                #     # st.write(chunks)
                #     # st.write(chunks[-1])
                #     for chunk in stream:
                #         # st.write(chunk)
                #         st.write(chunk.choices[0].delta)
                #     # response = cast(str, st.write_stream(stream))
                #     # st.write(response)

                text_in = [mm["text"] for m in messages for mm in m["content"] if mm["type"] == "text"]
                text_in = "\n".join(text_in)
                cost, num_tokens = get_cost_and_tokens(text_in, response, cast(str, MODEL))
                cost_msg = f"**Cost**: ≥{cost} USD.\n\n **Tokens**: ≥{num_tokens}."
                st.info(cost_msg)

            # Anomalies detected
            anomalies = i["anomalies"]
            st.markdown(f"{len(anomalies)} anomalies detected.")

            for anomaly_index, a in enumerate(anomalies):
                # Review icon
                if a["resolved"]:
                    icon = "✅"
                else:
                    icon = "⏳"

                # Anomaly explained (expander)
                with st.expander(f'{anomaly_index+1}/ {a["title"]}', expanded=False, icon=icon):
                    # Check if resolved
                    key = f"resolved_{dataset_index}_{indicator_index}_{anomaly_index}"

                    # Checkbox (if resolved)
                    st.checkbox(
                        "Mark as resolved",
                        value=a["resolved"],
                        key=key,
                    )

                    # Anomaly description
                    st.markdown(a["description"])
