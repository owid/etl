"""Ask chat GPT questions about our our docs.

references:
- https://docs.streamlit.io/knowledge-base/tutorials/build-conversational-apps#build-a-chatgpt-like-app
"""

import time
from typing import cast

import streamlit as st
from pydantic_ai import Agent
from structlog import get_logger

from apps.utils.gpt import OpenAIWrapper, get_cost_and_tokens
from apps.wizard.app_pages.expert.prompts import (
    SYSTEM_PROMPT_DATASETTE,
    SYSTEM_PROMPT_GUIDES,
    SYSTEM_PROMPT_INTRO,
    SYSTEM_PROMPT_METADATA,
)
from apps.wizard.app_pages.expert.prompts_dynamic import (
    SYSTEM_PROMPT_DATABASE,
    SYSTEM_PROMPT_FULL,
)
from etl.config import load_env

st.set_page_config(
    page_title="Wizard: Ask the Expert",
    page_icon="🪄",
)

# "Summarize your knowledge from your system prompt into one short sentence"
# SYSTEM_PROMPT_GUIDES      70925   299632
# SYSTEM_PROMPT_INTRO       26500   109799
# SYSTEM_PROMPT_PRINCIPLES  17821   75429
# SYSTEM_PROMPT_METADATA    13609   54469
# SYSTEM_PROMPT_START       9195    34678
# SYSTEM_PROMPT_DATASETTE   3917    14850
# SYSTEM_PROMPT_DATABASE    256     934

# LOG
log = get_logger()

# PAGE CONFIG
## Title/subtitle
st.title(":rainbow[:material/lightbulb_2:] Expert :gray[v2]")
st.markdown(
    "Ask the Expert any questions about ETL! Alternatively, visit [**our documentation ↗**](https://docs.owid.io/projects/etl])."
)

## Load variables
load_env()

# SESSION STATE
st.session_state.setdefault("expert_config", {})

# GPT CONFIG
MODEL_DEFAULT = "gpt-5"
MODELS_AVAILABLE = {
    "gpt-5": "GPT-5",  # IN: US$1.25 / 1M tokens; OUT: US$10.00 / 1M tokens
    "gpt-5-mini": "GPT-5 mini",  # IN: US$0.25 / 1M tokens; OUT: US$2.00 / 1M tokens
    "gpt-4o": "GPT-4o",  # IN: US$5.00 / 1M tokens; OUT: US$15.00 / 1M tokens
    "o4-mini": "GPT o4-mini",  # IN: US$1.10 / 1M tokens; OUT: US$4.40 / 1M tokens
    # "gpt-4-turbo": "GPT-4 Turbo",  # IN: US$10.00 / 1M tokens; OUT: US$30.00 / 1M tokens  (gpt-4-turbo-2024-04-09)
}
MODELS_AVAILABLE_LIST = list(MODELS_AVAILABLE.keys())
# Some models don't support certain arguments (I think these are the "mini" ones)
MODELS_DIFFERENT_API = {"o4-mini", "gpt-5", "gpt-5-mini"}

########### MODELS (PyDantic AI)
# See all of them in https://github.com/pydantic/pydantic-ai/blob/master/pydantic_ai_slim/pydantic_ai/models/__init__.py
MODELS_PYDANTIC = {
    'openai:gpt-5': "GPT-5",
    'openai:gpt-5-mini': "GPT-5 mini",
    'openai:gpt-4o': "GPT-4o",
    'openai:o3': "GPT o3",
    'anthropic:claude-sonnet-4-0': "Claude Sonnet 4.0",
    'google-gla:gemini-2.5-flash': "Gemini 2.5 Flash",
}

# CATEGORY FOR CHAT
# Chat category-switching
class Options:
    """Chat categories."""

    FULL = "**⭐️ All**"
    DATASETTE = "Datasette"
    DATABASE = "Analytics"
    METADATA = "ETL Metadata"
    INTRO = "Introduction"
    GUIDES = "Learn more"
    DEBUG = "Debug"


# Switch category function
def get_system_prompt() -> str:
    """Get appropriate system prompt."""
    # Choose context to provide to GPT
    match st.session_state["category_gpt"]:
        case Options.METADATA:
            log.info("Switching to 'Metadata' system prompt.")
            system_prompt = SYSTEM_PROMPT_METADATA
        case Options.INTRO:
            log.info("Switching to 'Getting started'/Design principles system prompt.")
            system_prompt = SYSTEM_PROMPT_INTRO
        case Options.GUIDES:
            log.info("Switching to 'Guides' system prompt.")
            system_prompt = SYSTEM_PROMPT_GUIDES
        case Options.FULL:
            log.warning("Switching to 'All' system prompt.")
            system_prompt = SYSTEM_PROMPT_FULL
        case Options.DATASETTE:
            log.warning("Switching to 'DATASETTE' system prompt.")
            system_prompt = SYSTEM_PROMPT_DATASETTE
        case Options.DATABASE:
            log.warning("Switching to 'DATABASE' system prompt.")
            system_prompt = SYSTEM_PROMPT_DATABASE
        case Options.DEBUG:
            log.warning("Switching to 'DEBUG' system prompt.")
            system_prompt = ""
        case _:
            log.info("Nothing selected. Switching to 'All' system prompt.")
            system_prompt = SYSTEM_PROMPT_FULL
    return system_prompt


# Reset chat history
def reset_messages() -> None:
    """Reset messages to default."""
    st.exception(Exception("This hasn't been implemented yet"))


def config_category():
    # CONFIG part 1: category
    options = [
        Options.FULL,
        Options.DATABASE,
        Options.METADATA,
        Options.INTRO,
        Options.GUIDES,
    ]
    st.segmented_control(
        label="Choose a category for the question",
        options=options,
        default=options[0],
        help="Choosing a specific domain reduces the cost of the query to chatGPT, because only a subset of the documentation (i.e. fewer tokens used) will be used in the query.",
        key="category_gpt",
        on_change=reset_messages,
        width="stretch",
    )

    ## EXAMPLE QUERIES
    if st.session_state["category_gpt"] in {Options.DATASETTE, Options.DATABASE}:
        EXAMPLE_QUERIES = [
            "> Which are our top 10 articles by pageviews?",
            "> How many charts do we have that use only a single indicator?",
            "> Do we have datasets whose indicators are not used in any chart?",
        ]
    else:
        EXAMPLE_QUERIES = [
            "> In the metadata yaml file, which field should I use to disable the map tap view?",
            "> In the metadata yaml file, how can I define a common `description_processing` that affects all indicators in a specific table?"
            "> What is the difference between `description_key` and `description_from_producer`? Be concise.",
            "> Is the following snapshot title correct? 'Cherry Blossom Full Blook Dates in Kyoto, Japan'",
            "> What is the difference between an Origin and Dataset?",
        ]
    with st.popover("Examples"):
        for example in EXAMPLE_QUERIES:
            st.markdown(example)


def config_model():
    # Model
    model_name = st.selectbox(
        label=":material/memory: Select model",
        options=list(MODELS_PYDANTIC.keys()),
        format_func=lambda x: MODELS_PYDANTIC[x],
        index=MODELS_AVAILABLE_LIST.index(MODEL_DEFAULT),
        help="[Pricing](https://openai.com/api/pricing) | [Model list](https://platform.openai.com/docs/models/)",
    )
    # Max tokens
    max_tokens = int(
        st.number_input(
            "Max tokens",
            min_value=32,
            max_value=4 * 4096,
            value=4096,
            step=32,
            help="The maximum number of tokens in the response.",
        )
    )
    # Temperature
    if model_name not in MODELS_DIFFERENT_API:
        temperature = st.number_input(
            "Temperature",
            min_value=0.0,
            max_value=2.0,
            value=1.0 if model_name in MODELS_DIFFERENT_API else 0.15,
            step=0.01,
            help="What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.",
        )
    else:
        temperature = 1.0
    # Reduced context
    use_reduced_context = st.toggle(
        "Low context",
        value=False,
        help="If checked, only the last user message will be accounted (i.e less tokens and therefore cheaper).",
    )

    # Add to session state
    st.session_state["expert_config"]["model_name"] = model_name
    st.session_state["expert_config"]["max_tokens"] = max_tokens
    st.session_state["expert_config"]["temperature"] = temperature
    st.session_state["expert_config"]["use_reduced_context"] = use_reduced_context


def config_others():
    st.button(
        label=":material/restart_alt: Clear chat",
        on_click=reset_messages,
    )
    with st.popover("Inspect system prompt", icon=":material/text_snippet:"):
        prompt = get_system_prompt()
        st.text(prompt)


container_chat = st.container()

### LLM CONFIG
with st.expander(f"**Model** :gray[(default is {MODEL_DEFAULT})]", icon=":material/settings:"):
    # 1/ Category
    with st.container(horizontal=True, vertical_alignment="bottom"):
        config_category()
    # 2/ Model
    st.session_state.analytics = st.session_state.get("analytics", True)
    with st.container(horizontal=True, vertical_alignment="bottom"):
        config_model()
    # 3/ Others
    with st.container(horizontal=True, vertical_alignment="bottom"):
        config_others()


# CHAT INTERFACE
with container_chat:
    # Pydantic AI Agent
    # agent = Agent(
    #     model=st.session_state["expert_config"]["model_name"],
    #     system_prompt=get_system_prompt(),
    # )

    agent = Agent(
        model="openai:gpt-5-mini",
        system_prompt=get_system_prompt(),
    )

    # api = OpenAIWrapper()

    # Initialize chat history
    # if "messages" not in st.session_state:
    #     reset_messages()

    # Display chat messages from history on app rerun
    # for message in st.session_state.messages:
    #     if message["role"] != "system":
    #         avatar = None
    #         if message["role"] == "user":
    #             avatar = ":material/person:"
    #         with st.chat_message(message["role"], avatar=avatar):
    #             st.markdown(message["content"])

    # Initialise session state
    st.session_state.response = st.session_state.get("response", None)
    st.session_state.prompt = st.session_state.get("prompt", None)
    st.session_state.feedback_key = st.session_state.get("feedback_key", 0)
    st.session_state.cost_last = st.session_state.get("cost_last", 0)

    # Placeholder for response
    container_response = st.container()

    # React to user input
    if prompt := st.chat_input("Ask me any question!"):
        st.session_state.feedback_key += 1
        st.toast("Agent working...", icon=":material/robot:")
        # Display user message in chat message container
        with container_response:
            with st.chat_message("user", avatar=":material/person:"):
                st.markdown(prompt)

        # Add user message to chat history
        # st.session_state.messages.append({"role": "user", "content": prompt})

        # Build GPT query (only use the system prompt and latest user input)
        # if st.session_state["expert_config"]["use_reduced_context"]:
        #     messages = [{"role": "system", "content": get_system_prompt()}, {"role": "user", "content": prompt}]
        # else:
        #     messages = st.session_state.messages

        # Display assistant response in chat message container
        with container_response:
            with st.chat_message("assistant"):
                # Start timer
                start_time = time.time()

                # Put agent to work
                async def agent_stream():
                    async with agent.run_stream(prompt) as result:
                        # """Stream agent response."""
                        # Yield each message from the stream
                        async for message in result.stream_text(delta=True):
                            if message is not None:
                                yield message

                    # At the very end, after the streaming is complete
                    # Capture the usage information in session state
                    if hasattr(result, 'usage'):
                        st.session_state['last_usage'] = result.usage()

                # text = agent.run_sync(prompt)

                # st.text(text)
                st.session_state.response = cast(str, st.write_stream(agent_stream))

                if 'last_usage' in st.session_state:
                    st.info(st.session_state['last_usage'])
                # We'll gather partial text to show incrementally
                # partial_text = ""
                # message_placeholder = st.empty()

                # # Render partial text as it arrives
                # async for chunk in result.stream_text(delta=True):
                #     partial_text += chunk
                #     message_placeholder.markdown(partial_text)

                # End timer and store duration
                end_time = time.time()
                st.session_state.response_time = end_time - start_time

            # Add new response by the System
            # st.session_state.messages.append({"role": "assistant", "content": st.session_state.response})

            # Add prompt to session state
            # st.session_state.prompt = prompt

            print("finished asking GPT...")


    # if st.session_state.response:
    #     # Get cost & tokens
    #     text_in = "\n".join([m["content"] for m in st.session_state.messages])
    #     cost, num_tokens = get_cost_and_tokens(
    #         text_in, st.session_state.response, cast(str, st.session_state["expert_config"]["model_name"])
    #     )

    #     # Format response time
    #     response_time = getattr(st.session_state, "response_time", 0)
    #     time_msg = f"⏱️ {response_time:.2f}s"

    #     cost_msg = f"≥{cost:.4f} USD (≥{num_tokens:,} tokens), {time_msg}, using **{MODELS_AVAILABLE[st.session_state['model_name']]}**"
    #     st.session_state.cost_last = cost

    #     # Show cost below feedback
    #     with container_response:
    #         with st.container(horizontal=True, horizontal_alignment="right"):
    #             st.markdown(f":blue-badge[:small[:material/paid: {cost_msg}]]")
