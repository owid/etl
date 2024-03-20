"""Ask chat GPT questions about our our docs.

references:
- https://docs.streamlit.io/knowledge-base/tutorials/build-conversational-apps#build-a-chatgpt-like-app
"""
from typing import cast

import streamlit as st
from st_pages import add_indentation
from streamlit_feedback import streamlit_feedback
from structlog import get_logger

from apps.wizard.pages.expert.prompts import (
    SYSTEM_PROMPT_FULL,
    SYSTEM_PROMPT_GUIDES,
    SYSTEM_PROMPT_METADATA,
    SYSTEM_PROMPT_PRINCIPLES,
    SYSTEM_PROMPT_START,
)
from apps.wizard.utils import set_states
from apps.wizard.utils.gpt import OpenAIWrapper, get_cost_and_tokens
from etl.config import load_env

# LOG
log = get_logger()

# CONFIG
st.set_page_config(page_title="Wizard: Ask the Expert", page_icon="🪄")
add_indentation()
## Title/subtitle
st.title("**Expert** 🧙")
st.markdown("Ask the Expert any questions about ETL!")

## Load variables
load_env()


@st.cache_data(show_spinner=True)
def ask_gpt(query, model):
    response = api.query_gpt(query, model=model)
    return response


# GPT CONFIG
MODEL_DEFAULT = "gpt-4-turbo-preview"
MODELS_AVAILABLE = {
    "gpt-3.5-turbo-0125": "GPT-3.5 Turbo (gpt-3.5-turbo-0125)",
    "gpt-4-turbo-preview": "GPT-4 Turbo (gpt-4-turbo-preview)",
}
MODELS_AVAILABLE_LIST = list(MODELS_AVAILABLE.keys())


# CATEGORY FOR CHAT
# Chat category-switching
class Options:
    """Chat categories."""

    METADATA = "Metadata"
    START = "Setting up your environment"
    GUIDES = "How to use, tools, APIs, and guides"
    PRINCIPLES = "Design principles"
    FULL = "Complete documentation"
    DEBUG = "Debug"


# Handle feedback
def handle_feedback(feedback) -> None:
    print("handle feedback")
    print(feedback)


# Switch category function
def get_system_prompt() -> str:
    """Get appropriate system prompt."""
    # Choose context to provide to GPT
    match st.session_state["category_gpt"]:
        case Options.METADATA:
            log.info("Switching to 'Metadata' system prompt.")
            system_prompt = SYSTEM_PROMPT_METADATA
        case Options.START:
            log.info("Switching to 'Getting started' system prompt.")
            system_prompt = SYSTEM_PROMPT_START
        case Options.GUIDES:
            log.info("Switching to 'Guides' system prompt.")
            system_prompt = SYSTEM_PROMPT_GUIDES
        case Options.PRINCIPLES:
            log.info("Switching to 'Design principles' system prompt.")
            system_prompt = SYSTEM_PROMPT_PRINCIPLES
        case Options.FULL:
            log.warning("Switching to 'All' system prompt.")
            system_prompt = SYSTEM_PROMPT_FULL
        case Options.DEBUG:
            log.warning("Switching to 'DEBUG' system prompt.")
            system_prompt = ""
        case _:
            log.info("Nothing selected. Switching to 'All' system prompt.")
            system_prompt = SYSTEM_PROMPT_FULL
    return system_prompt


def handle_category_switch() -> None:
    """Change the system prompt."""
    system_prompt = get_system_prompt()
    st.session_state.messages = [{"role": "system", "content": system_prompt}]


# Category for the chat
st.selectbox(
    label="Choose a category for the question",
    options=[
        Options.FULL,
        Options.METADATA,
        Options.START,
        Options.GUIDES,
        Options.PRINCIPLES,
        Options.DEBUG,
    ],
    index=1,
    help="Choosing a domain reduces the cost of the query to chatGPT, since only a subset of the documentation will be used in the query (i.e. fewer tokens used).",
    key="category_gpt",
    on_change=handle_category_switch,
)

## Examples
EXAMPLE_QUERIES = [
    "How can I add new dataset to ETL?",
    "What is the difference between `description_key` and `description_from_producer`? Be concise.",
    "Is the following snapshot title correct? 'Cherry Blossom Full Blook Dates in Kyoto, Japan'",
    "What is the difference between an Origin and Dataset?",
]
with st.popover("See examples"):
    for example in EXAMPLE_QUERIES:
        st.caption(example)

# Sidebar with GPT config
with st.sidebar:
    st.button(
        label="Clear chat",
        on_click=lambda: set_states({"messages": [{"role": "system", "content": get_system_prompt()}]}),
    )
    st.divider()
    st.markdown("## GPT Configuration")
    model_name = st.selectbox(
        label="Select GPT model",
        options=MODELS_AVAILABLE_LIST,
        format_func=lambda x: MODELS_AVAILABLE[x],
        index=MODELS_AVAILABLE_LIST.index(MODEL_DEFAULT),
        help="[Pricing](https://openai.com/pricing) | [Model list](https://platform.openai.com/docs/models/gpt-4-and-gpt-4-turbo)",
    )
    ## See pricing list: https://openai.com/pricing (USD)
    ## See model list: https://platform.openai.com/docs/models/gpt-4-and-gpt-4-turbo

    use_reduced_context = st.toggle(
        "Reduced context window",
        value=False,
        help="If checked, only the last user message will be accounted (i.e less tokens and therefore cheaper).",
    )
    temperature = st.slider(
        "Temperature",
        min_value=0.0,
        max_value=2.0,
        value=0.15,
        step=0.01,
        help="What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.",
    )
    max_tokens = int(
        st.number_input(
            "Max tokens",
            min_value=32,
            max_value=2048,
            value=512,
            step=32,
            help="The maximum number of tokens in the response.",
        )
    )

# API with OPENAI
api = OpenAIWrapper()

# ACTUAL APP
# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system", "content": SYSTEM_PROMPT_METADATA}]

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
# Reduce to only systme prompt
st.session_state.response = st.session_state.get("response", None)

# React to user input
response = None
if prompt := st.chat_input("Ask me!"):
    # Display user message in chat message container
    st.chat_message("user").markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Build GPT query (only use the system prompt and latest user input)
    if use_reduced_context:
        messages = [{"role": "system", "content": get_system_prompt()}, {"role": "user", "content": prompt}]
    else:
        messages = st.session_state.messages

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        # Ask GPT (stream)
        stream = api.chat.completions.create(
            model=cast(str, model_name),
            messages=messages,  # type: ignore
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )
        st.session_state.response = cast(str, st.write_stream(stream))

if st.session_state.response:
    feedback = streamlit_feedback(
        feedback_type="thumbs",
        # optional_text_label="[Optional] Please provide an explanation",
        # align="flex-start",
        key="fb_k",
        on_submit=handle_feedback,
    )

    # Get cost & tokens
    text_in = "\n".join([m["content"] for m in st.session_state.messages])
    cost, num_tokens = get_cost_and_tokens(text_in, st.session_state.response, cast(str, model_name))
    cost_msg = f"**Cost**: ≥{cost} USD.\n\n **Tokens**: ≥{num_tokens}."
    st.info(cost_msg)

    # Add new response by the System
    st.session_state.messages.append({"role": "assistant", "content": st.session_state.response})
