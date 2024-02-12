"""Ask chat GPT questions about our metadata.

references:
- https://docs.streamlit.io/knowledge-base/tutorials/build-conversational-apps#build-a-chatgpt-like-app
"""
from typing import cast

import streamlit as st
from st_pages import add_indentation

from apps.wizard.pages.meta_expert.prompts import SYSTEM_PROMPT_FULL, SYSTEM_PROMPT_REDUCED
from apps.wizard.utils.gpt import OpenAIWrapper, get_cost_and_tokens
from etl.config import load_env

# CONFIG
st.set_page_config(page_title="Wizard: Ask the Metadata Expert", page_icon="🪄")
add_indentation()
## Title/subtitle
st.title("Metadata 🧙 **:gray[Expert]**")
st.markdown("Ask the Expert any questions about the metadata!")
## Examples
EXAMPLE_QUERIES = [
    "What is the difference between `description_key` and `description_from_producer`? Be concise.",
    "Is the following snapshot title correct? 'Cherry Blossom Full Blook Dates in Kyoto, Japan'",
    "What is the difference between an Origin and Dataset?",
]
with st.expander("See examples"):
    for example in EXAMPLE_QUERIES:
        st.caption(example)
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


# Sidebar with GPT config
with st.sidebar:
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

    use_all_context = st.toggle(
        "Full chat as context",
        value=True,
        help="If set to True, all the messages in the chat are used to query GPT (i.e. more tokens, i.e. more expensive). Unselect for cheaper usage.",
    )
    use_full_docs = st.toggle(
        "Reduced docs",
        value=False,
        help="If set to True, a reduced ETL documentation is used in the GPT query. Otherwise, the complete documentation is used (slightly more costly)",
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

if use_full_docs:
    SYSTEM_PROMPT = SYSTEM_PROMPT_FULL
else:
    SYSTEM_PROMPT = SYSTEM_PROMPT_REDUCED
api = OpenAIWrapper()

# ACTUAL APP
# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system", "content": SYSTEM_PROMPT}]

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
# Reduce to only systme prompt
# st.session_state.messages = st.session_state.messages[-2:]

# React to user input
if prompt := st.chat_input("Ask me!"):
    # Display user message in chat message container
    st.chat_message("user").markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Build GPT query (only use the system prompt and latest user input)
    if use_all_context:
        messages = st.session_state.messages
    else:
        messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}]

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
        response = cast(str, st.write_stream(stream))
        # st.markdown(response.message_content)
        # st.info(f"Cost: {response.cost} USD. \nTokens: {response.usage.total_tokens}.")
        # Add assistant response to chat history

    # Get cost & tokens
    text_in = "\n".join([m["content"] for m in st.session_state.messages])
    cost, num_tokens = get_cost_and_tokens(text_in, response, cast(str, model_name))
    cost_msg = f"**Cost**: ≥{cost} USD.\n\n **Tokens**: ≥{num_tokens}."
    st.info(cost_msg)

    # Add new response by the System
    st.session_state.messages.append({"role": "assistant", "content": response})
