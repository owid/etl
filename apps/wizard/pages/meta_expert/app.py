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
st.title("Metadata 🧙 **:gray[Expert]**")
st.markdown("Ask me anything about the metadata!")

load_env()


@st.cache_data(show_spinner=True)
def ask_gpt(query, model):
    response = api.query_gpt(query, model=model)
    return response


# GPT CONFIG
MODEL_DEFAULT = "gpt-3.5-turbo-0125"
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
    )
    use_all_context = st.toggle(
        "Full context window",
        value=True,
        help="If set to True, all the messages in the chat are used to query GPT (i.e. more tokens, i.e. more expensive). Unselect for cheaper usage.",
    )
    temperature = st.slider("Temperature", min_value=0.0, max_value=2.0, value=0.5, step=0.01)
    max_tokens = int(st.number_input("Max tokens", min_value=32, max_value=2048, value=512, step=32))

if model_name == "gpt-4-turbo-preview":
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
if prompt := st.chat_input("Ask a question about the metadata"):
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
    st.info(st.session_state.cost_msg)

    # Add new response by the System
    st.session_state.messages.append({"role": "assistant", "content": response})
