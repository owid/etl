"""Ask chat GPT questions about our metadata.

references:
- https://docs.streamlit.io/knowledge-base/tutorials/build-conversational-apps#build-a-chatgpt-like-app
"""
import streamlit as st
from st_pages import add_indentation

from apps.wizard.pages.meta_expert.prompts import SYSTEM_PROMPT_FULL, SYSTEM_PROMPT_REDUCED
from apps.wizard.utils.gpt import GPTQuery, OpenAIWrapper, get_cost
from etl.config import load_env

# CONFIG
add_indentation()
st.title("Metadata 🧙 **:gray[Expert]**")
st.markdown(
    "Ask the Metadata Expert anything about the metadata. Currently, only the last user message is used to query GPT."
)

load_env()


@st.cache_data(show_spinner=True)
def ask_gpt(query, model):
    response = api.query_gpt(query, model=model)
    return response


# GPT CONFIG
MODELS_AVAILABLE = {
    "gpt-3.5-turbo-0125": "GPT-3.5 Turbo",
    "gpt-4-turbo-preview": "GPT-4 Turbo",
}
model_name = st.selectbox("Select GPT model", list(MODELS_AVAILABLE.keys()), format_func=lambda x: MODELS_AVAILABLE[x])
api = OpenAIWrapper()
# api = OpenAI()

if model_name == "gpt-4-turbo-preview":
    SYSTEM_PROMPT = SYSTEM_PROMPT_FULL
else:
    SYSTEM_PROMPT = SYSTEM_PROMPT_REDUCED

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
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}]
    messages = st.session_state.messages
    query = GPTQuery(messages=messages, temperature=0)
    # Ask GPT
    # response = ask_gpt(query, model=model_name)

    # response = api.chat.completions.create(model=model_name, messages=messages, temperature=0, stream=True)
    # chat_completion = GPTResponse(response)
    # Display response
    # if stream is not None:
    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        stream = api.chat.completions.create(
            model=model_name,
            messages=st.session_state.messages,
            temperature=0,
            stream=True,
        )
        response = st.write_stream(stream)
        # st.markdown(response.message_content)
        # st.info(f"Cost: {response.cost} USD. \nTokens: {response.usage.total_tokens}.")
        # Add assistant response to chat history

        # Get cost
        text_in = "\n".join([m["content"] for m in st.session_state.messages])
        cost, num_tokens = get_cost(text_in, response, model_name)
        st.info(f"**Cost**: {cost} USD.\n\n **Tokens**: {num_tokens}.")
        # Add new response by the System
        st.session_state.messages.append({"role": "assistant", "content": response})

        # cost = get_cost()
        # st.toast(f"Cost: {response.cost} USD!", icon="💰")
    # else:
    #     with st.chat_message("assistant"):
    #         st.markdown("Couldn't get a response. Please try again.")
