"""Ask chat GPT questions about our metadata."""
import streamlit as st
from st_pages import add_indentation

from apps.wizard.utils.gpt import GPTQuery, OpenAIWrapper
from docs.ignore.gen_metadata_reference import render_dataset, render_indicator, render_origin, render_table
from etl.config import load_env
from etl.helpers import read_json_schema
from etl.paths import SCHEMAS_DIR

# CONFIG
add_indentation()
st.title("Metadata 🧐 **:gray[Expert]**")
st.markdown("Ask the Metadata Expert anything about the metadata.")

load_env()
SNAPSHOT_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "snapshot-schema.json")
DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")


@st.cache_data(show_spinner=True)
def ask_gpt(query):
    response = api.query_gpt(query)
    return response


# LOAD SCHEMAS
meta_origin = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]
meta_dataset = DATASET_SCHEMA["properties"]["dataset"]
meta_table = DATASET_SCHEMA["properties"]["tables"]
meta_indicator = DATASET_SCHEMA["properties"]["tables"]["additionalProperties"]["properties"]["variables"]

# GPT CONFIG
api = OpenAIWrapper(model="gpt-4")

SYSTEM_PROMPT = f"""
You are an expert in the metadata structure and fields used in ETL. The user will ask you questions about this metadata, and you will answer them to the best of your ability in the context of ETL.

At high level:
- The metadata consists of four entities: Origin, Dataset, Table, and Indicator (or Variable).
- A Dataset is an object which consists of a collection of Tables.
- A Table is an object, very similar to a pandas DataFrame, but with some additional metadata. A Table can have various Indicators.
- An Indicator (or Variable) is a column in a Table.
- Each Indicator can have a set of Origins, which define the source of the data.

Find below a detailed explanation of each metadata field.



{render_indicator()}

"""

tabs = st.tabs(["Origin", "Dataset", "Tables", "Indicators"])
with tabs[0]:
    st.write(render_origin())
with tabs[1]:
    st.write(render_dataset())
with tabs[2]:
    st.write(render_table())
with tabs[3]:
    st.write(render_indicator())
# {render_origin()}

# ---

# {render_dataset()}

# ---

# {render_table()}

# ---
# st.markdown(len(SYSTEM_PROMPT))
# Split system prompt into chunks of ~8000 characters
# N_CHUNKS = 5
# CHUNK_SIZE = len(SYSTEM_PROMPT) // N_CHUNKS
# sys_prompts = []
# for i in range(N_CHUNKS):
#     sys_prompts.append(SYSTEM_PROMPT[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE])

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

    # Build GPT query
    query = GPTQuery(messages=st.session_state.messages, temperature=0)
    # Ask GPT
    response = ask_gpt(query)
    # Display response
    if response is not None:
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            st.markdown(response.message_content)
            st.write(f"Cost: {response.cost}")
        # Add assistant response to chat history
        st.session_state.messages.append({"role": "assistant", "content": response.message_content})
    else:
        with st.chat_message("assistant"):
            st.markdown("Couldn't get a response. Please try again.")
