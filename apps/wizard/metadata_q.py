"""Ask chat GPT questions about our metadata."""
import json

import streamlit as st

from etl.helpers import read_json_schema
from etl.paths import SCHEMAS_DIR

SNAPSHOT_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "snapshot-schema.json")
DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")


st.markdown("### Origin")
snap_meta = SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"]
snap_meta = json.loads(str(snap_meta))
print(SNAPSHOT_SCHEMA["properties"]["meta"]["properties"]["origin"])
st.write(snap_meta)
st.divider()

# st.markdown("### Dataset")
# st.write(DATASET_SCHEMA["properties"]["dataset"])
# st.divider()

# st.markdown("### Table")
# st.write(DATASET_SCHEMA["properties"]["dataset"])
# st.divider()

# st.markdown("### Indicators")
# st.write(DATASET_SCHEMA["properties"]["tables"]["additionalProperties"]["properties"]["variables"])
# st.divider()

st.chat_input("Ask me something about metadata")
st.divider()
