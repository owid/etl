"""
NOTES:

Hi Lars! How are you doing :3?

General notes:
- We will build this app incrementally, following the workshop slides.
- If you have doubts, you can find the final version at workshop/reference/final.py

Run the app:
- To run this app, run: `streamlit run --server.port 20003 workshop/lars.py`.
- Go to localhost:20003 (you might need to use port-forwarding)
"""


import streamlit as st

# get dataset names, wait for selection
dataset_names = ["dataset_1", "dataset_2"]
dataset_selected = st.selectbox(
    label="Select a dataset",
    options=dataset_names,
)
# action once user has selected a dataset
if dataset_selected:
    st.write(f"Loading {dataset_selected}...")
