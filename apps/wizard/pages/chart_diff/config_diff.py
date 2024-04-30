import difflib
import json

import streamlit as st


def process_diff(diff):
    processed_diff = []
    for line in diff:
        if line.startswith("+"):
            # Additions in green
            processed_diff.append(f":green[{line}]")
        elif line.startswith("-"):
            # Deletions in red
            processed_diff.append(f":red[{line}]")
        else:
            # Unchanged lines
            processed_diff.append(line)
    return processed_diff


def st_show_diff(config_1, config_2):
    config_1 = json.dumps(config_1, indent=4)
    config_2 = json.dumps(config_2, indent=4)
    diff = difflib.ndiff(config_1.splitlines(keepends=True), config_2.splitlines(keepends=True))
    processed_diff = process_diff(diff)
    with st.container(border=True):
        st.markdown("<br>".join(processed_diff), unsafe_allow_html=True)
