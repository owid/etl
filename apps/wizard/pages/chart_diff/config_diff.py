import difflib
import json

import streamlit as st


def st_show_diff(config_1, config_2) -> None:
    config_1 = json.dumps(config_1, indent=4)
    config_2 = json.dumps(config_2, indent=4)

    diff = difflib.unified_diff(
        config_1.splitlines(keepends=True), config_2.splitlines(keepends=True), fromfile="production", tofile="staging"
    )

    diff_string = "".join(diff)

    st.code(diff_string, line_numbers=True, language="diff")
