import os
import re
import subprocess

import streamlit as st
import streamlit.components.v1 as components
from rich.ansi import AnsiDecoder
from rich.console import Console
from rich.terminal_theme import MONOKAI


def run_command(cmd: list[str]):
    """Run a bash command with `subprocess.Popen` and show the output in a streamlit component."""
    console = Console(record=True)

    # Run bash command
    my_env = os.environ.copy()
    my_env["FORCE_COLOR"] = "1"
    result = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=my_env)
    stdout, stderr = result.communicate()

    stdout = stdout.decode()
    stderr = stderr.decode()

    if stderr:
        st.markdown("### Error")
        st.error(stderr)

    stdout = _strip_log_timestamps(stdout)

    decoder = AnsiDecoder()

    n_lines = 0
    for line in decoder.decode(stdout):
        n_lines += 1
        console.print(line, soft_wrap=True)

    html = console.export_html(inline_styles=True, theme=MONOKAI)

    st.markdown("### Output")
    components.html(html, scrolling=True, height=n_lines * 21)


def _strip_log_timestamps(stdout: str) -> str:
    pattern = r"\x1b\[\dm\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\x1b\[0m "
    return re.sub(pattern, "", stdout)
