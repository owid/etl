import os
import re
import subprocess
import sys
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
from git.exc import GitCommandError
from git.repo import Repo
from rich.ansi import AnsiDecoder
from rich.console import Console
from rich.terminal_theme import MONOKAI

from etl.paths import BASE_DIR

CURRENT_DIR = Path(__file__).resolve().parent


def main():
    st.title("etl-staging-sync")
    st.markdown(
        """
    Sync charts and revisions from staging server to production.
    """
    )

    # SIDEBAR
    with st.sidebar:
        with open(CURRENT_DIR / "instructions.md", "r") as f:
            st.markdown(f.read())

    st.markdown("### Config")
    source = st.text_input(
        "Source",
        placeholder="my-branch",
        help="Name of the branch to sync from (with existing `staging-site-mybranch` server).",
    )
    target = st.text_input("Target", value="live", help="Using `live` uses DB from local `.env` file as target.")
    publish = st.checkbox(
        "Automatically publish new charts", value=False, help="Otherwise you'd have to publish new charts manually."
    )
    approve_revisions = st.checkbox(
        "Automatically approve chart revisions for edited charts",
        value=False,
        help=" This still creates a chart revision if the target chart has been modified.",
    )
    dry_run = st.checkbox("Dry run", value=True)

    # Live uses `.env` file which points to the live database in production
    if target == "live":
        target = ".env"

    # Button to show text
    if st.button("Sync charts", help="This can take a while."):
        if not _is_valid_config(source, target):
            return

        # Open the local repository
        repo = Repo(BASE_DIR)

        # Fetch the specific branch from the remote
        try:
            repo.git.fetch("origin", source)
        except GitCommandError:
            st.error(f"Branch {source} not found in owid/etl repository.")
            sys.exit(1)

        cmd = ["poetry", "run", "etl-staging-sync", source, "master"]
        if dry_run:
            cmd.append("--dry-run")
        if publish:
            cmd.append("--publish")
        if approve_revisions:
            cmd.append("--approve-revisions")

        _run_command(cmd)


def _is_valid_config(source: str, target: str) -> bool:
    if source.strip() == "":
        st.warning("Please enter a valid source.")
        return False
    if target.strip() == "":
        st.warning("Please enter a valid target.")
        return False
    return True


def _run_command(cmd: list[str]):
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
    # st.markdown(f"`{' '.join(cmd)}`")
    components.html(html, scrolling=True, height=n_lines * 21)


def _strip_log_timestamps(stdout: str) -> str:
    pattern = r"\x1b\[\dm\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\x1b\[0m "
    return re.sub(pattern, "", stdout)


def cli():
    subprocess.run(["streamlit", "run", f"{CURRENT_DIR}/app.py"])


if __name__ == "__main__":
    main()
