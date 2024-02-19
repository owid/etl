import subprocess
from pathlib import Path

import streamlit as st
from st_pages import add_indentation

from apps.utils import run_command

CURRENT_DIR = Path(__file__).resolve().parent
add_indentation()


def main():
    st.title("Chart 🔄 **:gray[Sync]**")
    st.markdown(
        """
    Synchronize charts and revisions from the source server to the target server. Typically, the source is a
    **staging server** and the target is **live**. However, syncing charts between staging servers is also possible.

    Previously, the process required merging your branch first, then waiting for the ETL to build the dataset
    before creating charts in the live environment. In some cases, it was even necessary to push your dataset
    directly to live. This tool allows you to first create charts on the staging server and then synchronize
    them to the live environment after merging.
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
        help="Branch name of PR that created the staging server (with existing `staging-site-mybranch` server) or the name of staging server.",
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

        cmd = ["poetry", "run", "etl", "chart-sync", source, target]
        if dry_run:
            cmd.append("--dry-run")
        if publish:
            cmd.append("--publish")
        if approve_revisions:
            cmd.append("--approve-revisions")

        run_command(cmd)


def _is_valid_config(source: str, target: str) -> bool:
    if source.strip() == "":
        st.warning("Please enter a valid source.")
        return False
    if target.strip() == "":
        st.warning("Please enter a valid target.")
        return False
    return True


def cli():
    subprocess.run(["streamlit", "run", f"{CURRENT_DIR}/app.py"])


if __name__ == "__main__":
    main()
