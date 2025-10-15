import tempfile
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from apps.wizard.app_pages.expert_agent.utils import log


def display_generated_plots(question_id: str) -> None:
    """Discover and display any plots generated for the given question_id.

    Args:
        question_id: The unique identifier for the question/conversation turn
    """
    try:
        # Look for plot files in the temp directory
        temp_dir = Path(tempfile.gettempdir())
        all_files = list(temp_dir.glob(f"{question_id}_plot_*"))

        if not all_files:
            return  # No plots generated

        # Group files by their base name (without extension)
        plot_groups = {}
        for file_path in all_files:
            # Extract the base name (e.g., "chat_q_12345678_plot_abcd1234")
            base_name = file_path.stem
            if base_name not in plot_groups:
                plot_groups[base_name] = {"plot_file": None, "code_file": None}

            if file_path.suffix.lower() in [".png", ".html"]:
                plot_groups[base_name]["plot_file"] = file_path
            elif file_path.suffix.lower() == ".py":
                plot_groups[base_name]["code_file"] = file_path

        # Display each plot group
        for count, (base_name, files) in enumerate(sorted(plot_groups.items())):
            plot_file = files["plot_file"]
            code_file = files["code_file"]

            if plot_file:
                if count > 0:
                    st.markdown("---")  # Add separator between plots

                # Display the plot
                if plot_file.suffix.lower() == ".png":
                    st.image(str(plot_file), caption=f"Generated plot: {plot_file.name}")

                elif plot_file.suffix.lower() == ".html":
                    try:
                        with open(plot_file, "r") as f:
                            html_content = f.read()
                        components.html(html_content, height=600, scrolling=True)
                        st.caption(f"Interactive plot: {plot_file.name}")
                    except Exception as e:
                        log.warning(f"Could not display HTML plot {plot_file}: {e}")
                        _create_download_button(plot_file, "text/html")

                # Display the code in an expandable section
                if code_file and code_file.exists():
                    try:
                        with open(code_file, "r") as f:
                            code_content = f.read()

                        with st.expander(":material/code: View Generated Code", expanded=False):
                            st.code(code_content, language="python")

                            # Add download button for the code
                            st.download_button(
                                label="📥 Download Code",
                                data=code_content,
                                file_name=f"{base_name}.py",
                                mime="text/x-python",
                                help="Download the Python code used to generate this plot",
                            )
                    except Exception as e:
                        log.warning(f"Could not display code file {code_file}: {e}")

                # Add download button for the plot if it's not PNG
                if plot_file.suffix.lower() != ".png":
                    _create_download_button(plot_file)

            elif files["code_file"]:  # Only code file, no plot (shouldn't normally happen)
                st.warning(f"Found code file {files['code_file'].name} but no corresponding plot")

    except Exception as e:
        log.error(f"Error displaying plots for question {question_id}: {e}")
        # Don't show error to user, just log it


def _create_download_button(file_path: Path, mime_type: str | None = None) -> None:
    """Create a download button for a file.

    Args:
        file_path: Path to the file
        mime_type: MIME type for the download button
    """
    try:
        with open(file_path, "rb") as f:
            file_data = f.read()

        st.download_button(
            label=f":material/download: Download: {file_path.name}",
            data=file_data,
            file_name=file_path.name,
            mime=mime_type,
        )
    except Exception as e:
        log.warning(f"Could not create download button for {file_path}: {e}")


def save_code_file(plotting_code: str, filename_base: str, question_id: str, card_id: int) -> str:
    """Save the plotting code to a Python file with data fetching logic.

    Args:
        plotting_code: The generated plotting code
        filename_base: Base filename without extension
        question_id: Question ID for context
        card_id: Metabase card ID for data fetching

    Returns:
        str: Full path to the saved code file
    """
    code_filename = f"{filename_base}.py"
    code_filepath = Path(tempfile.gettempdir()) / code_filename

    with open(code_filepath, "w") as f:
        f.write(f"# Generated plotting code for question {question_id}\n")
        f.write(f"# Data fetched from Metabase card ID: {card_id}\n\n")

        f.write("import pandas as pd\n")
        f.write("import plotly.express as px\n")
        f.write("import plotly.graph_objects as go\n")
        f.write("from etl.analytics.metabase import get_question_data\n\n")

        f.write("# Fetch data from Metabase\n")
        f.write(f"df = get_question_data({card_id})\n\n")

        f.write("# Generated plotting code:\n")
        f.write(plotting_code)
        f.write("\n\n# Display the plot\nfig.show()")

    log.info(f"Generated plot code saved to: {code_filepath}")
    return str(code_filepath)


def save_plot_file(fig, filename_base: str) -> str:
    """Save plot file, trying PNG first, then falling back to HTML.

    Args:
        fig: Plotly figure object
        filename_base: Base filename without extension

    Returns:
        str: Full path to the saved file
    """
    try:
        png_filename = f"{filename_base}.png"
        png_filepath = Path(tempfile.gettempdir()) / png_filename
        fig.write_image(str(png_filepath), width=1200, height=800)
        log.info(f"Generated plot saved as PNG: {png_filepath}")
        return str(png_filepath)
    except Exception as png_error:
        # Fallback to HTML format
        log.warning(f"PNG export failed ({png_error}), saving as HTML instead")
        html_filename = f"{filename_base}.html"
        html_filepath = Path(tempfile.gettempdir()) / html_filename
        fig.write_html(str(html_filepath))
        log.info(f"Generated plot saved as HTML: {html_filepath}")
        return str(html_filepath)
