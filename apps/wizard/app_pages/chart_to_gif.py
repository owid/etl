from pathlib import Path

import streamlit as st
from structlog import get_logger

from apps.chart_to_gif.cli import (
    DOWNLOADS_DIR,
    create_gif_from_images,
    get_chart_slug,
    get_images_from_chart_url,
    get_years_in_chart,
)
from etl.config import ENV

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Chart to GIF",
    page_icon="🪄",
)

########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/tv: Chart to GIF")

# Initialize session state for generated files.
if "images_folder" not in st.session_state:
    st.session_state.images_folder = DOWNLOADS_DIR
if "image_paths" not in st.session_state:
    st.session_state.image_paths = None
if "gif_file" not in st.session_state:
    st.session_state.gif_file = None
if "iframe_html" not in st.session_state:
    st.session_state.iframe_html = None
if "years" not in st.session_state:
    # NOTE: The range of years will be loaded automatically from the chart's metadata. We just define this range here to avoid typing issues.
    st.session_state.years = range(2000, 2022)

# Step 1: Input chart URL and get years.
st.header("Step 1: Chart URL and timeline")
chart_url = st.text_input("Enter grapher URL", "")

# Get slug from URL.
slug = get_chart_slug(chart_url=chart_url)

# Set images folder and output file.
st.session_state.images_folder = DOWNLOADS_DIR / slug
st.session_state.gif_file = DOWNLOADS_DIR / f"{slug}.gif"


if st.button("Get chart"):
    # Embed the iframe in the app.
    st.session_state.iframe_html = f"""
    <iframe src="{chart_url}" loading="lazy"
            style="width: 100%; height: 600px; border: 0px none;"
            allow="web-share; clipboard-write"></iframe>
    """
    st.session_state.years = get_years_in_chart(chart_url)
    st.session_state.show_image_settings = True

# Display iframe if it was fetched.
if st.session_state.iframe_html:
    st.markdown("Modify chart as you wish, click on share-> copy link, and paste it in the box above.")
    st.components.v1.html(st.session_state.iframe_html, height=600)  # type: ignore

if st.session_state.get("show_image_settings"):
    st.header("Step 2: Image generation settings")
    # Slider for year range.
    year_min, year_max = st.slider(
        "Select year range",
        min_value=min(st.session_state.years),
        max_value=max(st.session_state.years),
        value=(min(st.session_state.years), max(st.session_state.years)),
    )

    # Tab and year range settings.
    tab = st.radio("Select tab", ["map", "chart"])
    year_range_open = st.checkbox(
        "Year range open",
        value=True,
        help="Only relevant for the chart view. If checked, the year range will be open. Uncheck if you want to generate a sequence of bar charts.",
    )

    # Button to generate images.
    # TODO: It's convenient to keep images in the folder, to avoid downloading them again.
    #  However, we should detect if the images are already there and have a button to delete them (in case the settings for image generation change).
    if st.button("Get images"):
        years = range(year_min, year_max + 1)
        st.session_state.image_paths = get_images_from_chart_url(
            chart_url=chart_url,
            png_folder=st.session_state.images_folder,
            tab=tab,
            years=years,
            year_range_open=year_range_open,
            max_workers=None,
            max_num_years=100,
        )
        st.session_state.show_gif_settings = True

# Step 3: GIF Settings and preview.
if st.session_state.get("show_gif_settings"):
    st.header("Step 3: GIF settings and preview")
    col1, col2 = st.columns([1, 1])

    # GIF settings.
    output_type = st.radio("Output Type", ["GIF", "Video"])
    if output_type == "Video":
        st.error("Video output is not yet supported. Please select GIF.")
    st.session_state.gif_file = st.session_state.gif_file.with_suffix(".gif" if output_type == "GIF" else ".mp4")
    remove_duplicates = st.checkbox("Remove Duplicate Frames?", value=True)
    repetitions_last_frame = st.number_input("Repetitions of Last Frame", value=0, step=1)
    duration = st.number_input("Duration (ms)", value=200, step=10)
    duration_of = st.radio("Duration of", ["Each frame", "Entire GIF"])
    loop_count = st.number_input("Number of Loops (0 = Infinite)", value=0, step=1)

    st.session_state.gif_file = Path(st.text_input("Output GIF", value=str(st.session_state.gif_file)))

    # Regenerate GIF button.
    if st.button("Generate GIF/Video"):
        st.session_state.gif_file = create_gif_from_images(
            image_paths=st.session_state.image_paths,
            output_gif=st.session_state.gif_file,
            duration=duration,
            loops=loop_count,
            remove_duplicate_frames=remove_duplicates,
            repetitions_last_frame=repetitions_last_frame,
            duration_of_full_gif=duration_of == "Entire GIF",
        )

    # GIF preview.
    if st.session_state.gif_file.exists():
        st.image(str(st.session_state.gif_file), caption="Preview of the Generated GIF", use_container_width=True)
    else:
        st.info("Generate a GIF to preview it.")

    if ENV in ("production", "staging"):
        # Add a download button if wizard is run remotely.
        if st.session_state.gif_file:
            with open(st.session_state.gif_file, "rb") as f:
                st.download_button(
                    label=f"Download {output_type}",
                    data=f.read(),
                    file_name=f"{str(st.session_state.gif_file.stem)}.{output_type.lower()}",
                    mime="image/gif" if output_type == "GIF" else "video/mp4",
                )
