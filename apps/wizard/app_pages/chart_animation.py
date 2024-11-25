import streamlit as st
from structlog import get_logger

from apps.chart_animation.cli import (
    DOWNLOADS_DIR,
    create_gif_from_images,
    get_chart_slug,
    get_images_from_chart_url,
    get_years_in_chart,
)
from apps.wizard.utils.components import grapher_chart_from_url

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Chart animation",
    page_icon="🪄",
)

########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/animated_images: Chart animation")

# Initialize session state for generated files.
st.session_state.chart_animation_images_folder = st.session_state.get("chart_animation_images_folder", DOWNLOADS_DIR)
st.session_state.chart_animation_image_paths = st.session_state.get("chart_animation_image_paths", None)
st.session_state.chart_animation_images_exist = st.session_state.get("chart_animation_images_exist", False)
st.session_state.chart_animation_gif_file = st.session_state.get("chart_animation_gif_file", None)
st.session_state.chart_animation_iframe_html = st.session_state.get("chart_animation_iframe_html", None)
st.session_state.chart_animation_show_image_settings = st.session_state.get(
    "chart_animation_show_image_settings", False
)
st.session_state.chart_animation_show_gif_settings = st.session_state.get("chart_animation_show_gif_settings", False)
# NOTE: The range of years will be loaded automatically from the chart's metadata. We just define this range here to avoid typing issues.
st.session_state.chart_animation_years = st.session_state.get("chart_animation_years", range(2000, 2022))

# Step 1: Input chart URL and get years.
st.markdown("### Step 1: Chart URL and timeline")
chart_url = st.text_input(
    "Enter grapher URL",
    "",
    placeholder="https://ourworldindata.org/grapher/share-electricity-low-carbon?tab=chart&country=OWID_WRL~OWID_EUR~OWID_AFR",
)

# Get slug from URL.
slug = get_chart_slug(chart_url=chart_url)

# Set images folder and output file.
st.session_state.chart_animation_images_folder = DOWNLOADS_DIR / slug
st.session_state.chart_animation_gif_file = DOWNLOADS_DIR / f"{slug}.gif"
st.session_state.chart_animation_images_exist = (
    len([image for image in st.session_state.chart_animation_images_folder.iterdir() if image.suffix == ".png"]) > 0
)

if st.button("Get chart"):
    if not chart_url:
        st.error("Please enter a valid chart URL.")
        st.stop()
    # Embed the iframe in the app.
    st.session_state.chart_animation_years = get_years_in_chart(chart_url)
    st.session_state.chart_animation_show_image_settings = True

# Display iframe if it was fetched.
if st.session_state.chart_animation_show_image_settings:
    st.info("Modify chart as you wish, click on share -> copy link, and paste it in the box above.")
    st.session_state.chart_animation_iframe_html = grapher_chart_from_url(chart_url)

    st.markdown("### Step 2: Image generation settings")
    # Slider for year range.
    year_min, year_max = st.slider(
        "Select year range",
        min_value=min(st.session_state.chart_animation_years),
        max_value=max(st.session_state.chart_animation_years),
        value=(min(st.session_state.chart_animation_years), max(st.session_state.chart_animation_years)),
    )

    # Tab and year range settings.
    tab = st.radio("Select tab", ["map", "chart"])
    year_range_open = st.checkbox(
        "Year range open",
        value=True,
        help="Only relevant for the chart view. If checked, the year range will be open. Uncheck if you want to generate a sequence of bar charts.",
    )

    # Button to generate images.
    if st.session_state.chart_animation_images_exist:
        st.warning(
            f"Images already exist in the folder: {st.session_state.chart_animation_images_folder}\nYears for which there is already a file will be skipped.\nEither delete them all or continue to get images for remaining years (if any is missing)."
        )
        # Create a button to delete the folder.
        if st.button("Delete images"):
            for image in [
                image for image in st.session_state.chart_animation_images_folder.iterdir() if image.suffix == ".png"
            ]:
                image.unlink()
            # Update session state to reflect that images are deleted.
            st.session_state.chart_animation_images_exist = False
            st.info("Images deleted.")

    if st.button("Get images"):
        years = range(year_min, year_max + 1)
        st.session_state.chart_animation_image_paths = get_images_from_chart_url(
            chart_url=chart_url,
            png_folder=st.session_state.chart_animation_images_folder,
            tab=tab,
            years=years,
            year_range_open=year_range_open,
            max_workers=None,
            max_num_years=100,
        )
        st.session_state.chart_animation_show_gif_settings = True

# Step 3: GIF Settings and preview.
if st.session_state.chart_animation_show_gif_settings:
    st.markdown("### Step 3: GIF settings and preview")
    col1, col2 = st.columns([1, 1])

    # GIF settings.
    output_type = st.radio("Output Type", ["GIF", "Video"])
    if output_type == "Video":
        st.error("Video output is not yet supported. Please select GIF.")
    st.session_state.chart_animation_gif_file = st.session_state.chart_animation_gif_file.with_suffix(
        ".gif" if output_type == "GIF" else ".mp4"
    )
    remove_duplicates = st.toggle("Remove duplicate frames", value=True)
    repetitions_last_frame = st.number_input("Repetitions of Last Frame", value=0, step=1)
    duration = st.number_input("Duration (ms)", value=200, step=10)
    duration_of = st.radio("Duration of", ["Each frame", "Entire animation"])
    loop_count = st.number_input("Number of Loops (0 = Infinite)", value=0, step=1)

    # Regenerate GIF button.
    if st.button("Generate animation"):
        st.session_state.chart_animation_gif_file = create_gif_from_images(
            image_paths=st.session_state.chart_animation_image_paths,
            output_gif=st.session_state.chart_animation_gif_file,
            duration=duration,
            loops=loop_count,
            remove_duplicate_frames=remove_duplicates,
            repetitions_last_frame=repetitions_last_frame,
            duration_of_full_gif=duration_of == "Entire animation",
        )

    # GIF preview.
    if st.session_state.chart_animation_gif_file.exists():
        st.info('Animation preview. Right click and "Save Image As..." to download it.')
        st.image(str(st.session_state.chart_animation_gif_file), use_container_width=True)
