import streamlit as st
from structlog import get_logger

from apps.chart_animation.cli import (
    DOWNLOADS_DIR,
    MAX_NUM_YEARS,
    create_gif_from_images,
    create_image_file_name,
    create_mp4_from_images,
    get_chart_slug,
    get_images_from_chart_url,
    get_years_in_chart,
)
from apps.wizard.utils.components import grapher_chart_from_url, st_horizontal, st_info

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
# NOTE: The range of years will be loaded automatically from the chart's metadata. We just define this range here to avoid typing issues.
st.session_state.chart_animation_years = st.session_state.get("chart_animation_years", range(2000, 2022))
st.session_state.chart_animation_max_num_years = MAX_NUM_YEARS

# Step 1: Input chart URL and get years.
chart_url = st.text_input(
    "Enter grapher URL",
    "",
    placeholder="https://ourworldindata.org/grapher/share-electricity-low-carbon?tab=chart&country=OWID_WRL~OWID_EUR~OWID_AFR",
)

# Get slug from URL.
slug = get_chart_slug(chart_url=chart_url)

# Set images folder and output file.
st.session_state.chart_animation_images_folder = DOWNLOADS_DIR / slug
if not st.session_state.chart_animation_images_folder.exists():
    # Create the default output folder if it doesn't exist.
    st.session_state.chart_animation_images_folder.mkdir(parents=True)
st.session_state.chart_animation_gif_file = DOWNLOADS_DIR / f"{slug}.gif"
st.session_state.chart_animation_images_exist = (
    len([image for image in st.session_state.chart_animation_images_folder.iterdir() if image.suffix == ".png"]) > 0
)
st.session_state.chart_animation_image_paths = [
    image for image in st.session_state.chart_animation_images_folder.iterdir() if image.suffix == ".png"
]

if st.button("Get chart"):
    if not chart_url:
        st.error("Please enter a valid chart URL.")
        st.stop()
    # Embed the iframe in the app.
    st.session_state.chart_animation_years = get_years_in_chart(chart_url)
    st.session_state.chart_animation_show_image_settings = True

# Display iframe if it was fetched.
if st.session_state.chart_animation_show_image_settings:
    with st.container(border=True):
        st.session_state.chart_animation_iframe_html = grapher_chart_from_url(chart_url)
        st_info(
            "Modify chart as you wish, click on share -> copy link, and paste it in the box above.",
        )

    # SHOW OPTIONS FOR CHART
    with st.container(border=True):
        # Create a slider to select min and max years.
        year_min, year_max = st.select_slider(
            "Select year range",
            options=st.session_state.chart_animation_years,
            value=(min(st.session_state.chart_animation_years), max(st.session_state.chart_animation_years)),
        )

        # Get the selected subset of years.
        years = [year for year in st.session_state.chart_animation_years if year_min <= year <= year_max]

        st.session_state.chart_animation_max_num_years = st.number_input(
            "Maximum number of years",
            value=MAX_NUM_YEARS,
            help="Maximum number of years to generate images for (to avoid too many API call).",
        )

    if len(years) > st.session_state.chart_animation_max_num_years:
        st.error(
            f"Number of years in the chart ({len(years)}) is higher than the maximum number of years ({st.session_state.chart_animation_max_num_years}). You can either increase the maximum number of years or select a smaller range."
        )
        st.stop()

    # SHOW OPTIONS FOR IMAGE GENERATION
    with st.container(border=True):
        # Tab and year range settings.
        def add_icons_to_tabs(tab_name):
            if tab_name == "map":
                return ":material/map: map"
            elif tab_name == "chart":
                return ":material/show_chart: chart"
            return f":material/{tab_name}: {tab_name}"

        # tab = st.radio("Select tab", ["map", "chart"], horizontal=True)
        tab = st.segmented_control("Select tab", ["map", "chart"], format_func=add_icons_to_tabs, default="map")

        if tab == "chart":
            year_range_open = st.toggle(
                "Year range open",
                value=True,
                help="Only relevant for the chart view. If checked, the year range will be open. Uncheck if you want to generate a sequence of bar charts.",
            )
        else:
            year_range_open = True

        # GIF settings.
        # output_type = st.radio("Output Type", ["GIF", "Video"], horizontal=True)
        output_type = st.segmented_control("Output Type", ["GIF", "Video"], default="GIF")
        st.session_state.chart_animation_gif_file = st.session_state.chart_animation_gif_file.with_suffix(
            ".gif" if output_type == "GIF" else ".mp4"
        )
        remove_duplicates = st.toggle("Remove duplicate frames", value=True)
        with st_horizontal():
            repetitions_last_frame = st.number_input("Repetitions of Last Frame", value=0, step=1)
            if output_type == "GIF":
                loop_count = st.number_input("Number of Loops (0 = Infinite)", value=0, step=1)
            duration = st.number_input("Duration (ms)", value=200, step=10)
        duration_of = st.radio("Duration of", ["Each frame", "Entire animation"], horizontal=True)

    # Fetch all needed images (skipping the ones that already exist).
    st.session_state.chart_animation_image_paths = get_images_from_chart_url(
        chart_url=chart_url,
        png_folder=st.session_state.chart_animation_images_folder,
        tab=tab,
        years=years,
        year_range_open=year_range_open,
        max_workers=None,
        max_num_years=st.session_state.chart_animation_max_num_years,
    )
    st.session_state.chart_animation_images_exist = len(st.session_state.chart_animation_image_paths) > 0  # type: ignore

    # Select only images that match the required parameters.
    image_paths_selected = [
        st.session_state.chart_animation_images_folder
        / create_image_file_name(year=year, year_range_open=year_range_open, tab=tab)
        for year in years
    ]

    # GIF/Video generation.
    if output_type == "GIF":
        st.session_state.chart_animation_gif_file = create_gif_from_images(
            image_paths=image_paths_selected,
            output_file=st.session_state.chart_animation_gif_file,
            duration=duration,
            loops=loop_count,  # type: ignore
            remove_duplicate_frames=remove_duplicates,
            repetitions_last_frame=repetitions_last_frame,
            duration_of_animation=duration_of == "Entire animation",
        )
        # GIF preview.
        st_info('Animation preview. Right click and "Save Image As..." to download it.')
        st.image(str(st.session_state.chart_animation_gif_file), use_container_width=True)
    else:
        st.session_state.chart_animation_gif_file = create_mp4_from_images(
            image_paths=image_paths_selected,
            output_file=st.session_state.chart_animation_gif_file,
            duration=duration,
            remove_duplicate_frames=remove_duplicates,
            repetitions_last_frame=repetitions_last_frame,
            duration_of_animation=duration_of == "Entire animation",
        )
        # Video preview
        st_info('Animation preview. Right click and "Save video as..." to download it.')
        with open(str(st.session_state.chart_animation_gif_file), "rb") as video_file:
            st.video(video_file.read(), format="video/mp4", autoplay=True)

    # Button to delete all images in the folder.
    if st.button(
        "Delete images",
        disabled=not st.session_state.chart_animation_images_exist,
        help=f"Delete images in folder: {st.session_state.chart_animation_images_folder}.",
    ):
        for image in st.session_state.chart_animation_image_paths:  # type: ignore
            image.unlink()
        # Update session state to reflect that images are deleted.
        st.session_state.chart_animation_images_exist = False
        st.toast("✅ Images deleted.")
