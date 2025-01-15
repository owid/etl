from urllib.parse import parse_qs, urlparse

import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

import etl.grapher.model as gm
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
from apps.wizard.utils import set_states
from apps.wizard.utils.components import grapher_chart_from_url, st_horizontal, st_info, url_persist
from etl.config import OWID_ENV, OWIDEnv

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Chart animation",
    page_icon="🪄",
)

# Global variables
TABS_ACCEPTED = ["chart", "map"]
TAB_DEFAULT = "map"

# Session state config
# Initialize session state for generated files.
st.session_state.chart_animation_images_folder = st.session_state.get("chart_animation_images_folder", DOWNLOADS_DIR)
st.session_state.chart_animation_image_paths = st.session_state.get("chart_animation_image_paths", None)
st.session_state.chart_animation_images_exist = st.session_state.get("chart_animation_images_exist", False)
st.session_state.chart_animation_file = st.session_state.get("chart_animation_file", None)
st.session_state.chart_animation_iframe_html = st.session_state.get("chart_animation_iframe_html", None)
st.session_state.chart_animation_show_image_settings = st.session_state.get(
    "chart_animation_show_image_settings", False
)
# NOTE: The range of years will be loaded automatically from the chart's metadata. We just define this range here to avoid typing issues.
st.session_state.chart_animation_years = st.session_state.get("chart_animation_years", range(2000, 2022))
st.session_state.chart_animation_years_selected = st.session_state.get(
    "chart_animation_years_selected", st.session_state.chart_animation_years
)
st.session_state.chart_animation_max_num_years = MAX_NUM_YEARS


# FUNCTIONS
def add_icons_to_tabs(tab_name):
    if tab_name == "map":
        return ":material/map: map"
    elif tab_name == "chart":
        return ":material/show_chart: chart"
    return f":material/{tab_name}: {tab_name}"


def get_query_parameters_in_chart(chart_url, slug, all_years):
    # Select default values.
    year_range_open = True
    year_start, year_end = min(all_years), max(all_years)
    tab = None

    # Attempt to get those parameters from the chart URL.
    url_parsed = urlparse(chart_url)
    query_params = parse_qs(url_parsed.query)
    if "time" in query_params:
        time = query_params["time"][0]
        if ".." in time:
            year_range_open = True
            year_start, year_end = time.split("..")
            if year_start == "earliest":
                year_start = min(all_years)
            if year_end == "latest":
                year_end = max(all_years)
        else:
            year_range_open = False
            year_start = int(time)
            year_end = year_start
    if "tab" in query_params:
        tab = query_params["tab"][0]

    # EXTRA params:
    chart_details = get_chart_details(slug=slug)

    tab = tab or chart_details["tab"] or TAB_DEFAULT

    if tab not in TABS_ACCEPTED:
        st.error(f"Tab '{tab}' is not supported. Please select one of {TABS_ACCEPTED}.")

    params = {
        "year_range_open": year_range_open,
        "year_min": int(year_start),
        "year_max": int(year_end),
        "tab": tab,
        "chart_type": chart_details["type"],
    }

    return params


def remove_images():
    """Remove all images in the local folder."""
    for image in st.session_state.chart_animation_image_paths:  # type: ignore
        image.unlink()
    # Update session state to reflect that images are deleted.
    st.session_state.chart_animation_images_exist = False
    st.toast("✅ Images deleted.")

    # Reset button!
    st.session_state.chart_animation_show_image_settings = False

    # Stop skipping button via query parameter
    if "animation_skip_button" in st.query_params:
        st.query_params.pop("animation_skip_button")


def get_chart_details(slug):
    """Get chart type from PROD database."""
    # Get environment. If running in prod, just use LIVE db, otherwise use staging-site-master
    if OWID_ENV.env_local == "production":
        env = OWID_ENV
    else:
        env = OWIDEnv.from_staging("master")

    with Session(env.engine) as session:
        chart = gm.Chart.load_chart(session, slug=slug)

    details = {
        "type": chart.chart_config.chartType,
        "tab": chart.chart_config.full.get("tab"),
    }
    return details


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/animated_images: Chart animation")

# 1/ INPUT CHART & GET YEARS
chart_url = url_persist(st.text_input)(
    label="Enter grapher URL",
    value="",
    placeholder="https://ourworldindata.org/grapher/share-electricity-low-carbon?tab=chart&country=OWID_WRL~OWID_EUR~OWID_AFR",
    help="""Paste the URL of the chart you want to animate.

**Considerations**:
- Only production links work.
- You can pass the full URL, or skip the https://ourworldindata.org/ bit.
- Some parameters cannot be extracted from the URL (e.g. the type of tab view). But you can modify them afterwards.
    - If coming from Admin, tab view will be extracted correctly!
""",
    key="animation_chart_url",
)
# Add base URL if not present
if chart_url.startswith("/grapher"):
    chart_url = f"https://ourworldindata.org{chart_url}"
elif chart_url.startswith("grapher/"):
    chart_url = f"https://ourworldindata.org/{chart_url}"

# Get slug from URL.
slug = get_chart_slug(chart_url=chart_url)

# Set images folder and output file.
st.session_state.chart_animation_images_folder = DOWNLOADS_DIR / slug
if not st.session_state.chart_animation_images_folder.exists():
    # Create the default output folder if it doesn't exist.
    st.session_state.chart_animation_images_folder.mkdir(parents=True)
image_paths = [image for image in st.session_state.chart_animation_images_folder.iterdir() if image.suffix == ".png"]
st.session_state.chart_animation_file = DOWNLOADS_DIR / f"{slug}.gif"
st.session_state.chart_animation_image_paths = image_paths
st.session_state.chart_animation_images_exist = len(image_paths) > 0

# Check if we want to skip the button and generate the Chart directly
skip_button = st.query_params.get("animation_skip_button", False)

# Button
if (
    st.button(
        "Get chart",
        type="primary",
    )
    or skip_button
):
    if not chart_url:
        st.error("Please enter a valid chart URL.")
        st.stop()

    # Embed the iframe in the app.
    set_states(
        {
            "chart_animation_years": get_years_in_chart(chart_url),
            "chart_animation_show_image_settings": True,
        }
    )

# 2/ CONTINUE IF NO ERROR
if st.session_state.chart_animation_show_image_settings:
    # Display iframe if it was fetched.
    with st.expander("**Preview**", expanded=True):
        st.session_state.chart_animation_iframe_html = grapher_chart_from_url(chart_url)
        st_info(
            "Modify chart as you wish, click on share -> copy link, and paste it in the box above.",
        )

        # 2.1/ CONFIGURE INPUT: CHART EDIT
        with st.container(border=True):
            st.caption("**Chart settings**")
            # Configure the chart (input to animation generation).
            query_parameters = get_query_parameters_in_chart(
                chart_url, slug, all_years=st.session_state.chart_animation_years
            )

            with st_horizontal():
                tab = st.segmented_control(
                    "Select tab", ["map", "chart"], format_func=add_icons_to_tabs, default=query_parameters["tab"]
                )
                st.session_state.chart_animation_max_num_years = st.number_input(
                    "Maximum number of years",
                    value=MAX_NUM_YEARS,
                    help="Maximum number of years to generate images for (to avoid too many API call).",
                )

            # Create a slider to select min and max years.
            year_min, year_max = st.select_slider(
                "Select year range",
                options=st.session_state.chart_animation_years,
                value=(query_parameters["year_min"], query_parameters["year_max"]),
            )

        # Get the selected subset of years.
        years = [year for year in st.session_state.chart_animation_years if year_min <= year <= year_max]

    if len(years) > st.session_state.chart_animation_max_num_years:
        st.error(
            f"Number of years in the chart ({len(years)}) is higher than the maximum number of years ({st.session_state.chart_animation_max_num_years}). You can either increase the maximum number of years or select a smaller range."
        )
        st.stop()

    # 2.2/ SHOW OPTIONS FOR IMAGE GENERATION
    with st.expander("**Output settings**", expanded=True):
        with st_horizontal():
            # Choose: GIF or Video
            output_type = st.segmented_control(
                "Output format",
                ["Video", "GIF"],
                default="Video",
            )
            # Social media?
            output_style = st.segmented_control(
                "Output style",
                ["Classic", "Square format"],
                default="Classic",
                help="Use 'square format' for mobile or social media.",
            )
            social_media_square = output_style == "Square format"

        st.session_state.chart_animation_file = st.session_state.chart_animation_file.with_suffix(
            ".gif" if output_type == "GIF" else ".mp4"
        )

        # If chart, show option to just show single year
        if tab == "chart":
            year_range_open = not st.toggle(
                "Show single year",
                value=not query_parameters["year_range_open"],
                help="Only relevant for the chart view. If checked, the animated chart will only display a single year per frame. For LineCharts, this means a sequence of bar charts. For ScatterCharts, this means a sequence of bubbles (and not vectors).",
            )
        else:
            year_range_open = True

        with st.container(border=True):
            st.caption("**Frame settings**")
            remove_duplicates = not st.toggle(
                "Allow duplicate frames",
                value=False,
                help="Some charts may have duplicate frames. If checked, these frames will be shown.",
            )

            with st_horizontal():
                duration = st.number_input(
                    "Duration (ms)",
                    value=200,
                    step=10,
                    help="Duration (in ms) of each frame, or the entire animation.",
                    # label_visibility="collapsed",
                )
                duration_of = st.segmented_control(
                    "Duration of",
                    ["Each frame", "Entire animation"],
                    help="Choose if the duration parameter refers to each frame, or the entire animation. Note that each frame cannot be shorter than 20ms.",
                    default="Each frame",
                    # label_visibility="collapsed",
                )
                repetitions_last_frame = st.number_input(
                    "Duration of the last frame (ms)",
                    min_value=duration,
                    value=duration,
                    step=duration,
                    help="Increase this to make the last frame last longer.",
                )
                repetitions_last_frame = repetitions_last_frame // duration - 1
                if output_type == "GIF":
                    loop_count = st.number_input("Number of Loops (0 = Infinite)", value=0, step=1)

    # Fetch all needed images (skipping the ones that already exist).
    st.session_state.chart_animation_image_paths = get_images_from_chart_url(
        chart_url=chart_url,
        png_folder=st.session_state.chart_animation_images_folder,
        tab=tab,
        social_media_square=social_media_square,
        years=years,
        year_range_open=year_range_open,
        max_workers=None,
        max_num_years=st.session_state.chart_animation_max_num_years,
    )
    st.session_state.chart_animation_images_exist = len(st.session_state.chart_animation_image_paths) > 0  # type: ignore

    # Select only images that match the required parameters.
    image_paths_selected = [
        st.session_state.chart_animation_images_folder
        / create_image_file_name(
            year=year,
            year_range_open=year_range_open,
            tab=tab,
            social_media_square=social_media_square,
        )
        for year in years
    ]

    # Skip first frame for LineCharts, bc it renders a bar chart!
    first_frame = 1 if (query_parameters["chart_type"] == "LineChart" and query_parameters["tab"] == "chart") else 0

    # GIF/Video generation.
    with st.spinner("Generating animation. This can take few seconds..."):
        if output_type == "GIF":
            st.session_state.chart_animation_file = create_gif_from_images(
                image_paths=image_paths_selected,
                output_file=st.session_state.chart_animation_file,
                duration=duration,
                loops=loop_count,  # type: ignore
                remove_duplicate_frames=remove_duplicates,
                repetitions_last_frame=repetitions_last_frame,
                duration_of_animation=duration_of == "Entire animation",
                first_frame=first_frame,
            )
            # GIF preview.
            st.image(str(st.session_state.chart_animation_file), use_container_width=True)
            st_info('Animation preview. Right click and "Save Image As..." to download it.')
        else:
            st.session_state.chart_animation_file = create_mp4_from_images(
                image_paths=image_paths_selected,
                output_file=st.session_state.chart_animation_file,
                duration=duration,
                remove_duplicate_frames=remove_duplicates,
                repetitions_last_frame=repetitions_last_frame,
                duration_of_animation=duration_of == "Entire animation",
                first_frame=first_frame,
            )
            # Video preview
            with open(str(st.session_state.chart_animation_file), "rb") as video_file:
                st.video(video_file.read(), format="video/mp4", autoplay=True)
            st_info('Animation preview. Right click and "Save video as..." to download it.')

    # Button to delete all images in the folder.
    st.button(
        "Delete images",
        disabled=not st.session_state.chart_animation_images_exist,
        help=f"To generate the animation, several chart images were downloaded and saved in in folder: `{st.session_state.chart_animation_images_folder}`. Click this button to delete them.",
        on_click=remove_images,
    )
