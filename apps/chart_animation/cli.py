"""Create a GIF for a given chart URL.

"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import click
import requests
from PIL import Image
from rich_click.rich_command import RichCommand
from structlog import get_logger
from tqdm.auto import tqdm

# Initialize log.
log = get_logger()

# Define default downloads folder (to use if either output_gif is None or png_folder is None).
DOWNLOADS_DIR = Path.home() / ".chart_animation"

# Default maximum number of years to fetch images for.
MAX_NUM_YEARS = 100


def get_chart_metadata(chart_url):
    # Given a chart URL, get the chart metadata.
    base_url = urlunparse(urlparse(chart_url)._replace(query=""))
    chart_metadata_url = str(base_url).rstrip("/") + ".metadata.json"
    log.info(f"Fetching metadata from: {chart_metadata_url}")
    response = requests.get(chart_metadata_url)
    response.raise_for_status()
    chart_metadata = response.json()

    return chart_metadata


def get_indicator_metadata(indicator_metadata_url):
    # Given an indicator metadata URL, get the indicator metadata.
    response = requests.get(indicator_metadata_url)
    response.raise_for_status()
    return response.json()


def get_indicators_metadata_from_chart_metadata(chart_metadata, max_workers=None):
    # Given a chart metadata, get the metadata for all the indicators in the chart.

    # Get indicator API URLs.
    indicator_metadata_urls = [
        column["fullMetadata"]
        for column in chart_metadata["columns"].values()
        if "undefined" not in column["fullMetadata"]
    ]

    # Get metadata for each of the indicators in the chart.
    indicators_metadata = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks for each URL
        future_to_url = {executor.submit(get_indicator_metadata, url): url for url in indicator_metadata_urls}
        for future in as_completed(future_to_url):
            try:
                indicators_metadata.append(future.result())
            except Exception as e:
                print(f"Error fetching metadata from {future_to_url[future]}: {e}")

    return indicators_metadata


def get_years_in_chart(chart_url):
    # Given a chart URL, get the years available in the chart.
    chart_metadata = get_chart_metadata(chart_url)
    indicators_metadata = get_indicators_metadata_from_chart_metadata(chart_metadata)
    years = sorted(
        set(
            [
                year["id"]
                for year in sum(
                    [
                        values["values"]
                        for indicator in indicators_metadata
                        for dimension, values in indicator["dimensions"].items()
                        if dimension in ["years"]
                    ],
                    [],
                )
            ]
        )
    )
    return years


def modify_chart_url(chart_url, year, year_range_open, tab):
    # Take a chart URL, modify its parameters, and create a new URL for the PNG download.
    parsed_url = urlparse(chart_url)
    path = parsed_url.path
    if not path.endswith(".png"):
        path += ".png"

    query_params = parse_qs(parsed_url.query)
    if year_range_open:
        query_params["time"] = [f"earliest..{year}"]
    else:
        query_params["time"] = [str(year)]
    query_params["tab"] = [tab]
    query_params["download"] = ["png"]
    updated_query = urlencode(query_params, doseq=True)
    png_url = urlunparse(parsed_url._replace(path=path, query=updated_query))
    return png_url


def download_chart_png(png_url, output_file):
    # Download a PNG file from a given URL.
    output_file = Path(output_file)

    # Skip download if the file already exists.
    if output_file.exists():
        log.info(f"File {output_file} already exists. Skipping download.")
        return output_file

    # Ensure the directory exists.
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Download PNG.
    try:
        response = requests.get(png_url, stream=True)
        response.raise_for_status()
        with open(output_file, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return output_file
    except Exception as e:
        log.error(f"Failed to create file {output_file}: {e}")
        return None


def get_chart_slug(chart_url):
    # Given a chart URL, get the chart slug.
    return urlparse(chart_url).path.split("/")[-1]


def get_images_from_chart_url(
    chart_url,
    png_folder,
    tab=None,
    years=None,
    year_range_open=True,
    max_workers=None,
    max_num_years=MAX_NUM_YEARS,
):
    # Given a chart URL, download the PNGs into a folder. If they already exists, skip them.

    # If the tab parameter is not provided, extract it from the chart URL.
    if tab is None:
        # Extract query parameters
        tab = parse_qs(urlparse(chart_url).query).get("tab", [None])[0]
        if tab is None:
            # Default to "map" if the tab parameter is not found.
            tab = "map"

    if years is None:
        years = get_years_in_chart(chart_url=chart_url)

        if not years:
            log.error("No years available.")
            return None

        if year_range_open:
            if len(years) < 2:
                log.error("Cannot generate year ranges with less than two years.")
                return None
            years = years[1:]

    if max_num_years is not None and len(years) > max_num_years:
        log.error(
            f"Number of years ({len(years)}) exceeds the maximum number of years ({max_num_years}). Consider setting years explicitly or increasing max_num_years. Years available: {years}"
        )
        return None

    # Download PNGs in parallel.
    log.info("Downloading images in parallel.")
    image_paths = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                download_chart_png,
                modify_chart_url(chart_url, year, year_range_open, tab),
                Path(png_folder) / f"{year}.png",
            ): year
            for year in years
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading PNGs"):
            try:
                image_path = future.result()
                if image_path:
                    image_paths.append(image_path)
            except Exception as e:
                log.error(f"Error downloading image: {e}")

    return image_paths


def create_gif_from_images(
    image_paths,
    output_gif,
    duration=200,
    loops=0,
    remove_duplicate_frames=True,
    repetitions_last_frame=0,
    duration_of_full_gif=False,
):
    images = [Image.open(img) for img in sorted(image_paths)]
    if remove_duplicate_frames:
        # Sometimes, even though the list of years is correct for all countries, for the specifically selected ones there may not be any data.
        # In this case, the PNGs will be the same, so we can remove duplicates.
        images = [images[i] for i in range(len(images)) if i == 0 or images[i] != images[i - 1]]

    # Optionally repeat the last frame.
    images = images + [images[-1]] * repetitions_last_frame

    if duration_of_full_gif:
        duration = duration // len(images)

    # There seems to be a PIL bug when specifying loops.
    if loops == 1:
        # Repeat loop only once.
        images[0].save(output_gif, save_all=True, append_images=images[1:], optimize=True, duration=duration)
    elif loops == 0:
        # Infinite loop.
        images[0].save(
            output_gif, save_all=True, append_images=images[1:], optimize=True, duration=duration, loop=loops
        )
    else:
        # Repeat loop a fixed number of times.
        images[0].save(
            output_gif, save_all=True, append_images=images[1:], optimize=True, duration=duration, loop=loops - 1
        )
    log.info(f"GIF successfully created at {output_gif}")
    return output_gif


@click.command(name="chart_to_gif", cls=RichCommand, help=__doc__)
@click.argument("chart_url", type=str)
@click.option(
    "--output-gif",
    type=str,
    default=None,
    help="Output GIF file path. If None, uses Downloads folder.",
)
@click.option(
    "--tab",
    type=click.Choice(["map", "chart"]),
    default=None,
    help="Chart tab view (either map or chart). If not specified, it is inferred from URL, and otherwise defaults to map.",
)
@click.option(
    "--years",
    type=str,
    default=None,
    help="Comma-separated list of years to plot. If None, uses all years in the chart. To avoid many queries, a parameter --max-num-years is defined.",
)
@click.option(
    "--year-range-open/--year-range-closed",
    default=True,
    help="Whether the year range is open or closed. If open, the range is from earliest to the year. If closed, the range is only the year.",
)
@click.option(
    "--duration",
    type=int,
    default=200,
    help="Duration in milliseconds (of each frame, or of the entire GIF).",
)
@click.option(
    "--loops",
    type=int,
    default=0,
    help="Number of times the GIF should loop. 0 = infinite looping.",
)
@click.option(
    "--repetitions-last-frame",
    type=int,
    default=0,
    help="Number of repetitions of the last frame.",
)
@click.option(
    "--max-workers",
    type=int,
    default=None,
    help="Maximum number of parallel threads. If None, uses the number of CPUs available.",
)
@click.option(
    "--png-folder",
    type=str,
    default=None,
    help="Directory to save downloaded PNG images. If None, use Downloads folder.",
)
@click.option(
    "--max-num-years",
    type=int,
    default=MAX_NUM_YEARS,
    help="Maximum number of years to download. If the number of years in the chart exceeds this value, the script will stop.",
)
@click.option(
    "--duration-of-full-gif/--duration-of-each-frame",
    default=False,
    help="Whether the duration is for each frame or the entire GIF.",
    is_flag=True,
)
@click.option(
    "--remove-duplicate-frames",
    is_flag=True,
    help="Remove duplicate frames from the GIF.",
)
def cli(
    chart_url,
    output_gif,
    tab,
    years,
    year_range_open,
    duration,
    loops,
    repetitions_last_frame,
    max_workers,
    png_folder,
    max_num_years,
    duration_of_full_gif,
    remove_duplicate_frames,
):
    # Given a chart URL, create a GIF with the chart data.

    # Parse years.
    if years is not None:
        years = [int(year) for year in years.split(",")]

    # Get chart slug.
    slug = get_chart_slug(chart_url)

    # Determine the default directory for PNGs.
    if png_folder is None:
        png_folder = DOWNLOADS_DIR / slug
        png_folder.mkdir(parents=True, exist_ok=True)
        log.info(f"Using Downloads folder for PNGs: {png_folder}")
    else:
        Path(png_folder).mkdir(parents=True, exist_ok=True)

    # Define output file for the GIF.
    if output_gif is None:
        output_gif = DOWNLOADS_DIR / f"{slug}.gif"

    # Get images from chart URL.
    image_paths = get_images_from_chart_url(
        chart_url=chart_url,
        png_folder=png_folder,
        tab=tab,
        years=years,
        year_range_open=year_range_open,
        max_workers=max_workers,
        max_num_years=max_num_years,
    )

    # Create GIF from images.
    if image_paths:
        log.info("Creating GIF...")
        return create_gif_from_images(
            image_paths=image_paths,
            output_gif=output_gif,
            duration=duration,
            loops=loops,
            remove_duplicate_frames=remove_duplicate_frames,
            repetitions_last_frame=repetitions_last_frame,
            duration_of_full_gif=duration_of_full_gif,
        )
    else:
        log.error("Could not create GIF because there are no images downloaded.")
        return None


if __name__ == "__main__":
    cli()
