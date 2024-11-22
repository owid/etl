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

log = get_logger()


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


def create_gif_from_chart_url(
    chart_url,
    output_gif=None,
    png_folder=None,
    tab=None,
    years=None,
    year_range_open=True,
    duration_frame=200,
    duration_loop=None,
    loops=0,
    max_workers=None,
    remove_duplicate_frames=True,
    max_num_years=100,
):
    # Given a chart URL, create a GIF with the chart data.

    # Get chart slug.
    slug = urlparse(chart_url).path.split("/")[-1]

    # Define default downloads folder (to use if either output_gif is None or png_folder is None).
    download_folder = Path.home() / "Downloads"

    # Define output file for the GIF.
    if output_gif is None:
        output_gif = download_folder / f"{slug}.gif"

    # Determine the default directory for PNGs.
    if png_folder is None:
        png_folder = download_folder / slug
        png_folder.mkdir(parents=True, exist_ok=True)
        log.info(f"Using Downloads folder for PNGs: {png_folder}")
    else:
        Path(png_folder).mkdir(parents=True, exist_ok=True)

    # If the tab parameter is not provided, extract it from the chart URL.
    if tab is None:
        # Extract query parameters
        tab = parse_qs(urlparse(chart_url).query).get("tab", [None])[0]
        if tab is None:
            # Default to "map" if the tab parameter is not found.
            tab = "map"

    # By default, assume an open year range (which is the most likely desired output).
    # # Infer year_range_open is not provided, infer it based on URL.
    # if year_range_open is None:
    #     _year_range_open = parse_qs(urlparse(chart_url).query).get("time", [None])[0]
    #     year_range_open = ".." in _year_range_open if _year_range_open else False

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

    # Decide configuration for GIF creation.
    if duration_loop is not None:
        if duration_frame is not None:
            raise ValueError("Cannot specify both duration_frame and duration_loop.")
        duration = duration_loop // len(years)
        if duration == 0:
            log.warning("Duration per frame is less than 1ms. Set a longer duration.")
    else:
        duration = duration_frame

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

    # Create GIF from images.
    if image_paths:
        log.info("Creating GIF...")
        images = [Image.open(img) for img in sorted(image_paths)]
        if remove_duplicate_frames:
            # Sometimes, even though the list of years is correct for all countries, for the specifically selected ones there may not be any data.
            # In this case, the PNGs will be the same, so we can remove duplicates.
            images = [images[i] for i in range(len(images)) if i == 0 or images[i] != images[i - 1]]
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
    else:
        log.error("Could not create GIF because there are no images downloaded.")
        return None


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
    "--duration-frame",
    type=int,
    default=200,
    help="Duration of each frame in milliseconds.",
)
@click.option(
    "--duration-loop",
    type=int,
    default=None,
    help="Duration of the entire GIF in milliseconds. NOTE: Cannot specify both duration_frame and duration_loop.",
)
@click.option(
    "--loops",
    type=int,
    default=0,
    help="Number of times the GIF should loop. 0 = infinite looping.",
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
    default=100,
    help="Maximum number of years to download. If the number of years in the chart exceeds this value, the script will stop.",
)
@click.option(
    "--remove-duplicate-frames",
    is_flag=True,
    help="Remove duplicate frames from the GIF.",
)
def main(
    chart_url,
    output_gif,
    tab,
    years,
    year_range_open,
    duration_frame,
    duration_loop,
    loops,
    max_workers,
    png_folder,
    max_num_years,
    remove_duplicate_frames,
):
    # Parse years.
    if years is not None:
        years = [int(year) for year in years.split(",")]

    create_gif_from_chart_url(
        chart_url=chart_url,
        output_gif=output_gif,
        tab=tab,
        years=years,
        year_range_open=year_range_open,
        duration_frame=duration_frame,
        duration_loop=duration_loop,
        loops=loops,
        png_folder=png_folder,
        max_workers=max_workers,
        max_num_years=max_num_years,
        remove_duplicate_frames=remove_duplicate_frames,
    )


if __name__ == "__main__":
    main()
