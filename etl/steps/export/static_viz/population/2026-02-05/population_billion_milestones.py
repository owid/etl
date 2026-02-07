"""Create visualization showing time for world population to increase by one billion.

This code generates a bar chart showing the decreasing time intervals between
population milestones from 1 billion to 10 billion.
"""

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import structlog
from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl import paths as etl_paths
from etl.helpers import create_dataset

# Configure matplotlib to use SVG text elements instead of paths
matplotlib.rcParams["svg.fonttype"] = "none"

# Paths for this export step
CURRENT_DIR = Path(__file__).parent
OUTPUT_DIR = etl_paths.EXPORT_DIR / "static_viz/2026-02-05/population_billion_milestones"
SHORT_NAME = "population_billion_milestones"

log = structlog.get_logger()

POPULATION_TARGETS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
POPULATION_TARGETS = [x * 1e9 for x in POPULATION_TARGETS]


def find_population_milestones(tb: Table) -> list[dict]:
    """Find years when population crossed each billion milestone.

    Args:
        tb: Table with 'year' and 'population' columns

    Returns:
        List of dicts with milestone, year_start, year_end, duration info
    """
    # Linearly interpolate population
    tb = interpolate_population(tb)

    # Round population values to significant (resolution of 1e8), e.g. 521,324,321 -> 5e8
    tb = round_population_values(tb)

    # Keep only population of interest values
    tb = tb.loc[tb["population_target"].isin(POPULATION_TARGETS)]
    # Keep one row for each population rounded. That's when the 'target' is reached.
    tb = get_target_years(tb)

    # Sort by population target to ensure correct ordering
    tb = tb.sort_values("population_target").reset_index(drop=True)

    # Build milestones list
    milestones = []

    for i in range(len(tb)):
        year_reached = int(tb.loc[i, "year"])
        target_pop = tb.loc[i, "population_target"]
        target_billion = int(target_pop / 1e9)

        # For the first billion, use earliest data year
        if i == 0:
            year_start = int(tb["year"].min())
        else:
            # Use the year when previous billion was reached
            year_start = int(tb.loc[i - 1, "year"])

        duration = year_reached - year_start

        # For the first billion, use a capped display duration (~140 years)
        # while keeping the actual year_start for the label
        if i == 0:
            display_duration = 145  # Visual cap for first bar
        else:
            display_duration = duration

        milestones.append(
            {
                "milestone": target_billion,
                "year_start": year_start,
                "year_end": year_reached,
                "duration": duration,
                "display_duration": display_duration,
            }
        )

    return milestones


def round_population_values(tb: Table) -> Table:
    """Round population values to significant figures."""
    msk = tb["population"] <= 1e9
    tb.loc[msk, "population_target"] = tb.loc[msk, "population"].round(-7)
    tb.loc[-msk, "population_target"] = tb.loc[-msk, "population"].round(-8)
    return tb


def interpolate_population(tb: Table) -> Table:
    """Interpolate population values for missing years.

    tb should be of Nx2 dimensions, 2 being the number of culumns: year, population.
    """
    # Create new index ([start_year, end_year])
    idx = pd.RangeIndex(tb.year.min(), tb.year.max() + 1, name="year")
    tb = tb.set_index(["year"]).reindex(idx)
    # Interpolate population values
    tb["population"] = tb["population"].interpolate(method="index")
    tb = tb.reset_index()
    return tb


def get_target_years(tb: Table) -> Table:
    """Get years of interest.

    Multiple population values are rounded to the same value. We want to keep only the row for the year when the population target was reached.
    How? We obtain rows where target population was reached (e.g target-crossing)
    """

    ## 1. Calculate the sign of the population error
    tb["population_error"] = tb["population"] - tb["population_target"]
    ## 2. Check if the sign of the population error changes (from negative to positive)
    ## Keep start and end year of target-crossing
    ## Tag target-crossing with a number (so that we know that the start- and end-years belong to the same target-crossing)
    tb["target_crossing"] = np.sign(tb["population_error"]).diff().fillna(0) > 0
    tb["target_crossing"] = np.where(tb["target_crossing"], tb["target_crossing"].cumsum(), 0)
    tb["target_crossing"] = tb["target_crossing"] + tb["target_crossing"].shift(-1).fillna(0)

    ## 2b. Sometimes there is no 'crossing' due to resolution (single year jump)
    # Find targets where all rows have target_crossing == 0
    targets_with_no_crossing = []
    for target in tb["population_target"].unique():
        target_rows = tb[tb["population_target"] == target]
        if (target_rows["target_crossing"] == 0).all():
            targets_with_no_crossing.append(target)

    # For these targets, assign a unique crossing ID to each row
    if targets_with_no_crossing:
        max_crossing_id = tb["target_crossing"].max() if tb["target_crossing"].max() > 0 else 0
        for target in targets_with_no_crossing:
            mask = tb["population_target"] == target
            tb.loc[mask, "target_crossing"] = max_crossing_id + 1
            max_crossing_id += 1

    tb = tb[tb["target_crossing"] > 0]

    ## 3. Keep start OR end year of target-crossing, based on which is closed to target
    tb["population_error_abs"] = tb["population_error"].abs()
    tb = tb.sort_values("population_error_abs").drop_duplicates(subset=["target_crossing"])

    ## 4. Keep relevant columns
    tb = tb.loc[:, ["year", "population", "population_target"]]

    return tb


def build_source_citation(tb_historical, tb_projection) -> str:
    """Build source citation text from table metadata.

    Args:
        tb_historical: Historical population table with metadata
        tb_projection: Projection population table with metadata

    Returns:
        Formatted source citation string
    """
    # Collect unique origins from all indicators used
    all_origins = []

    if "population_historical" in tb_historical.columns:
        col = tb_historical["population_historical"]
        if hasattr(col.metadata, "origins") and col.metadata.origins:
            all_origins.extend(col.metadata.origins)

    if "population_projection" in tb_projection.columns:
        col = tb_projection["population_projection"]
        if hasattr(col.metadata, "origins") and col.metadata.origins:
            all_origins.extend(col.metadata.origins)

    # Deduplicate origins
    unique_origins = {}
    for origin in all_origins:
        key = (origin.attribution_short, origin.title, origin.date_published)
        if key not in unique_origins:
            unique_origins[key] = origin

    # Build source citation
    source_parts = []
    for (producer, title, date_pub), origin in sorted(unique_origins.items()):
        year = date_pub.split("-")[0] if date_pub else ""
        source_parts.append(f"{producer} ({year})")

    return "Data sources: " + "; ".join(source_parts) if source_parts else "Data sources: UN World Population Prospects"


def find_peak_population(tb_projection) -> tuple[float, int]:
    """Find peak population and year from projection data.

    Args:
        tb_projection: Projection population table

    Returns:
        Tuple of (peak_population_billions, peak_year)
    """
    world_proj = tb_projection[tb_projection.index.get_level_values("country") == "World"].reset_index()
    if "population_projection" in world_proj.columns and not world_proj.empty:
        peak_idx = world_proj["population_projection"].idxmax()
        peak_pop = world_proj.loc[peak_idx, "population_projection"] / 1e9
        peak_year = int(world_proj.loc[peak_idx, "year"])
        return peak_pop, peak_year
    return None, None


def create_visualization(
    milestones: list[dict],
    tb_historical,
    tb_projection,
    source_text: str,
    peak_pop: float | None,
    peak_year: int | None,
    historical_cutoff_year: int,
) -> plt.Figure:
    """Create the population milestone bar chart.

    Args:
        milestones: List of milestone dictionaries
        tb_historical: Historical population table with metadata
        tb_projection: Projection population table with metadata
        source_text: Source citation text
        peak_pop: Peak population in billions (from projections)
        peak_year: Year of peak population
        historical_cutoff_year: Last year of historical data

    Returns:
        Matplotlib figure object
    """
    # Colors from OWID style
    bar_color = "#8193A8"  # Muted blue-grey
    text_color = "#333333"
    grey_text = "#666666"

    fig, ax = plt.subplots(figsize=(16, 10))

    # Prepare data
    x_positions = [m["milestone"] for m in milestones]
    # Use display_duration for bar height (capped for first bar)
    durations = [m["display_duration"] for m in milestones]

    # Create bars
    bars = ax.bar(x_positions, durations, width=0.7, color=bar_color, edgecolor="none")

    # Customize axes (dynamically based on data)
    num_milestones = len(milestones)
    ax.set_xlim(0.3, num_milestones + 0.7)
    ax.set_ylim(0, max(durations) * 1.15)

    # X-axis: billion labels
    ax.set_xticks(x_positions)
    ax.set_xticklabels([f"{int(m)} billion" for m in x_positions], fontsize=14, color=text_color)
    ax.tick_params(axis="x", length=0, pad=10)

    # Y-axis: years
    y_max = max(durations)

    # Determine appropriate tick interval based on range
    if y_max > 1000:
        tick_interval = 20
        y_max_display = 160  # Cap display at reasonable value, first bar will extend beyond
    else:
        tick_interval = 20
        y_max_display = y_max * 1.15

    y_ticks = list(range(0, int(y_max_display) + tick_interval, tick_interval))
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([f"{y} years" for y in y_ticks], fontsize=12, color=grey_text)
    ax.tick_params(axis="y", length=0, left=False)

    # Set y limit to accommodate all bars
    ax.set_ylim(0, y_max * 1.15)

    # Grid
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#EEEEEE", linewidth=1)
    ax.grid(axis="x", visible=False)

    # Remove spines
    for spine in ["top", "right", "left", "bottom"]:
        ax.spines[spine].set_visible(False)

    # Add labels on bars
    for i, (milestone, bar) in enumerate(zip(milestones, bars)):
        duration = milestone["duration"]  # Use actual duration for label
        year_range = f"{milestone['year_start']}-{milestone['year_end']}"

        # Special annotation for first billion (skip duration labels)
        if milestone["milestone"] == 1:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 0.5,
                f"All of human\nhistory up\nto {milestone['year_end']}",
                ha="center",
                va="center",
                fontsize=11,
                color=text_color,
            )
        else:
            # Main duration label on top of bar
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 3,
                f"{duration} years",
                ha="center",
                va="bottom",
                fontsize=13,
                fontweight="bold",
                color=text_color,
            )

            # Year range below duration
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() - 5,
                year_range,
                ha="center",
                va="top",
                fontsize=11,
                color=text_color,
            )

    # Determine where projections start based on historical cutoff year
    # Find first milestone where year_end is after the historical cutoff
    projection_start_milestone = None
    for i, milestone in enumerate(milestones):
        if milestone["year_end"] > historical_cutoff_year:
            projection_start_milestone = i
            break

    # Add projection annotation if we have peak data
    if peak_pop is not None and peak_year is not None and len(milestones) > 0:
        last_milestone = milestones[-1]
        last_pos = last_milestone["milestone"]
        ax.annotate(
            f"The latest UN projection\nexpects that the world population\npeaks at {peak_pop:.1f} billion in {peak_year}.",
            xy=(last_pos, last_milestone["duration"]),
            xytext=(last_pos, last_milestone["duration"] + 30),
            ha="center",
            va="bottom",
            fontsize=11,
            color=grey_text,
            arrowprops=dict(
                arrowstyle="->",
                connectionstyle="arc3,rad=0",
                color=grey_text,
                lw=1.5,
            ),
        )

        # Add "UN Projections" label if we detected projection milestones
        if projection_start_milestone is not None and projection_start_milestone < len(milestones):
            # Position label between projection start and end
            label_x = (milestones[projection_start_milestone]["milestone"] + last_pos) / 2
            ax.text(
                label_x,
                -8,
                "UN Projections",
                ha="center",
                va="top",
                fontsize=11,
                color=grey_text,
                style="italic",
            )

            # Add arrow under projection bars
            arrow_start = milestones[projection_start_milestone]["milestone"] - 0.3
            arrow_end = last_pos + 0.3
            ax.annotate(
                "",
                xy=(arrow_end, -8),
                xytext=(arrow_start, -8),
                arrowprops=dict(arrowstyle="->", color=grey_text, lw=1.5),
            )

    # Title
    fig.suptitle(
        "Time for the world population to increase by one billion",
        x=0.06,
        y=0.96,
        ha="left",
        fontsize=28,
        fontweight="normal",
        color=text_color,
    )
    # Source note (using metadata-derived sources)
    fig.text(
        0.06,
        0.05,
        f"Note: This is calculated based on the year that the population is one billion larger by the UN's mid-year estimate.\n"
        f"{source_text}\n"
        "OurWorldInData.org – Research and data to make progress against the world's largest problems.",
        fontsize=9,
        color=grey_text,
        ha="left",
        va="bottom",
    )

    # Layout
    fig.subplots_adjust(top=0.90, left=0.06, right=0.94, bottom=0.12)

    return fig


def run() -> None:
    """Create population billion milestone visualization.

    Generates a bar chart showing the decreasing time intervals between
    consecutive billion-person population milestones.

    Output:
    - SVG and PNG charts
    - Dataset with milestone data
    """
    # Load the population dataset
    ds = Dataset(etl_paths.DATA_DIR / "garden/demography/2024-07-15/population")

    # Load historical and projection tables
    tb_historical = ds["historical"]
    tb_projection = ds["projections"]

    # Extract World data
    world_hist = tb_historical[tb_historical.index.get_level_values("country") == "World"].reset_index()
    world_proj = tb_projection[tb_projection.index.get_level_values("country") == "World"].reset_index()

    # Combine historical and projection data
    hist_data = world_hist[["year", "population_historical"]].copy()
    hist_data.columns = ["year", "population"]

    proj_data = world_proj[["year", "population_projection"]].copy()
    proj_data.columns = ["year", "population"]

    # Combine
    tb = pr.concat([hist_data, proj_data], ignore_index=True).sort_values("year").drop_duplicates(subset=["year"])
    # Find milestones
    milestones = find_population_milestones(tb)

    # Build source citation from metadata
    source_text = build_source_citation(tb_historical, tb_projection)

    # Find peak population from projections
    peak_pop, peak_year = find_peak_population(tb_projection)

    # Get historical cutoff year
    historical_cutoff_year = int(world_hist["year"].max())

    # Create visualization
    fig = create_visualization(
        milestones, tb_historical, tb_projection, source_text, peak_pop, peak_year, historical_cutoff_year
    )

    # Save outputs
    output_path_svg = CURRENT_DIR / "population_billion_milestones.svg"
    fig.savefig(
        output_path_svg,
        format="svg",
        dpi=300,
        bbox_inches="tight",
        metadata={"Date": None},
    )
    log.info(f"Saved chart to {output_path_svg}")

    output_path_png = CURRENT_DIR / "population_billion_milestones.png"
    fig.savefig(output_path_png, format="png", dpi=300, bbox_inches="tight")
    log.info(f"Saved chart to {output_path_png}")

    # Export data
    milestones_table = Table(milestones)
    milestones_table["country"] = "World"
    milestones_table = milestones_table.format(["milestone", "country"], short_name=SHORT_NAME)

    # Create dataset
    ds_export = create_dataset(OUTPUT_DIR, tables=[milestones_table])
    ds_export.save()

    plt.close(fig)
    log.info("Visualization complete")


if __name__ == "__main__":
    run()
