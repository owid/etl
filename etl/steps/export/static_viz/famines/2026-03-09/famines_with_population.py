"""Create visualization showing famine deaths by decade with world population overlay.

This code generates a bar chart showing decadal famine deaths with a line showing
world population growth over the same period.
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Configure matplotlib to use SVG text elements instead of paths
matplotlib.rcParams["svg.fonttype"] = "none"
# Set deterministic hash for reproducible SVG output
matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"

paths = PathFinder(__file__)


def run() -> None:
    """Create famines with population visualization.

    Generates a dual-axis chart showing famine deaths by decade (bars) and
    world population (line).

    Output:
    - SVG and PNG charts
    - Dataset with combined data
    """
    # Load the famines dataset
    ds_famines = paths.load_dataset("total_famines_by_year_decade")
    tb_famines = ds_famines.read("total_famines_by_year_decade")

    # Load population data
    ds_population = paths.load_dataset("population")
    tb_pop_hist = ds_population.read("historical")

    # Prepare data
    famine_data, pop_data = prepare_data(tb_famines, tb_pop_hist)

    # Build source citation
    source_text = build_source_citation(tb_famines, tb_pop_hist)
    paths.log.info(f"Source citation: {source_text}")

    # Get the latest decade info for the note
    latest_decade_info = get_latest_decade_info(tb_famines)

    # Create visualization
    fig = create_visualization(famine_data, pop_data, source_text, latest_decade_info)

    # Save outputs
    paths.export_fig(fig, "famines_with_population", ["svg", "png"], dpi=300, bbox_inches="tight", transparent=True)
    paths.log.info("Optimized SVG for Figma")

    # Export data
    import owid.catalog.processing as pr

    # Convert to Tables and merge
    famine_table = Table(famine_data[["decade", "total_deaths"]])
    pop_table = Table(pop_data[["decade", "population"]])

    combined_table = pr.merge(famine_table, pop_table, on="decade", how="outer")
    combined_table["country"] = "World"
    combined_table = combined_table.format(["decade", "country"], short_name=paths.short_name)

    # Create dataset
    ds_export = paths.create_dataset(tables=[combined_table])
    ds_export.save()

    plt.close(fig)
    paths.log.info("Visualization complete")


def prepare_data(tb_famines: Table, tb_pop: Table) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare famine and population data for visualization.

    Args:
        tb_famines: Famines table with decadal deaths
        tb_pop: Population table with historical data

    Returns:
        Tuple of (famine_data_df, population_data_df)
    """
    # Filter to World data
    tb_famines_reset = tb_famines.reset_index()
    world_famines = tb_famines_reset[tb_famines_reset["country"] == "World"].copy()

    # Create decade labels and aggregate
    world_famines["decade"] = (world_famines["year"] // 10) * 10

    # Group by decade and sum deaths
    decade_data = (
        world_famines.groupby("decade")
        .agg({"decadal_famine_deaths": "first"})  # Use first value as it's already decadal
        .reset_index()
    )
    decade_data.columns = ["decade", "total_deaths"]

    # Filter to decades with data (exclude zeros)
    decade_data = decade_data[decade_data["total_deaths"] > 0].copy()

    # Get world population at start of each decade
    tb_pop_reset = tb_pop.reset_index()
    world_pop = tb_pop_reset[tb_pop_reset["country"] == "World"].copy()

    # Get population at decade start years
    decade_years = decade_data["decade"].unique()
    pop_decade = world_pop[world_pop["year"].isin(decade_years)].copy()
    pop_decade["decade"] = pop_decade["year"]
    pop_decade = pop_decade[["decade", "population_historical"]].copy()
    pop_decade.columns = ["decade", "population"]

    return decade_data, pop_decade


def get_latest_decade_info(tb_famines: Table) -> str:
    """Get information about the latest incomplete decade.

    Args:
        tb_famines: Famines table with year data

    Returns:
        String describing the latest decade's data range, or empty string if decade is complete
    """
    tb_famines_reset = tb_famines.reset_index()
    world_famines = tb_famines_reset[tb_famines_reset["country"] == "World"].copy()

    # Get the maximum year in the data
    max_year = world_famines["year"].max()
    current_decade = (max_year // 10) * 10

    # Calculate the start and end years covered in the current decade
    decade_data = world_famines[world_famines["year"] >= current_decade]
    if not decade_data.empty:
        min_year_in_decade = decade_data["year"].min()
        max_year_in_decade = decade_data["year"].max()

        # If the decade is incomplete (doesn't reach the last year of the decade)
        if max_year_in_decade < current_decade + 9:
            return f"The figure for the {current_decade}s is preliminary and includes data from {min_year_in_decade}-{max_year_in_decade}."

    return ""


def build_source_citation(tb_famines, tb_pop) -> str:
    """Build source citation text from table metadata.

    Args:
        tb_famines: Famines table with metadata
        tb_pop: Population table with metadata

    Returns:
        Formatted source citation string
    """
    # Collect unique origins
    all_origins = []

    # Get famines origins
    if "decadal_famine_deaths" in tb_famines.columns:
        col = tb_famines["decadal_famine_deaths"]
        if hasattr(col.metadata, "origins") and col.metadata.origins:
            all_origins.extend(col.metadata.origins)

    # Get population origins
    if "population_historical" in tb_pop.columns:
        col = tb_pop["population_historical"]
        if hasattr(col.metadata, "origins") and col.metadata.origins:
            all_origins.extend(col.metadata.origins)

    # Deduplicate origins and filter out invalid entries
    unique_origins = {}
    for origin in all_origins:
        # Use attribution_short for population sources, producer for famine sources
        producer = None
        if hasattr(origin, "attribution_short") and origin.attribution_short:
            producer = origin.attribution_short
        elif hasattr(origin, "producer") and origin.producer:
            producer = origin.producer
        elif hasattr(origin, "title_snapshot") and origin.title_snapshot:
            producer = origin.title_snapshot
        elif hasattr(origin, "title") and origin.title:
            producer = origin.title

        # Skip origins without valid producer or if producer is string "None"
        if not producer or str(producer) == "None":
            continue

        key = (producer, origin.title, origin.date_published)
        if key not in unique_origins:
            unique_origins[key] = origin

    # Build source citation
    source_parts = []
    # Sort by producer name
    sorted_origins = sorted(unique_origins.items(), key=lambda x: (str(x[0][0]), x[0][2] or ""))
    for (producer, title, date_pub), origin in sorted_origins:
        year = date_pub.split("-")[0] if date_pub else ""
        # Skip if producer converts to "None" string
        if str(producer) != "None":
            source_parts.append(f"{producer} ({year})")

    return "Data sources: " + "; ".join(source_parts) if source_parts else "Data sources: World Peace Foundation; UN"


def create_visualization(
    famine_data: pd.DataFrame, pop_data: pd.DataFrame, source_text: str, latest_decade_info: str
) -> plt.Figure:
    """Create the famines with population dual-axis chart.

    Args:
        famine_data: DataFrame with decade and total_deaths
        pop_data: DataFrame with decade and population
        source_text: Source citation text
        latest_decade_info: Information about the latest incomplete decade

    Returns:
        Matplotlib figure object
    """
    # Colors from OWID style
    bar_color = "#8C4556"  # Dark red for famines
    line_color = "#4C6A9C"  # Blue for population line
    text_color = "#333333"
    grey_text = "#666666"

    fig, ax1 = plt.subplots(figsize=(20, 12), facecolor="none")

    # Merge data for alignment
    plot_data = pd.merge(famine_data, pop_data, on="decade", how="left")

    # Set transparent background for axes
    ax1.patch.set_alpha(0)

    # Primary axis: Famine deaths (bars)
    x_positions = np.arange(len(plot_data))
    ax1.bar(x_positions, plot_data["total_deaths"], width=0.7, color=bar_color, edgecolor="none", zorder=2)

    # Format primary y-axis (deaths)
    ax1.set_ylabel("Famine victims (left axis)", fontsize=14, color=text_color, labelpad=10)
    max_deaths = plot_data["total_deaths"].max()
    y_max = max_deaths * 1.15

    # Set y-axis ticks for deaths (in millions) - every 5 million
    y_ticks = np.arange(0, y_max + 5_000_000, 5_000_000)
    ax1.set_yticks(y_ticks)
    ax1.set_yticklabels(
        [f"{int(y/1_000_000)} Million" if y > 0 else "0" for y in y_ticks], fontsize=12, color=grey_text
    )
    ax1.set_ylim(0, y_max)
    ax1.tick_params(axis="y", length=0, left=False, labelcolor=text_color)

    # Secondary axis: World population (line)
    ax2 = ax1.twinx()
    valid_pop = plot_data[plot_data["population"].notna()]

    if not valid_pop.empty:
        # Plot population line
        pop_line = ax2.plot(
            x_positions[plot_data["population"].notna()],
            valid_pop["population"],
            color=line_color,
            linewidth=3,
            marker="none",
            zorder=3,
            label="World population (right axis)",
        )

        # Format secondary y-axis (population in billions)
        ax2.set_ylabel("World population (right axis)", fontsize=14, color=line_color, labelpad=10)
        pop_max = valid_pop["population"].max()
        pop_max_display = pop_max * 1.1

        # Set population ticks (in billions) - every 2 billion
        pop_ticks = np.arange(0, pop_max_display + 2e9, 2e9)
        ax2.set_yticks(pop_ticks)
        ax2.set_yticklabels(
            [f"{int(p/1e9)} Billion" if p > 0 else "0" for p in pop_ticks], fontsize=12, color=line_color
        )
        ax2.set_ylim(0, pop_max_display)
        ax2.tick_params(axis="y", length=0, right=False, labelcolor=line_color)

    # X-axis: Decades
    ax1.set_xticks(x_positions)
    decade_labels = [f"{int(d)}s" for d in plot_data["decade"]]
    ax1.set_xticklabels(decade_labels, fontsize=13, color=text_color)
    ax1.set_xlim(-0.5, len(plot_data) - 0.5)
    ax1.tick_params(axis="x", length=0, pad=10)

    # Grid
    ax1.set_axisbelow(True)
    ax1.grid(axis="y", color="#EEEEEE", linewidth=1, zorder=1)
    ax1.grid(axis="x", visible=False)

    # Remove spines
    for spine in ["top", "right", "left", "bottom"]:
        ax1.spines[spine].set_visible(False)
    for spine in ["top", "right", "left", "bottom"]:
        ax2.spines[spine].set_visible(False)

    # Legend
    from matplotlib.patches import Rectangle

    legend_elements = [
        Rectangle((0, 0), 1, 1, fc=bar_color, ec="none", label="Famine victims (left axis)"),
        plt.Line2D([0], [0], color=line_color, lw=3, label="World population (right axis)"),
    ]
    ax1.legend(
        handles=legend_elements,
        loc="upper right",
        frameon=False,
        fontsize=12,
        bbox_to_anchor=(0.98, 0.98),
    )

    # Title
    fig.suptitle(
        "Famine victims worldwide since the 1870s",
        x=0.06,
        y=0.975,
        ha="left",
        fontsize=28,
        fontweight="normal",
        color=text_color,
    )

    # Subtitle
    fig.text(
        0.06,
        0.92,
        "Global deaths in famines that are estimated to have killed at least 100,000 people.",
        fontsize=13,
        color=grey_text,
        ha="left",
    )

    # Notes - build note text dynamically
    note_lines = [
        "Note: For famines that happened at the end of a decade and the beginning of the next decade, the famine victims are split by decade on a year-by-year basis."
    ]
    if latest_decade_info:
        note_lines.append(latest_decade_info)
    note_lines.append(source_text)
    note_lines.append("OurWorldInData.org – Research and data to make progress against the world's largest problems.")

    fig.text(
        0.06,
        0.04,
        "\n".join(note_lines),
        fontsize=9,
        color=grey_text,
        ha="left",
        va="bottom",
    )

    # Layout
    fig.subplots_adjust(top=0.89, left=0.06, right=0.88, bottom=0.14)

    return fig


if __name__ == "__main__":
    run()
