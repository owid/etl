"""Create world population growth visualization (1700-2100).

This code generates a chart showing world population and growth rates from 1700 to 2100, combining historical data with UN projections.
"""

from pathlib import Path

import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import structlog
from owid.catalog import Dataset

from etl import paths as etl_paths
from etl.helpers import create_dataset

# Paths for this export step
CURRENT_DIR = Path(__file__).parent
OUTPUT_DIR = etl_paths.EXPORT_DIR / "static_viz/2026-01-26/world_population_growth"
SHORT_NAME = "world_population_growth"

log = structlog.get_logger()


def calculate_milestones(tb: pd.DataFrame) -> list[tuple[int, float, str]]:
    """Calculate population milestones from data.

    Args:
        tb: DataFrame with 'year' and 'population_billions' columns

    Returns:
        List of tuples: (year, population_billions, label_text)
    """
    # Population milestones to mark on the chart
    milestone_thresholds = [0.5, 1, 2, 5, 8, 9, 10]
    milestones = []

    for threshold in milestone_thresholds:
        # Find the year when population first crosses this threshold
        crossing_data = tb[tb["population_billions"] >= threshold]
        if not crossing_data.empty:
            year = int(crossing_data.iloc[0]["year"])
            pop = float(crossing_data.iloc[0]["population_billions"])

            # Format the label
            if threshold < 1.0:
                # Convert to millions
                label = f"{int(pop * 1000)} Million\nin {year}"
            else:
                # Keep as billions
                if threshold == int(pop):
                    label = f"{int(pop)} Billion\nin {year}"
                else:
                    label = f"{pop} Billion\nin {year}"

            milestones.append((year, pop, label))

    return milestones


def build_source_citation(tb_historical: pd.DataFrame, tb_projection: pd.DataFrame) -> str:
    """Build source citation text from table metadata.

    Args:
        tb_historical: Historical population table with metadata
        tb_projection: Projection population table with metadata

    Returns:
        Formatted source citation string
    """
    # Collect unique origins from all indicators used
    all_origins = []
    for col_name in ["population_historical", "growth_rate_historical"]:
        col = tb_historical[col_name]
        if hasattr(col.metadata, "origins") and col.metadata.origins:
            all_origins.extend(col.metadata.origins)
    for col_name in ["population_projection", "growth_rate_projection"]:
        col = tb_projection[col_name]
        if hasattr(col.metadata, "origins") and col.metadata.origins:
            all_origins.extend(col.metadata.origins)

    # Deduplicate origins
    unique_origins = {}
    for origin in all_origins:
        key = (origin.producer, origin.title, origin.date_published)
        if key not in unique_origins:
            unique_origins[key] = origin

    # Build source citation
    source_parts = []
    for (producer, title, date_pub), origin in sorted(unique_origins.items()):
        year = date_pub.split("-")[0] if date_pub else ""
        if "HYDE" in title:
            source_parts.append(f"HYDE ({year})")
        elif "Gapminder" in producer:
            source_parts.append(f"Gapminder ({year})")
        elif "United Nations" in producer and "World Population Prospects" in title:
            source_parts.append(f"UN World Population Prospects ({year})")

    return "Data sources: " + "; ".join(source_parts)


def run() -> None:
    """Create world population growth visualization chart and export data.

    Generates a dual-axis chart showing:
    - World population 1700-2100 (area fill)
    - Annual growth rate 1700-2100 (line)

    The growth rate for 1700 is special: calculated using 10,000 BCE as baseline
    to show long-term historical context. Other growth rates use adjacent years.

    Growth rates are selectively displayed to create a smooth visualization:
    - 100-year intervals before 1800
    - 50-year intervals 1800-1900
    - 5-year intervals 1900-1950
    - Annual values from 1950 onwards

    Output:
    - SVG and PNG charts
    - Dataset with population and growth rate data
    """
    # Load the population dataset from grapher
    ds = Dataset(etl_paths.DATA_DIR / "garden/demography/2024-07-15/population")

    # Load historical and projection tables
    tb_historical = ds["historical"]
    tb_projection = ds["projections"]
    # Extract World data from historical
    world_hist = tb_historical[tb_historical.index.get_level_values("country") == "World"].reset_index()
    world_proj = tb_projection[tb_projection.index.get_level_values("country") == "World"].reset_index()

    # Get full historical data to access 10,000 BCE
    tb_full_hist = world_hist[["year", "population_historical", "growth_rate_historical"]].copy()

    tb_hist_filtered = tb_full_hist[["year", "population_historical"]].copy()
    tb_hist_filtered.columns = ["year", "population"]
    tb_hist_filtered = tb_hist_filtered.sort_values("year").reset_index(drop=True)

    # Calculate growth rate for 1700 using 10,000 BCE as baseline
    growth_rate_1700 = None
    pop_10000bce_data = tb_full_hist[tb_full_hist["year"] == -10000]["population_historical"]
    pop_1700_data = tb_hist_filtered[tb_hist_filtered["year"] == 1700]["population"]

    if not pop_10000bce_data.empty and not pop_1700_data.empty:
        pop_10000bce = pop_10000bce_data.iloc[0]
        pop_1700 = pop_1700_data.iloc[0]
        growth_rate_1700 = 100 * (np.log(pop_1700 / pop_10000bce) / (1700 - (-10000)))
        log.info(f"Calculated growth rate for 1700 (10,000 BCE to 1700): {growth_rate_1700:.4f}%")
    else:
        log.warning("Could not calculate growth rate for 1700: missing 10,000 BCE or 1700 data")

    # Vectorized growth rate calculation for adjacent years
    tb_hist_filtered["year_prev"] = tb_hist_filtered["year"].shift(1)
    tb_hist_filtered["pop_prev"] = tb_hist_filtered["population"].shift(1)
    tb_hist_filtered["years_diff"] = tb_hist_filtered["year"] - tb_hist_filtered["year_prev"]

    # Calculate growth rates (only where valid)
    mask = (tb_hist_filtered["years_diff"] > 0) & (tb_hist_filtered["pop_prev"] > 0)
    tb_hist_filtered["growth_rate"] = np.nan
    tb_hist_filtered.loc[mask, "growth_rate"] = 100 * (
        np.log(tb_hist_filtered.loc[mask, "population"] / tb_hist_filtered.loc[mask, "pop_prev"])
        / tb_hist_filtered.loc[mask, "years_diff"]
    )

    # Override 1700 with the special calculation if available
    if growth_rate_1700 is not None:
        tb_hist_filtered.loc[tb_hist_filtered["year"] == 1700, "growth_rate"] = growth_rate_1700

    # Clean up temporary columns
    tb_hist_filtered = tb_hist_filtered.drop(columns=["year_prev", "pop_prev", "years_diff"])

    log.info(f"Filtered historical data to {len(tb_hist_filtered)} years and computed growth rates")

    # Prepare projection data
    tb_proj = world_proj[["year", "population_projection", "growth_rate_projection"]].copy()
    tb_proj.columns = ["year", "population", "growth_rate"]

    # Combine filtered historical and projection data
    tb = pd.concat([tb_hist_filtered, tb_proj], ignore_index=True).sort_values("year")

    # Filter to 1700-2100
    tb = tb[(tb["year"] >= 1700) & (tb["year"] <= 2100)].copy()

    # Validate expected years exist
    expected_years = [1700, 2023, 2100]
    missing_years = [year for year in expected_years if year not in tb["year"].values]
    if missing_years:
        log.warning(f"Missing expected years in data: {missing_years}")

    # Validate no gaps between historical and projection
    year_cut = int(world_hist["year"].max())
    if year_cut not in tb["year"].values or (year_cut + 1) not in tb["year"].values:
        log.warning(f"Potential gap at historical/projection boundary (year {year_cut})")

    # Omit growth rates for years that don't match the R filtering pattern:
    # Keep only: 100-year intervals (1700-1800), 50-year intervals (1800-1900), 5-year intervals (1900-1950), all years >= 1950
    filter_mask = (
        ((tb["year"] >= 1700) & (tb["year"] < 1800) & (tb["year"] % 100 == 0))
        | ((tb["year"] >= 1800) & (tb["year"] < 1900) & (tb["year"] % 50 == 0))
        | ((tb["year"] >= 1900) & (tb["year"] < 1950) & (tb["year"] % 5 == 0))
        | (tb["year"] >= 1950)
    )
    tb.loc[~filter_mask, "growth_rate"] = np.nan

    # Convert population to billions
    tb["population_billions"] = tb["population"] / 1e9

    # Colors sampled from the original PNG
    color_pop_hist = "#0C988C"
    color_pop_proj = "#17A899"
    color_growth = "#B4358C"
    grid_color = "#EEEEEE"
    axis_grey = "#999999"
    text_grey = "#666666"

    # Split for projection shading
    # Determine cutoff year: last year of historical data
    year_cut = int(world_hist["year"].max())
    tb = tb.sort_values("year")
    hist = tb[tb["year"] <= year_cut].copy()
    proj = tb[tb["year"] >= year_cut].copy()
    # Figure aspect close to 2048×1500
    fig, ax1 = plt.subplots(figsize=(13.65, 10.0))

    # Vertical gridlines only (subtle)
    ax1.set_axisbelow(True)
    ax1.grid(axis="x", color=grid_color, linewidth=1)
    ax1.grid(axis="y", visible=False)

    # Area fills (no edgecolor; outline is drawn separately)
    ax1.fill_between(hist["year"], 0, hist["population_billions"], color=color_pop_hist, linewidth=0, zorder=1)
    ax1.fill_between(proj["year"], 0, proj["population_billions"], color=color_pop_proj, linewidth=0, zorder=1)

    # Black outline
    ax1.plot(tb["year"], tb["population_billions"], color="black", linewidth=1.6, zorder=3)

    # Axes limits & ticks
    ax1.set_xlim(1700, 2100)

    pop_max = float(tb["population_billions"].max())
    # Make room below 0 for the projection bracket and label
    pop_min = -2  # Need extra space for bracket at -0.7 and text well below at -0.95
    pop_max_adjusted = pop_max * 1
    ax1.set_ylim(pop_min, pop_max_adjusted)

    ax1.set_xticks([1700, 1750, 1800, 1850, 1900, 1950, 2000, 2023, 2050, 2100])
    ax1.tick_params(axis="x", colors=text_grey, labelsize=12, length=0)

    # Set y-axis ticks for population (left side)
    # Create ticks at nice round numbers in billions
    pop_ticks = [0, 2, 4, 6, 8, 10]
    ax1.set_yticks(pop_ticks)
    ax1.set_yticklabels([f"{int(t)}" for t in pop_ticks])
    ax1.tick_params(axis="y", colors=text_grey, labelsize=12, length=0, left=False, labelleft=True)

    # Spines: left and bottom, light grey
    for s in ["right", "top"]:
        ax1.spines[s].set_visible(False)
    ax1.spines["bottom"].set_color(axis_grey)
    ax1.spines["bottom"].set_linewidth(1)
    ax1.spines["left"].set_color(axis_grey)
    ax1.spines["left"].set_linewidth(1)

    # Growth-rate axis
    ax2 = ax1.twinx()

    tb_growth = tb.dropna(subset=["growth_rate"])
    ax2.plot(tb_growth["year"], tb_growth["growth_rate"], color=color_growth, linewidth=2.5, zorder=10)

    # Align axes so both have 0 at the same visual position
    # The visible chart area should have 0 aligned, but pop_min extends below for labels
    # Calculate where 0 appears in the population axis
    zero_ratio = (0 - pop_min) / (pop_max_adjusted - pop_min)

    log.info(f"Axis alignment: pop_min={pop_min}, pop_max={pop_max_adjusted}, zero_ratio={zero_ratio}")

    # Set growth rate limits so 0 is at zero_ratio position
    # If 0 is at zero_ratio, then: (0 - gr_min) / (gr_max - gr_min) = zero_ratio
    # We want reasonable limits, so set gr_max first, then calculate gr_min
    gr_max = 2.5  # Maximum growth rate to show
    gr_min = -gr_max * zero_ratio / (1 - zero_ratio)
    ax2.set_ylim(gr_min, gr_max)

    log.info(f"Growth rate axis: gr_min={gr_min}, gr_max={gr_max}")

    # Set up right axis with ticks at regular intervals (similar spacing to population axis)
    ax2.set_yticks([0, 0.5, 1.0, 1.5, 2.0])
    ax2.tick_params(axis="y", colors=text_grey, labelsize=12, length=0, right=False, labelright=True)
    ax2.set_ylabel("Annual growth rate (%)", color=text_grey, fontsize=12, labelpad=10)

    # Show only right spine, light grey
    for s in ["left", "top", "bottom"]:
        ax2.spines[s].set_visible(False)
    ax2.spines["right"].set_color(axis_grey)
    ax2.spines["right"].set_linewidth(1)

    # ----- Header (title + OWID-like “legend” under title) -----
    fig.suptitle("World population growth, 1700-2100", x=0.07, y=0.96, ha="left", fontsize=30, fontweight="normal")

    # Small swatches in figure coordinates
    # Growth line swatch
    fig.lines.append(
        plt.Line2D([0.075, 0.105], [0.905, 0.905], transform=fig.transFigure, color=color_growth, linewidth=3)
    )
    fig.text(
        0.11,
        0.905,
        "Annual growth rate of the world population",
        color=color_growth,
        fontsize=15,
        ha="left",
        va="center",
    )

    # Population swatch (filled mini-rect)
    rect = patches.Rectangle(
        (0.075, 0.868), 0.03, 0.018, transform=fig.transFigure, facecolor=color_pop_proj, edgecolor="none"
    )
    fig.add_artist(rect)
    fig.text(0.11, 0.877, "World population", color=color_pop_proj, fontsize=15, ha="left", va="center")

    # ----- Milestones (extracted from data) -----
    milestones = calculate_milestones(tb)
    for year, pop, label in milestones:
        ax1.plot(year, pop, "o", color="black", markersize=5, zorder=20)
        # Adjust label offset: smaller for higher populations to avoid going off-chart
        label_offset = 0.3 if pop < 5 else 0.4
        ax1.text(year, pop + label_offset, label, ha="right", va="bottom", fontsize=11, color="#1f2a2a")

    # ----- Growth-rate labels (no arrows in the PNG) -----
    # Peak label
    tb_growth_valid = tb.dropna(subset=["growth_rate"])
    if not tb_growth_valid.empty:
        peak_idx = tb_growth_valid["growth_rate"].idxmax()
        peak_year = int(tb_growth_valid.loc[peak_idx, "year"])
        peak_rate = float(tb_growth_valid.loc[peak_idx, "growth_rate"])
        ax2.text(
            peak_year + 6,
            peak_rate + 0.05,
            f"{peak_rate:.1f}%\nin {peak_year}",
            color=color_growth,
            fontsize=12,
            ha="left",
            va="bottom",
        )

    # 2023 label (if available)
    tb_2023 = tb[tb["year"] == 2023]
    if not tb_2023.empty and not pd.isna(tb_2023["growth_rate"].iloc[0]):
        gr_2023 = float(tb_2023["growth_rate"].iloc[0])
        ax2.text(2023 + 8, gr_2023, f"{gr_2023:.1f}%\nin 2023", color=color_growth, fontsize=12, ha="left", va="center")

    # 2100 label (usually -0.1%)
    tb_2100 = tb[tb["year"] == 2100]
    if not tb_2100.empty and not pd.isna(tb_2100["growth_rate"].iloc[0]):
        gr_2100 = float(tb_2100["growth_rate"].iloc[0])
        ax2.text(2100 + 3, gr_2100, f"{gr_2100:.1f}%", color=color_growth, fontsize=12, ha="left", va="center")

    # ----- 10,000 BCE to 1700 note (magenta in the PNG) -----
    tb_1700 = tb[tb["year"] == 1700]
    if not tb_1700.empty and not pd.isna(tb_1700["growth_rate"].iloc[0]):
        growth_rate_1700_val = float(tb_1700["growth_rate"].iloc[0])
        rate_text = "0.04%" if abs(growth_rate_1700_val - 0.04) < 0.005 else f"{growth_rate_1700_val:.2f}%"
    else:
        rate_text = "N/A"
    ax2.text(
        1705,
        0.4,  # position to the left of the chart area
        f"{rate_text} was the average\npopulation growth rate\nbetween 10,000 BCE\nand 1700",
        fontsize=10,
        color=color_growth,
        ha="left",
        va="center",
    )

    # ----- Projection bracket (instead of dashed vline) -----
    # Draw bracket in data coordinates
    bracket_y = -0.7  # Fixed position in data coordinates
    bracket_height = 0.08
    bracket_center_x = (year_cut + 2100) / 2

    log.info(f"Bracket: year_cut={year_cut}, center={(year_cut + 2100) / 2}")

    ax1.plot([year_cut, 2100], [bracket_y, bracket_y], color=axis_grey, linewidth=1, clip_on=False)
    ax1.plot([year_cut, year_cut], [bracket_y, bracket_y + bracket_height], color=axis_grey, linewidth=1, clip_on=False)
    ax1.plot([2100, 2100], [bracket_y, bracket_y + bracket_height], color=axis_grey, linewidth=1, clip_on=False)

    # Place text centered below the bracket - use clip_on=False to allow it outside axes
    ax1.text(
        bracket_center_x,  # Center in x (data coordinates)
        -0.95,  # Absolute position in data coordinates
        "Projection\n(UN Medium Fertility Variant)",
        fontsize=9,
        color=text_grey,
        ha="center",
        va="top",
        clip_on=False,  # Allow text to appear outside axes
    )

    # ----- Source note (bottom-left, grey) -----
    source_text = build_source_citation(tb_historical, tb_projection)

    fig.text(
        0.07,
        0.05,
        f"{source_text}\n"
        "This is a visualization from OurWorldInData.org, where you find data and research on how the world is changing.",
        fontsize=9,
        color=text_grey,
        ha="left",
        va="bottom",
    )

    # Layout: reserve space for header + footer (extra bottom space for projection label)
    fig.subplots_adjust(top=0.84, left=0.06, right=0.97, bottom=0.16)

    # Prepare table data
    tb["country"] = "World"
    # Add notes explaining special cases
    tb["note"] = None
    if growth_rate_1700 is not None:
        tb.loc[tb["year"] == 1700, "note"] = (
            f"Growth rate calculated using population between 10,000 BCE and 1700: {growth_rate_1700:.4f}%"
        )
    tb.loc[~filter_mask, "note"] = "Growth rate omitted for smooth visualization"

    tb = tb.format(["year", "country"], short_name=SHORT_NAME)

    ds_export = create_dataset(OUTPUT_DIR, tables=[tb])
    ds_export.save()

    # Save charts AFTER dataset is saved (so they don't get deleted)
    output_path = OUTPUT_DIR / "world_population_growth_1700_2100.svg"
    plt.savefig(output_path, format="svg", dpi=300, bbox_inches="tight")
    log.info(f"Saved chart to {output_path}")

    output_path_png = OUTPUT_DIR / "world_population_growth_1700_2100.png"
    plt.savefig(output_path_png, format="png", dpi=300, bbox_inches="tight")
    log.info(f"Saved chart to {output_path_png}")

    plt.close()
