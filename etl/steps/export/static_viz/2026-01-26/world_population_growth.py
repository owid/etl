"""Create world population growth visualization (1700-2100)."""

import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import structlog
from owid.catalog import Dataset

from etl import paths as etl_paths

# Output directory for export step
output_dir = etl_paths.EXPORT_DIR / "static_viz/2026-01-26/world_population_growth"
log = structlog.get_logger()


def run() -> None:
    """Create world population growth chart."""
    # Load the population dataset from garden
    ds = Dataset(etl_paths.DATA_DIR / "garden/demography/2024-07-15/population")

    # Load historical and projection tables
    tb_historical = ds["historical"]
    tb_projection = ds["projections"]
    # Extract World data from historical
    world_hist = tb_historical[tb_historical.index.get_level_values("country") == "World"].reset_index()
    world_proj = tb_projection[tb_projection.index.get_level_values("country") == "World"].reset_index()

    # Get full historical data to access 10,000 BCE
    tb_full_hist = world_hist[["year", "population_historical", "growth_rate_historical"]].copy()

    # Filter historical data to only include specific years before 1950
    years_before_1950_to_keep = [1700, 1750, 1800, 1850, 1900, 1925]
    tb_hist_filtered = tb_full_hist[
        (tb_full_hist["year"].isin(years_before_1950_to_keep)) | (tb_full_hist["year"] >= 1950)
    ][["year", "population_historical"]].copy()
    tb_hist_filtered.columns = ["year", "population"]

    # Compute growth rates from population values for filtered historical data
    tb_hist_filtered = tb_hist_filtered.sort_values("year").reset_index(drop=True)
    tb_hist_filtered["growth_rate"] = np.nan

    # Special case: Calculate growth rate for 1700 using 10,000 BCE to 1700
    pop_10000bce = tb_full_hist[tb_full_hist["year"] == -10000]["population_historical"].values
    pop_1700 = tb_hist_filtered[tb_hist_filtered["year"] == 1700]["population"].values
    if len(pop_10000bce) > 0 and len(pop_1700) > 0:
        growth_rate_1700 = 100 * (np.log(pop_1700[0] / pop_10000bce[0]) / (1700 - (-10000)))
        log.info(f"Calculated growth rate for 1700 (10,000 BCE to 1700): {growth_rate_1700:.4f}%")
        tb_hist_filtered.loc[tb_hist_filtered["year"] == 1700, "growth_rate"] = growth_rate_1700

    # Calculate growth rates for other adjacent years in our filtered data
    for i in range(1, len(tb_hist_filtered)):
        year_current = tb_hist_filtered.loc[i, "year"]
        # Skip if this is 1700 (already calculated above)
        if year_current == 1700:
            continue
        year_prev = tb_hist_filtered.loc[i - 1, "year"]
        pop_current = tb_hist_filtered.loc[i, "population"]
        pop_prev = tb_hist_filtered.loc[i - 1, "population"]
        years_diff = year_current - year_prev
        if years_diff > 0 and pop_prev > 0:
            tb_hist_filtered.loc[i, "growth_rate"] = 100 * (np.log(pop_current / pop_prev) / years_diff)

    log.info(f"Filtered historical data to {len(tb_hist_filtered)} years and computed growth rates")

    # Prepare projection data
    tb_proj = world_proj[["year", "population_projection", "growth_rate_projection"]].copy()
    tb_proj.columns = ["year", "population", "growth_rate"]

    # Combine filtered historical and projection data
    tb = pd.concat([tb_hist_filtered, tb_proj], ignore_index=True).sort_values("year")

    # Filter to 1700-2100
    tb = tb[(tb["year"] >= 1700) & (tb["year"] <= 2100)].copy()

    # Omit growth rates for years marked as "Omitted for smoothing" in the spreadsheet (1750, 1930, 1950)
    # tb.loc[tb["year"].isin([1750, 1930, 1950]), "growth_rate"] = np.nan
    # tb.loc[tb["year"].isin([1750, 1930, 1950]), "note"] = "Growth rate omitted for smoothing"

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
    # Make room below 0 for the projection bracket
    ax1.set_ylim(-0.65, pop_max * 1.03)

    ax1.set_xticks([1700, 1750, 1800, 1850, 1900, 1950, 2000, 2023, 2050, 2100])
    ax1.tick_params(axis="x", colors=text_grey, labelsize=12, length=0)

    # Hide y-axis entirely (OWID PNG has no y ticks/labels)
    ax1.tick_params(axis="y", left=False, labelleft=False)

    # Spines: only bottom, light grey
    for s in ["left", "right", "top"]:
        ax1.spines[s].set_visible(False)
    ax1.spines["bottom"].set_color(axis_grey)
    ax1.spines["bottom"].set_linewidth(1)

    # Growth-rate axis
    ax2 = ax1.twinx()

    tb_growth = tb.dropna(subset=["growth_rate"])
    ax2.plot(tb_growth["year"], tb_growth["growth_rate"], color=color_growth, linewidth=2.5, zorder=10)

    # Match spacing between axes - set fixed range to ensure equal visual spacing
    # Using 4 tick intervals (0, 0.5, 1.0, 1.5, 2.0) with equal spacing as population axis
    gr_min = -0.5  # Start at -0.5 to accommodate negative values
    gr_max = 2.5  # End at 2.5 to give equal spacing above
    ax2.set_ylim(gr_min, gr_max)

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
    # Define milestone thresholds in billions
    milestone_thresholds = [0.5, 1, 2, 2, 5, 8, 9, 10, 10.5]
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
                # Format with appropriate decimal places
                if threshold == int(pop):
                    label = f"{int(pop)} Billion\nin {year}"
                else:
                    label = f"{pop} Billion\nin {year}"

            milestones.append((year, pop, label))

    for year, pop, label in milestones:
        ax1.plot(year, pop, "o", color="black", markersize=5, zorder=20)
        ax1.text(year, pop + 0.25, label, ha="center", va="bottom", fontsize=11, color="#1f2a2a")

    # ----- Growth-rate labels (no arrows in the PNG) -----
    # Peak label
    peak_idx = tb["growth_rate"].idxmax()
    peak_year = int(tb.loc[peak_idx, "year"])
    peak_rate = float(tb.loc[peak_idx, "growth_rate"])
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
    if (tb["year"] == 2023).any() and not np.isnan(tb.loc[tb["year"] == 2023, "growth_rate"]).all():
        gr_2023 = float(tb.loc[tb["year"] == 2023, "growth_rate"].iloc[0])
        ax2.text(2023 + 8, gr_2023, f"{gr_2023:.1f}%\nin 2023", color=color_growth, fontsize=12, ha="left", va="center")

    # 2100 label (usually -0.1%)
    if (tb["year"] == 2100).any() and not np.isnan(tb.loc[tb["year"] == 2100, "growth_rate"]).all():
        gr_2100 = float(tb.loc[tb["year"] == 2100, "growth_rate"].iloc[0])
        ax2.text(2100 + 3, gr_2100, f"{gr_2100:.1f}%", color=color_growth, fontsize=12, ha="left", va="center")

    # ----- 10,000 BCE to 1700 note (magenta in the PNG) -----
    growth_rate_1700_val = float(tb.loc[tb["year"] == 1700, "growth_rate"].iloc[0])
    rate_text = "0.04%" if abs(growth_rate_1700_val - 0.04) < 0.005 else f"{growth_rate_1700_val:.2f}%"
    ax2.text(
        1665,
        0.55,  # position to the left of the chart area
        f"{rate_text} was the average\npopulation growth rate\nbetween 10,000 BCE\nand 1700",
        fontsize=10,
        color=color_growth,
        ha="left",
        va="center",
    )

    # ----- Projection bracket (instead of dashed vline) -----
    y0 = -0.28
    ax1.plot([year_cut, 2100], [y0, y0], color=axis_grey, linewidth=1)
    ax1.plot([year_cut, year_cut], [y0, y0 + 0.12], color=axis_grey, linewidth=1)
    ax1.plot([2100, 2100], [y0, y0 + 0.12], color=axis_grey, linewidth=1)
    ax1.text(
        2060,
        y0 - 0.06,
        "Projection\n(UN Medium Fertility Variant)",
        fontsize=10,
        color=text_grey,
        ha="center",
        va="top",
    )

    # ----- Source note (bottom-left, grey) -----
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

    source_text = "Data sources: " + "; ".join(source_parts)

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

    # Layout: reserve space for header + footer
    fig.subplots_adjust(top=0.84, left=0.06, right=0.97, bottom=0.12)

    # Prepare table data
    tb["country"] = "World"
    # Add notes explaining special cases
    tb["note"] = None
    tb.loc[tb["year"] == 1700, "note"] = (
        f"Growth rate calculated using population between 10,000 BCE and 1700: {growth_rate_1700:.4f}%"
    )
    tb = tb.format(["year", "country"], short_name="world_population_growth")

    # Create dataset for export step
    from etl.helpers import create_dataset

    ds_export = create_dataset(output_dir, tables=[tb])
    ds_export.save()

    # Save CSV file
    csv_path = output_dir / "world_population_growth_1700_2100.csv"
    tb.to_csv(csv_path, index=False)
    log.info(f"Saved CSV to {csv_path}")

    # Save charts AFTER dataset is saved (so they don't get deleted)
    output_path = output_dir / "world_population_growth_1700_2100.svg"
    plt.savefig(output_path, format="svg", dpi=300, bbox_inches="tight")
    log.info(f"Saved chart to {output_path}")

    output_path_png = output_dir / "world_population_growth_1700_2100.png"
    plt.savefig(output_path_png, format="png", dpi=300, bbox_inches="tight")
    log.info(f"Saved chart to {output_path_png}")

    plt.close()
