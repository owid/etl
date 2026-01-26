"""Create world population growth visualization (1700-2100)."""

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create world population growth chart."""
    # Load the population dataset
    ds = paths.load_dataset("population", namespace="demography", version="2024-07-15")

    # Load historical and projection tables
    tb_historical = ds["historical"]
    tb_projection = ds["projections"]

    # Extract World data from historical
    world_hist = tb_historical[tb_historical.index.get_level_values("country") == "World"].reset_index()
    world_proj = tb_projection[tb_projection.index.get_level_values("country") == "World"].reset_index()

    # Get full historical data to access 10,000 BCE
    tb_full_hist = world_hist[["year", "population_historical", "growth_rate_historical"]].copy()

    # Calculate growth rate for 1700 using 10,000 BCE to 1700
    pop_10000bce = tb_full_hist[tb_full_hist["year"] == -10000]["population_historical"].values
    pop_1700 = tb_full_hist[tb_full_hist["year"] == 1700]["population_historical"].values
    if len(pop_10000bce) > 0 and len(pop_1700) > 0:
        growth_rate_1700 = 100 * (np.log(pop_1700[0] / pop_10000bce[0]) / (1700 - (-10000)))
        paths.log.info(f"Calculated growth rate for 1700 (10,000 BCE to 1700): {growth_rate_1700:.4f}%")
        # Update the growth rate for 1700
        tb_full_hist.loc[tb_full_hist["year"] == 1700, "growth_rate_historical"] = growth_rate_1700

    # Prepare historical data
    tb_hist = tb_full_hist[["year", "population_historical", "growth_rate_historical"]].copy()
    tb_hist.columns = ["year", "population", "growth_rate"]

    # Prepare projection data
    tb_proj = world_proj[["year", "population_projection", "growth_rate_projection"]].copy()
    tb_proj.columns = ["year", "population", "growth_rate"]

    # Combine historical and projection data
    import pandas as pd

    tb = pd.concat([tb_hist, tb_proj], ignore_index=True).sort_values("year")

    # Filter to 1700-2100
    tb = tb[(tb["year"] >= 1700) & (tb["year"] <= 2100)].copy()

    # Convert population to billions
    tb["population_billions"] = tb["population"] / 1e9

    # --- Create the chart (OWID-like styling) ---
    import matplotlib.patches as patches

    # Colors sampled from the original PNG
    color_pop_hist = "#0C988C"
    color_pop_proj = "#17A899"
    color_growth = "#B4358C"
    grid_color = "#EEEEEE"
    axis_grey = "#999999"
    text_grey = "#666666"

    # Split for projection shading
    year_cut = 2023
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
    for s in ["left", "right", "top", "bottom"]:
        ax2.spines[s].set_visible(False)
    ax2.tick_params(axis="y", right=False, labelright=False)

    tb_growth = tb.dropna(subset=["growth_rate"])
    ax2.plot(tb_growth["year"], tb_growth["growth_rate"], color=color_growth, linewidth=2.5, zorder=10)

    # Allow negatives (needed for -0.1% by 2100)
    gr_min = float(np.nanmin(tb["growth_rate"]))
    gr_max = float(np.nanmax(tb["growth_rate"]))
    pad = 0.2 * (gr_max - gr_min) if gr_max > gr_min else 0.2
    ax2.set_ylim(gr_min - pad, gr_max + pad)

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
    milestone_thresholds = [0.6, 1, 2, 2, 5, 8, 9, 10, 10.5]
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
                label = f"{int(threshold * 1000)} million\nin {year}"
            else:
                # Keep as billions
                # Format with appropriate decimal places
                if threshold == int(threshold):
                    label = f"{int(threshold)} Billion\nin {year}"
                else:
                    label = f"{threshold} Billion\nin {year}"

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
    fig.text(
        0.07,
        0.05,
        "Data sources: Our World in Data based on HYDE, UN, and UN Population Division (2022 Revision)\n"
        "This is a visualization from OurWorldInData.org, where you find data and research on how the world is changing.",
        fontsize=9,
        color=text_grey,
        ha="left",
        va="bottom",
    )

    # Layout: reserve space for header + footer
    fig.subplots_adjust(top=0.84, left=0.06, right=0.97, bottom=0.12)

    # Save
    output_path = paths.directory / "world_population_growth_1700_2100.svg"
    plt.savefig(output_path, format="svg", dpi=300, bbox_inches="tight")
    paths.log.info(f"Saved chart to {output_path}")

    output_path_png = paths.directory / "world_population_growth_1700_2100.png"
    plt.savefig(output_path_png, format="png", dpi=300, bbox_inches="tight")
    paths.log.info(f"Saved chart to {output_path_png}")

    plt.close()
    tb["country"] = "World"
    # Add note for 1700 explaining how growth rate was calculated
    tb["note"] = None
    tb.loc[tb["year"] == 1700, "note"] = (
        f"Growth rate calculated using population between 10,000 BCE and 1700: {growth_rate_1700:.4f}%"
    )

    tb = tb.format(["year", "country"], short_name="world_population_growth")

    # Create an empty dataset to satisfy ETL requirements
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
