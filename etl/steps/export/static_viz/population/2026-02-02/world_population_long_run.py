"""Create world population long-run visualization (10,000 BCE - 2100).

This code generates a chart showing world population from 10,000 BCE to 2100, combining historical data with UN projections.
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder

# Set matplotlib to use SVG backend with deterministic output (must be before creating figures)
matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"

# Initialize paths
paths = PathFinder(__file__)


def calculate_milestones(tb: Table) -> list[tuple[int, float, str]]:
    """Calculate population milestones from data.

    Args:
        tb: Table with 'year' and 'population_billions' columns

    Returns:
        List of tuples: (year, population_billions, label_text)
    """
    milestones = []

    # First, add specific years: 1700, 1800, 1900
    specific_years = [1700, 1800, 1900]
    for year in specific_years:
        year_data = tb.loc[tb["year"] == year]
        if not year_data.empty:
            pop = float(year_data.iloc[0]["population_billions"])
            # Format the label
            if pop < 1.0:
                label = f"{int(pop * 1000)} million in {year}"
            else:
                label = f"{pop:.2f} billion in {year}"
            milestones.append((year, pop, label))

    # Then add population milestones from 2 to 10 billion
    milestone_thresholds = [2, 3, 4, 5, 6, 7, 8, 9, 10]
    for threshold in milestone_thresholds:
        # Find the year when population first crosses this threshold
        crossing_data = tb.loc[tb["population_billions"] >= threshold]
        if not crossing_data.empty:
            year = int(crossing_data.iloc[0]["year"])
            pop = float(crossing_data.iloc[0]["population_billions"])
            label = f"{int(round(pop))} billion in {year}"
            milestones.append((year, pop, label))

    return milestones


def build_source_citation(tb: Table) -> str:
    """Build source citation text from table metadata.

    Args:
        tb: Population table with metadata

    Returns:
        Formatted source citation string
    """

    unique_origins = {}
    for origin in tb["population"].metadata.origins:
        key = (origin.attribution_short, origin.title, origin.date_published)
        if key not in unique_origins:
            unique_origins[key] = origin

    # Build source citation
    source_parts = []
    for (producer, title, date_pub), origin in sorted(unique_origins.items()):
        year = date_pub.split("-")[0] if date_pub else ""
        source_parts.append(f"{producer} ({year})")
    return "Data sources: " + "; ".join(source_parts)


def create_visualization(
    tb: Table,
    year_cut: int,
    life_expectancy_before_1800: float | None = None,
    life_expectancy_2023: float | None = None,
) -> plt.Figure:
    """Create the long-run population visualization figure.

    Args:
        tb: Complete table with year, population_billions
        year_cut: Year where historical data ends and projections begin

    Returns:
        Matplotlib figure object
    """
    # Colors sampled from the original PNG
    color_hist = "#804D7F"  # Purple for historical
    color_proj = "#C588C1"  # Light pink for projection
    grid_color = "#EEEEEE"
    axis_grey = "#999999"
    text_grey = "#666666"
    text_dark = "#1f2a2a"

    # Split for projection shading
    tb = tb.sort_values("year")
    hist = tb.loc[tb["year"] <= year_cut].copy()
    proj = tb.loc[tb["year"] >= year_cut].copy()

    # Figure aspect similar to the original
    fig, ax = plt.subplots(figsize=(10, 18))

    # Make background transparent
    fig.patch.set_alpha(0)
    ax.patch.set_alpha(0)

    # Vertical gridlines only (subtle)
    ax.set_axisbelow(True)
    ax.grid(axis="x", color=grid_color, linewidth=1)
    ax.grid(axis="y", visible=False)

    # Area fills (no edgecolor; outline is drawn separately)
    ax.fill_between(hist["year"], 0, hist["population_billions"], color=color_hist, linewidth=0, zorder=1)
    ax.fill_between(proj["year"], 0, proj["population_billions"], color=color_proj, linewidth=0, zorder=1)

    # Black outline
    ax.plot(tb["year"], tb["population_billions"], color="black", linewidth=1.6, zorder=3)

    # Axes limits & ticks
    ax.set_xlim(-10000, 2400)  # Extended right margin for labels

    pop_max = float(tb["population_billions"].max())
    ax.set_ylim(-1.2, pop_max * 1.08)  # More space below for life expectancy labels

    # X-axis ticks: Major BCE years and CE years
    xticks = [-12000, -10000, -8000, -6000, -4000, -2000, 0, 2000]
    ax.set_xticks(xticks)
    ax.set_xticklabels(["12,000 BCE", "10,000 BCE", "8,000 BCE", "6,000 BCE", "4,000 BCE", "2,000 BCE", "0", "2000"])
    ax.tick_params(axis="x", colors=text_grey, labelsize=12, length=0)

    # Y-axis ticks for population
    pop_ticks = list(range(0, int(pop_max) + 2, 2))
    ax.set_yticks(pop_ticks)
    ax.set_yticklabels([f"{int(t)}" for t in pop_ticks])
    ax.tick_params(axis="y", colors=text_grey, labelsize=12, length=0, left=False, labelleft=True)

    # Spines: left and bottom, light grey
    for s in ["right", "top"]:
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(axis_grey)
    ax.spines["bottom"].set_linewidth(1)
    ax.spines["left"].set_color(axis_grey)
    ax.spines["left"].set_linewidth(1)

    # Title
    fig.suptitle(
        "The size of the world population over the long-run",
        x=0.07,
        y=0.96,
        ha="left",
        fontsize=28,
        fontweight="normal",
    )

    # Milestones (extracted from data)
    milestones = calculate_milestones(tb)
    for year, pop, label in milestones:
        ax.plot(year, pop, "o", color="#804D7F", markersize=6, zorder=20)
        # Adjust label position based on year
        if year < 1500:
            # Early years: label above the marker
            ax.text(year, pop + 0.3, label, ha="center", va="bottom", fontsize=10.5, color=text_dark, weight="normal")
        else:
            # Recent years: label on the right side
            ax.text(2110, pop, label, ha="left", va="center", fontsize=10.5, color=text_dark, weight="normal")

    # Annotations (calculated from data)
    # Population in 10,000 BCE
    pop_10000bce_data = tb.loc[tb["year"] == -10000]
    if not pop_10000bce_data.empty:
        pop_10000bce = pop_10000bce_data.iloc[0]["population"]
        pop_10000bce_millions = int(pop_10000bce / 1e6)
        ax.text(
            -10000,
            0.5,
            f"In 10,000 BCE the\nworld population\nwas around {pop_10000bce_millions} million",
            fontsize=11,
            color=text_dark,
            ha="left",
            va="bottom",
        )

    # Calculate average growth rate from 10,000 BCE to 1700
    pop_1700_data = tb.loc[tb["year"] == 1700]
    if not pop_10000bce_data.empty and not pop_1700_data.empty:
        pop_10000bce = pop_10000bce_data.iloc[0]["population"]
        pop_1700 = pop_1700_data.iloc[0]["population"]
        years_diff = 1700 - (-10000)
        avg_growth_rate = 100 * (np.log(pop_1700 / pop_10000bce) / years_diff)
        ax.text(
            -4000,
            0.5,
            f"The average growth rate\nfrom 10,000 BCE to 1700\nwas just {avg_growth_rate:.2f}% per year",
            fontsize=11,
            color=text_dark,
            ha="center",
            va="bottom",
        )

    # Population in year 0
    pop_year0_data = tb.loc[tb["year"] == 0]
    if not pop_year0_data.empty:
        pop_year0 = pop_year0_data.iloc[0]["population"]
        pop_year0_millions = int(pop_year0 / 1e6)
        ax.text(
            0,
            1.5,
            f"In the year 0 the world\npopulation was around\n{pop_year0_millions} million",
            fontsize=11,
            color=text_dark,
            ha="center",
            va="bottom",
        )

    # "In the mid 14th century the Black Death pandemic killed between a quarter and half of all people in Europe"
    ax.text(
        1350,
        2.5,
        "In the mid 14th century the\nBlack Death pandemic killed\nbetween a quarter and half\nof all people in Europe",
        fontsize=11,
        color=text_dark,
        ha="right",
        va="bottom",
    )

    # Projection annotation (right side) - calculate peak from projection data
    proj_data = tb.loc[tb["year"] > year_cut]
    if not proj_data.empty:
        peak_idx = proj_data["population_billions"].idxmax()
        peak_year = int(proj_data.loc[peak_idx, "year"])
        peak_pop = float(proj_data.loc[peak_idx, "population_billions"])
        ax.text(
            peak_year - 30,
            10.4,
            f"The UN demographers expect the\nworld population to peak at {peak_pop:.1f}\nbillion in {peak_year} and to decline\nthereafter.",
            fontsize=10.5,
            color=text_dark,
            ha="right",
            va="top",
        )

    # Pink/purple line annotation
    ax.text(
        1980,
        8.2,
        "The pink line shows the \nprojection by the UN Population Division",
        fontsize=10.5,
        color="#C588C1",
        ha="right",
        va="top",
    )

    ax.text(
        1980,
        6.8,
        "The purple line shows the size of\nthe world population over the last\n12,000 years",
        fontsize=10.5,
        color="#804D7F",
        ha="right",
        va="top",
    )

    # Life expectancy annotations (bottom) - use data if available
    if life_expectancy_before_1800 is not None:
        ax.text(
            -1000,
            -0.4,
            f"Global life expectancy before\n1800 was less than {int(life_expectancy_before_1800)} years",
            fontsize=10,
            color="#18a0a8",
            ha="center",
            va="top",
            clip_on=False,
        )

    if life_expectancy_2023 is not None:
        ax.text(
            2100,
            -0.4,
            f"Global life expectancy\nin 2023: {int(life_expectancy_2023)} years",
            fontsize=10,
            color="#18a0a8",
            ha="center",
            va="top",
            clip_on=False,
        )

    # Source note (bottom-left, grey)
    source_text = build_source_citation(tb)

    fig.text(
        0.07,
        0.03,
        f"{source_text}\nThis is a visualization from OurWorldInData.org.",
        fontsize=9,
        color=text_grey,
        ha="left",
        va="bottom",
    )

    # License note (bottom-right, grey)
    fig.text(
        0.93,
        0.03,
        "Licensed under CC-BY-SA by the author Max Roser.",
        fontsize=9,
        color=text_grey,
        ha="right",
        va="bottom",
    )

    # Layout: reserve space for header + footer, with extra bottom space for life expectancy
    fig.subplots_adjust(top=0.91, left=0.06, right=0.85, bottom=0.14)

    return fig


def run() -> None:
    """Create world population long-run visualization chart.
    Generates a chart showing world population from 10,000 BCE to 2100.

    Output:
    - SVG and PNG charts
    """
    # Load population dataset using PathFinder
    ds_pop = paths.load_dataset("population")

    # Load population table (contains all data from -10000 to 2100)
    tb = ds_pop.read("population")
    tb = tb.loc[tb["country"] == "World", ["year", "population"]].copy()

    # Filter to 10,000 BCE - 2100 CE first
    tb = tb.loc[(tb["year"] >= -10000) & (tb["year"] <= 2100)].copy()
    tb["population_billions"] = tb["population"] / 1e9

    # Load historical and projection tables (needed for metadata and year_cut)
    tb_historical = ds_pop.read("historical")
    year_cut = int(tb_historical["year"].max())

    # Load life expectancy data (if available)
    life_expectancy_2023 = None
    life_expectancy_before_1800 = None
    try:
        ds_life_exp = paths.load_dataset("life_expectancy")
        tb_life_exp = ds_life_exp.read("life_expectancy_at_birth")
        world_life_exp = tb_life_exp.loc[tb_life_exp["country"] == "World"].reset_index()

        # Get life expectancy for 2023
        life_exp_2023_data = world_life_exp.loc[world_life_exp["year"] == 2023]
        if not life_exp_2023_data.empty:
            life_expectancy_2023 = float(life_exp_2023_data["life_expectancy_0"].iloc[0])

        # Get life expectancy before 1800 (use max from available data before 1800)
        pre_1800_data = world_life_exp.loc[world_life_exp["year"] < 1800]
        if not pre_1800_data.empty:
            life_expectancy_before_1800 = float(pre_1800_data["life_expectancy_0"].max())
    except FileNotFoundError:
        paths.log.warning("Life expectancy dataset not found. Try rerunning the life_expectancy etl step.")

    # Create visualization
    fig = create_visualization(tb, year_cut, life_expectancy_before_1800, life_expectancy_2023)

    # Save chart outputs
    output_path = paths.directory / "world_population_10000bce_2100.svg"
    fig.savefig(
        output_path,
        format="svg",
        dpi=300,
        bbox_inches="tight",
        metadata={"Date": None},  # Remove timestamp for cleaner diffs
    )

    # Optimize SVG for Figma editing
    paths.log.info(f"Saved chart to {output_path}")

    output_path_png = paths.directory / "world_population_10000bce_2100.png"
    fig.savefig(output_path_png, format="png", dpi=300, bbox_inches="tight", transparent=True)
    paths.log.info(f"Saved chart to {output_path_png}")

    plt.close(fig)


if __name__ == "__main__":
    run()
