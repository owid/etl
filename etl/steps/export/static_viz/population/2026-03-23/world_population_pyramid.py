"""Create world population pyramid visualization (1950-2100).

Generates a layered population pyramid showing the age-sex distribution
of the world population from 1950 to 2100, combining UN WPP historical
estimates with medium-variant projections.
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from etl.helpers import PathFinder

matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"
matplotlib.rcParams["svg.fonttype"] = "none"

paths = PathFinder(__file__)

# Single-year age groups for the pyramid (bottom to top)
AGE_GROUPS = [str(i) for i in range(100)]
# Y-axis position for each age (integer ages 0–99)
AGE_MIDPOINTS = list(range(100))

# Fixed decades to display (historical + projection anchors)
# The latest historical year is inserted dynamically from the data
FIXED_HISTORICAL_YEARS = [1950, 1960, 1970, 1980, 1990, 2000]
PROJECTION_YEARS = [2050, 2075, 2100]

# Color palette: high-contrast progression dark navy → teal → green → lime → amber
# Each decade clearly distinguishable. Latest historical year gets the "yellow" slot.
YEAR_COLORS_FIXED = {
    1950: "#002b5c",  # deep navy
    1960: "#0a5688",  # cobalt blue
    1970: "#0f88a0",  # steel teal
    1980: "#10a88c",  # medium teal
    1990: "#18c070",  # sea green
    2000: "#58c83c",  # bright green
    2050: "#f0b000",  # amber
    2075: "#f07020",  # orange
    2100: "#e83820",  # red-orange
}
LATEST_HISTORICAL_COLOR = "#d4d000"  # yellow — used for the latest estimates year

# Label placement: approximate age where each year's contour is most visible
# The latest historical year is inserted dynamically
LABEL_YEAR_AGES_FIXED = {
    1950: 5,
    1960: 10,
    1970: 17,
    1980: 25,
    1990: 35,
    2000: 43,
    2050: 70,
    2075: 78,
    2100: 85,
}
LATEST_HISTORICAL_LABEL_AGE = 60


def get_latest_historical_year(ds_wpp) -> int:
    """Derive the latest year covered by estimates (not projections) from the data."""
    tb = ds_wpp.read("population")
    return int(tb.loc[(tb["country"] == "World") & (tb["variant"] == "estimates"), "year"].max())


def build_display_config(latest_historical_year: int) -> tuple[list[int], dict[int, str], dict[int, int]]:
    """Build display years, colors, and label ages with the latest historical year inserted."""
    display_years = FIXED_HISTORICAL_YEARS + [latest_historical_year] + PROJECTION_YEARS
    year_colors = {**YEAR_COLORS_FIXED, latest_historical_year: LATEST_HISTORICAL_COLOR}
    label_year_ages = {**LABEL_YEAR_AGES_FIXED, latest_historical_year: LATEST_HISTORICAL_LABEL_AGE}
    return display_years, year_colors, label_year_ages


def load_pyramid_data(ds_wpp, display_years: list[int]) -> pd.DataFrame:
    """Load and prepare population data for the pyramid (in millions)."""
    tb = ds_wpp.read("population")

    mask = (
        (tb["country"] == "World")
        & (tb["age"].isin(AGE_GROUPS))
        & (tb["sex"].isin(["male", "female"]))
        & (tb["variant"].isin(["estimates", "medium"]))
        & (tb["year"].isin(display_years))
    )
    tb = tb[mask][["year", "sex", "age", "population"]].copy()
    tb["population"] = tb["population"] / 1e6  # convert to millions

    tb_wide = tb.pivot_table(index=["year", "age"], columns="sex", values="population").reset_index()
    tb_wide.columns.name = None
    return tb_wide


def load_median_ages(ds_wpp, display_years: list[int]) -> dict[int, float]:
    """Load median age for the World for display years."""
    tb = ds_wpp.read("median_age")
    mask = (
        (tb["country"] == "World")
        & (tb["variant"].isin(["estimates", "medium"]))
        & (tb["year"].isin(display_years))
        & (tb["sex"] == "all")
        & (tb["age"] == "all")
    )
    sub = tb[mask][["year", "median_age"]].drop_duplicates("year")
    return dict(zip(sub["year"], sub["median_age"]))


def build_source_citation(ds_wpp) -> str:
    """Build source citation from dataset metadata."""
    tb = ds_wpp.read("population")
    col = tb["population"]
    if not hasattr(col.metadata, "origins") or not col.metadata.origins:
        raise ValueError("No origins found in population column metadata")

    unique_origins = {}
    for origin in col.metadata.origins:
        key = (origin.attribution_short, origin.date_published)
        if key not in unique_origins:
            unique_origins[key] = origin

    parts = []
    for (producer, date_pub), _ in sorted(unique_origins.items()):
        year = date_pub.split("-")[0] if date_pub else ""
        parts.append(f"{producer} ({year})")
    return "Data source: " + "; ".join(parts)



def create_visualization(
    tb: pd.DataFrame,
    median_ages: dict[int, float],
    source_text: str,
    display_years: list[int],
    year_colors: dict[int, str],
    label_year_ages: dict[int, int],
    latest_historical_year: int,
) -> plt.Figure:
    """Create the layered population pyramid figure."""

    axis_grey = "#bbbbbb"
    text_grey = "#666666"
    text_dark = "#222222"

    age_midpts = np.array(AGE_MIDPOINTS)  # y-coordinates

    x_max = 70  # fixed axis range: 0–70 million per side

    fig, ax = plt.subplots(figsize=(16, 12))
    fig.patch.set_alpha(0)
    ax.patch.set_alpha(0)

    # Draw layers back-to-front (oldest first so newest sits on top)
    for year in reversed(display_years):
        year_data = tb[tb["year"] == year].copy()
        if year_data.empty:
            continue

        # Sort by age (numeric)
        year_data = year_data.copy()
        year_data["age_idx"] = year_data["age"].astype(int)
        year_data = year_data.sort_values("age_idx")

        male_vals = year_data["male"].values.astype(float)
        female_vals = year_data["female"].values.astype(float)

        color = year_colors[year]

        # Taper to 0 at the top
        y_pts = np.concatenate([age_midpts, [102]])
        male_pts = np.concatenate([male_vals, [0]])
        female_pts = np.concatenate([female_vals, [0]])

        ax.fill_betweenx(y_pts, 0, -male_pts, color=color, alpha=0.9, linewidth=0)
        ax.fill_betweenx(y_pts, 0, female_pts, color=color, alpha=0.9, linewidth=0)
        ax.plot(-male_pts, y_pts, color=color, linewidth=0.8, alpha=1)
        ax.plot(female_pts, y_pts, color=color, linewidth=0.8, alpha=1)

        # Year labels on contours for selected years
        if year in label_year_ages:
            label_age = label_year_ages[year]
            # Find the nearest age group to the desired label age
            nearest_idx = np.argmin(np.abs(age_midpts - label_age))
            label_y = age_midpts[nearest_idx]
            label_x_r = female_vals[nearest_idx]
            label_x_l = male_vals[nearest_idx]

            ax.text(
                label_x_r + x_max * 0.025,
                label_y,
                str(year),
                ha="left",
                va="center",
                fontsize=9,
                color=color,
                fontweight="bold",
                clip_on=False,
            )
            ax.text(
                -(label_x_l + x_max * 0.025),
                label_y,
                str(year),
                ha="right",
                va="center",
                fontsize=9,
                color=color,
                fontweight="bold",
                clip_on=False,
            )

    # Axes setup
    ax.set_xlim(-x_max * 1.1, x_max * 1.1)
    ax.set_ylim(-2, 103)

    # X-axis at TOP (mirror of standard)
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position("top")

    x_tick_vals = np.arange(0, x_max + 1, 10)
    all_x_ticks = np.concatenate([-x_tick_vals[1:][::-1], x_tick_vals])
    ax.set_xticks(all_x_ticks)
    ax.set_xticklabels(
        [f"{int(abs(t))} Million" if t != 0 else "" for t in all_x_ticks],
        fontsize=10,
        color=text_grey,
    )
    ax.tick_params(axis="x", length=0, pad=5)

    # Y-axis (age)
    y_ticks = list(range(0, 100, 10))
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([f"{y} years" for y in y_ticks], fontsize=11, color=text_grey)
    ax.tick_params(axis="y", length=0, pad=6, left=True, right=False)

    # Grid — vertical lines from top to bottom
    ax.set_axisbelow(True)
    ax.grid(axis="x", color="#e8e8e8", linewidth=1, zorder=0)
    ax.grid(axis="y", visible=False)

    # Spines
    for s in ["left", "right", "bottom"]:
        ax.spines[s].set_visible(False)
    ax.spines["top"].set_color(axis_grey)
    ax.spines["top"].set_linewidth(1)

    # Center divider
    ax.axvline(0, color=axis_grey, linewidth=1, zorder=5)

    # "Men" / "Women" labels at the bottom
    ax.text(-x_max * 0.5, -1.5, "Men", ha="center", va="top", fontsize=14, color=text_grey)
    ax.text(x_max * 0.5, -1.5, "Women", ha="center", va="top", fontsize=14, color=text_grey)

    # Right-side median age legend
    label_x = x_max * 1.12
    legend_years = [1950, 2000, latest_historical_year, 2050, 2075, 2100]
    legend_y_start = 100
    ax.text(
        label_x,
        legend_y_start + 3,
        "Median age",
        ha="left",
        va="bottom",
        fontsize=10,
        color=text_grey,
        style="italic",
        clip_on=False,
    )
    for i, year in enumerate(legend_years):
        if year in median_ages:
            med = median_ages[year]
            color = year_colors.get(year, text_grey)
            bold = "bold" if year in (1950, latest_historical_year, 2100) else "normal"
            ax.text(
                label_x,
                legend_y_start - i * 7.5,
                f"{year}:  {med:.0f} years",
                ha="left",
                va="center",
                fontsize=10,
                color=color,
                fontweight=bold,
                clip_on=False,
            )

    # Title
    fig.suptitle(
        "The Demography of the World Population from 1950 to 2100",
        x=0.06,
        y=0.97,
        ha="left",
        fontsize=22,
        fontweight="normal",
        color=text_dark,
    )
    ax.set_title(
        "Age distribution by sex – historical estimates (UN WPP) and medium-variant projections to 2100",
        loc="left",
        pad=36,
        fontsize=11,
        color=text_grey,
    )

    # Source + license
    fig.text(
        0.06,
        0.02,
        f"{source_text}\nOurWorldInData.org",
        fontsize=9,
        color=text_grey,
        ha="left",
        va="bottom",
    )

    fig.subplots_adjust(top=0.87, left=0.07, right=0.78, bottom=0.07)

    return fig


def run() -> None:
    """Create world population pyramid visualization (SVG + PNG)."""
    ds_wpp = paths.load_dataset("un_wpp_single_age")

    latest_historical_year = get_latest_historical_year(ds_wpp)
    display_years, year_colors, label_year_ages = build_display_config(latest_historical_year)

    tb = load_pyramid_data(ds_wpp, display_years)
    median_ages = load_median_ages(ds_wpp, display_years)
    source_text = build_source_citation(ds_wpp)

    fig = create_visualization(
        tb, median_ages, source_text, display_years, year_colors, label_year_ages, latest_historical_year
    )

    paths.export_fig(
        fig,
        "world_population_pyramid",
        ["svg", "png"],
        dpi=300,
        bbox_inches="tight",
        transparent=True,
    )
    plt.close(fig)


if __name__ == "__main__":
    run()
