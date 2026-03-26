"""Create visualization showing life expectancy by age in the United Kingdom.

This code generates a multi-line chart showing total life expectancy given that a
person reached a certain age (conditional life expectancy), for the United Kingdom.

- Age 0 (at birth): long-run data back to 1543 from Zijdeman / HMD
- Ages 10, 15, 25, 45, 65, 80: HMD data from 1922 onwards
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from etl.helpers import PathFinder

# Configure matplotlib for reproducible SVG output
matplotlib.rcParams["svg.fonttype"] = "none"
matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"

paths = PathFinder(__file__)

# Ages available in the dataset (0 comes from at_birth table; rest from life_expectancy table)
AGES = [0, 1, 5, 10, 15, 25, 45, 65, 80]

# Color gradient: dark red at birth → through warm and cool tones → purple at 80
AGE_COLORS = {
    0: "#6B0000",
    1: "#A01010",
    5: "#D03020",
    10: "#E05C20",
    15: "#E89020",
    25: "#C8A800",
    45: "#27AE60",
    65: "#2980B9",
    80: "#6C3483",
}

AGE_LABELS = {
    0: "At birth",
    1: "Age 1",
    5: "Age 5",
    10: "Age 10",
    15: "Age 15",
    25: "Age 25",
    45: "Age 45",
    65: "Age 65",
    80: "Age 80",
}

COUNTRY = "United Kingdom"


def run() -> None:
    """Create life expectancy by age visualization for the United Kingdom.

    Output:
    - SVG and PNG charts
    - Dataset with the plotted data
    """
    ds = paths.load_dataset("life_expectancy")

    # --- Age 0 from long-run at_birth table ---
    tb_birth = ds.read("life_expectancy_at_birth")
    tb_birth = tb_birth[(tb_birth["country"] == COUNTRY)].copy()
    tb_birth = tb_birth[["country", "year", "life_expectancy_0"]].rename(
        columns={"life_expectancy_0": "life_expectancy"}
    )
    tb_birth["age"] = 0

    # --- Ages 1, 5, 10-80 from life_expectancy table ---
    tb_le = ds.read("life_expectancy")
    tb_le = tb_le[(tb_le["country"] == COUNTRY) & (tb_le["sex"] == "total")].copy()
    tb_le = tb_le[["country", "year", "age", "life_expectancy"]]

    # Combine
    tb = pd.concat([tb_birth, tb_le], ignore_index=True)
    tb = tb.sort_values(["age", "year"]).reset_index(drop=True)

    # Build source citation from metadata
    source_text = _build_source_citation(tb_birth, tb_le)

    fig = create_visualization(tb, source_text)

    paths.export_fig(fig, "life_expectancy_by_age_uk", ["svg", "png"], dpi=300, bbox_inches="tight")
    paths.log.info("Saved SVG and PNG")

    plt.close(fig)
    paths.log.info("Visualization complete")


def create_visualization(tb, source_text: str) -> plt.Figure:
    """Create the multi-line life expectancy chart."""
    text_color = "#333333"
    grey_text = "#666666"
    grid_color = "#EEEEEE"
    axis_grey = "#999999"

    fig, ax = plt.subplots(figsize=(16, 13))

    ax.set_axisbelow(True)
    ax.grid(axis="y", color=grid_color, linewidth=1)
    ax.grid(axis="x", visible=False)

    # Collect last-point y values for label placement
    last_points = {}  # age -> (year, y_value)

    # Plot one line per age group
    for age in AGES:
        tb_age = tb[tb["age"] == age].sort_values("year")
        if tb_age.empty:
            continue

        color = AGE_COLORS[age]
        ax.plot(
            tb_age["year"],
            tb_age["life_expectancy"],
            color=color,
            linewidth=2.0,
            zorder=5,
        )

        last = tb_age.iloc[-1]
        last_points[age] = (int(last["year"]), float(last["life_expectancy"]))

    # Place labels with bidirectional spreading to avoid overlap
    MIN_SPACING = 2.8
    sorted_ages = sorted(last_points.keys(), key=lambda a: last_points[a][1])
    adjusted_y = [last_points[age][1] for age in sorted_ages]

    # Iteratively push labels apart (both directions) until no overlaps remain
    for _ in range(100):
        moved = False
        for i in range(1, len(adjusted_y)):
            gap = adjusted_y[i] - adjusted_y[i - 1]
            if gap < MIN_SPACING:
                push = (MIN_SPACING - gap) / 2
                adjusted_y[i - 1] -= push
                adjusted_y[i] += push
                moved = True
        if not moved:
            break

    adjusted_y_map = {age: y for age, y in zip(sorted_ages, adjusted_y)}

    label_x = max(v[0] for v in last_points.values()) + 2
    for age in AGES:
        if age not in last_points:
            continue
        # Draw a faint connector line from label to line end when they diverge
        line_end_y = last_points[age][1]
        label_y = adjusted_y_map[age]
        line_end_x = last_points[age][0]
        if abs(label_y - line_end_y) > 1.0:
            ax.plot(
                [line_end_x, label_x - 0.5],
                [line_end_y, label_y],
                color=AGE_COLORS[age],
                linewidth=0.6,
                alpha=0.5,
                zorder=4,
            )
        ax.text(
            label_x,
            label_y,
            AGE_LABELS[age],
            color=AGE_COLORS[age],
            fontsize=10,
            va="center",
            ha="left",
        )

    # Axis limits
    year_min = int(tb["year"].min())
    year_max = int(tb["year"].max())
    ax.set_xlim(year_min - 10, year_max + 30)
    ax.set_ylim(15, 100)

    # X-axis ticks
    tick_start = int(np.ceil(year_min / 50) * 50)
    x_ticks = list(range(tick_start, year_max + 1, 50))
    # Add fine ticks for the last 100 years
    fine_ticks = list(range(1950, year_max + 1, 25))
    x_ticks = sorted(set(x_ticks + fine_ticks))
    if year_max not in x_ticks:
        x_ticks.append(year_max)
    ax.set_xticks(x_ticks)
    ax.tick_params(axis="x", colors=grey_text, labelsize=11, length=0)

    ax.set_yticks(range(20, 101, 10))
    ax.set_yticklabels([str(y) for y in range(20, 101, 10)], fontsize=11, color=grey_text)
    ax.tick_params(axis="y", length=0, left=False)

    # Spines
    for s in ["top", "right", "left"]:
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(axis_grey)
    ax.spines["bottom"].set_linewidth(1)

    # Annotation: Spanish flu dip on at-birth line
    tb_birth = tb[tb["age"] == 0].sort_values("year")
    flu_data = tb_birth[tb_birth["year"] == 1918]
    if not flu_data.empty:
        flu_le = float(flu_data["life_expectancy"].iloc[0])
        ax.annotate(
            "Spanish flu\npandemic, 1918",
            xy=(1918, flu_le),
            xytext=(1895, flu_le - 14),
            fontsize=10,
            color=grey_text,
            ha="center",
            va="top",
            arrowprops=dict(arrowstyle="->", color=grey_text, lw=1.2, connectionstyle="arc3,rad=0.2"),
        )

    # Annotation: WWII dip on at-birth line — place text to the right to avoid flu overlap
    ww2_data = tb_birth[(tb_birth["year"] >= 1939) & (tb_birth["year"] <= 1945)]
    if not ww2_data.empty:
        ww2_min_idx = ww2_data["life_expectancy"].idxmin()
        ww2_year = int(ww2_data.loc[ww2_min_idx, "year"])
        ww2_le = float(ww2_data.loc[ww2_min_idx, "life_expectancy"])
        ax.annotate(
            "World War II",
            xy=(ww2_year, ww2_le),
            xytext=(ww2_year + 18, ww2_le - 10),
            fontsize=10,
            color=grey_text,
            ha="center",
            va="top",
            arrowprops=dict(arrowstyle="->", color=grey_text, lw=1.2, connectionstyle="arc3,rad=-0.2"),
        )

    # Note explaining conditional life expectancy lines start later — top left, below subtitle area
    ax.text(
        year_min + 5,
        95,
        "Data for ages 1+ available from 1922 onwards.",
        fontsize=9,
        color=grey_text,
        ha="left",
        va="top",
        style="italic",
    )

    # Y-axis label
    ax.set_ylabel("Life expectancy (years)", fontsize=12, color=grey_text, labelpad=10)

    # Title and subtitle
    fig.suptitle(
        f"Life expectancy by age in the {COUNTRY}, {year_min}–{year_max}",
        x=0.06,
        y=0.97,
        ha="left",
        fontsize=26,
        fontweight="normal",
        color=text_color,
    )
    fig.text(
        0.06,
        0.915,
        "Shown is the total life expectancy given that a person reached a certain age.",
        fontsize=13,
        color=grey_text,
        ha="left",
        va="top",
    )

    # Source note
    fig.text(
        0.06,
        0.03,
        f"{source_text}\n"
        "OurWorldInData.org – Research and data to make progress against the world's largest problems.",
        fontsize=9,
        color=grey_text,
        ha="left",
        va="bottom",
    )

    fig.subplots_adjust(top=0.88, left=0.07, right=0.86, bottom=0.12)

    return fig


def _build_source_citation(tb_birth, tb_le) -> str:
    """Build source citation text from table metadata."""
    try:
        all_origins = []
        for col_name, tb in [("life_expectancy_0", tb_birth), ("life_expectancy", tb_le)]:
            # After rename/concat, column is 'life_expectancy'; check original tables
            check_col = "life_expectancy" if col_name not in tb.columns else col_name
            if check_col in tb.columns:
                col = tb[check_col]
                if hasattr(col, "metadata") and hasattr(col.metadata, "origins") and col.metadata.origins:
                    all_origins.extend(col.metadata.origins)

        unique_origins = {}
        for origin in all_origins:
            key = (
                getattr(origin, "attribution_short", None),
                getattr(origin, "title", None),
                getattr(origin, "date_published", None),
            )
            if key not in unique_origins:
                unique_origins[key] = origin

        source_parts = []
        for (producer, title, date_pub), origin in sorted(
            unique_origins.items(), key=lambda x: x[0][0] or ""
        ):
            year = date_pub.split("-")[0] if date_pub else ""
            if producer:
                source_parts.append(f"{producer} ({year})" if year else producer)

        return "Data sources: " + "; ".join(source_parts) if source_parts else "Data sources: HMD; UN WPP; Zijdeman et al."
    except Exception:
        return "Data sources: HMD; UN WPP; Zijdeman et al."


if __name__ == "__main__":
    run()
