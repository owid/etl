"""Recreate the 'Expected height of boys and girls' growth-curve chart.

Draws one panel per sex, each showing nested percentile bands from the WHO growth
reference standards, the median, and the -2 SD stunting threshold. Each panel also
carries a faint copy of the other sex's median, so the crossover in early adolescence
stays visible once boys and girls are split apart.

Replaces the hand-drawn 'Expected Healthy Growth Curves for Boys and Girls' image used
on the human-height topic page and the stunting-definition article.
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import to_rgb
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from owid.catalog import Table

from etl.helpers import PathFinder

# Use non-path text so SVGs stay editable in Figma
matplotlib.rcParams["svg.fonttype"] = "none"
# Set deterministic hash for reproducible SVG output
matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"

paths = PathFinder(__file__)

# One panel per sex, with the base color its curves are drawn in.
PANELS = {"Boys": "#b13507", "Girls": "#286bbb"}

# Nested percentile bands, drawn widest first so the narrower ones sit on top. Each is
# (lower column, upper column, how far the fill is blended towards white, legend label).
BANDS = [
    ("height_percentile_3", "height_percentile_97", 0.86, "Middle 94%"),
    ("height_percentile_10", "height_percentile_90", 0.68, "Middle 80%"),
    ("height_percentile_25", "height_percentile_75", 0.46, "Middle 50%"),
]

MEDIAN_COLUMN = "height_percentile_50"
STUNTING_COLUMN = "height_sd_minus_2"

TEXT_COLOR = "#333333"
MUTED_COLOR = "#777777"
GRID_COLOR = "#e8e8e8"
GHOST_COLOR = "#999999"

TITLE = "Expected height of boys and girls, from birth to age 19"


def run() -> None:
    """Load data, render and save chart."""
    tb = load_growth_reference()
    paths.log.info(f"Loaded {len(tb)} rows covering ages {tb['age_years'].min():.1f}-{tb['age_years'].max():.1f}")

    source_citation = build_source_citation(tb)
    paths.log.info(f"Source citation: {source_citation}")

    breaks = find_discontinuities(tb)
    paths.log.info(f"Steps down in the median at ages: {[round(age, 2) for age in breaks]}")

    fig = create_visualization(tb, source_citation, breaks)

    # Save chart in multiple formats
    paths.export_fig(fig, "height_for_age", ["png", "svg"], dpi=300, bbox_inches="tight")

    plt.close(fig)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_growth_reference() -> Table:
    """Load the spliced WHO height-for-age reference from garden."""
    ds = paths.load_dataset("height_for_age")
    return ds.read("height_for_age")


def build_source_citation(tb: Table) -> str:
    """Build the source footer from the origins attached to the median indicator."""
    seen = set()
    parts = []
    for origin in tb[MEDIAN_COLUMN].metadata.origins:
        # Origin titles carry the indicator after a colon; the product name is enough here.
        product = origin.title.split(":")[0].strip()
        year = origin.date_published.split("-")[0] if origin.date_published else ""
        key = (product, year)
        if key not in seen:
            seen.add(key)
            parts.append(f"{product} ({year})")
    return "Data sources: " + "; ".join(parts)


def find_crossover(tb: Table) -> tuple[float, float]:
    """Return the age range, in years, over which girls are taller than boys on average.

    The subtitle states this as one span, so check that it really is a single contiguous
    stretch. Two separate windows would otherwise be reported as one wide range that
    includes ages where boys are in fact taller.
    """
    medians = tb.pivot(index="age_years", columns="sex", values=MEDIAN_COLUMN).sort_index()
    taller = np.flatnonzero((medians["Girls"] > medians["Boys"]).to_numpy())
    assert len(taller) > 0, "Girls are never taller than boys, so the crossover sentence does not apply."
    assert (np.diff(taller) == 1).all(), (
        "Girls are taller than boys over more than one separate age range, which the subtitle "
        "would collapse into a single span."
    )
    ages = medians.index.to_numpy()
    return float(ages[taller[0]]), float(ages[taller[-1]])


def find_discontinuities(tb: Table) -> list[float]:
    """Return the ages, in years, where the median steps down.

    There are two, both in the source data: the switch from lying to standing measurement
    at age 2, and the join between WHO's two products at age 5. Reading them off the data
    rather than hardcoding them keeps the footnote honest if the source ever changes.
    """
    ages = set()
    for _, tb_sex in tb.groupby("sex", observed=True):
        tb_sex = tb_sex.sort_values("age_days")
        median = tb_sex[MEDIAN_COLUMN].to_numpy()
        years = tb_sex["age_years"].to_numpy()
        for i in np.flatnonzero(np.diff(median) < 0):
            ages.add(round(float(years[i + 1]), 1))
    return sorted(ages)


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------


def tint(color: str, weight: float) -> tuple[float, float, float]:
    """Blend a color towards white. weight=0 keeps it, weight=1 turns it white."""
    r, g, b = to_rgb(color)
    return (r + (1 - r) * weight, g + (1 - g) * weight, b + (1 - b) * weight)


def create_visualization(tb: Table, source_citation: str, breaks: list[float]) -> plt.Figure:
    """Build the two-panel growth-curve chart.

    Layout notes:
    - One panel per sex, sharing a y-axis, nested percentile bands light to dark
    - Median drawn solid on top; the other sex's median repeated as a faint dashed line
    - Dashed -2 SD line marks the stunting threshold
    - Shared legend under the subtitle, so neither panel carries an inset key
    - Axis limits, ticks and footnote ages all derived from the data
    """
    age_max = float(tb["age_years"].max())
    height_max = float(tb["height_percentile_99_9"].max())

    medians = {}
    for sex, tb_sex in tb.groupby("sex", observed=True):
        tb_sex = tb_sex.sort_values("age_days")
        medians[sex] = (tb_sex["age_years"].to_numpy(), tb_sex[MEDIAN_COLUMN].to_numpy())

    fig, axes = plt.subplots(1, 2, figsize=(14, 8), sharey=True)
    fig.patch.set_facecolor("white")

    for ax, (sex, color) in zip(axes, PANELS.items()):
        tb_sex = tb[tb["sex"] == sex].sort_values("age_days")
        age = tb_sex["age_years"].to_numpy()

        ax.set_facecolor("white")
        ax.set_axisbelow(True)
        ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.8)
        ax.xaxis.grid(False)
        for spine in ["top", "right", "left"]:
            ax.spines[spine].set_visible(False)
        ax.spines["bottom"].set_color("#999999")
        ax.spines["bottom"].set_linewidth(0.8)

        # --- nested percentile bands, widest first ---
        for lower, upper, weight, _ in BANDS:
            ax.fill_between(
                age,
                tb_sex[lower].to_numpy(),
                tb_sex[upper].to_numpy(),
                facecolor=tint(color, weight),
                linewidth=0,
                zorder=2,
            )

        # --- the other sex's median, so the crossover stays visible in both panels ---
        for other_sex, (other_age, other_median) in medians.items():
            if other_sex == sex:
                continue
            ax.plot(other_age, other_median, color=GHOST_COLOR, linewidth=1.2, linestyle=(0, (4, 3)), zorder=6)

        # --- stunting threshold ---
        ax.plot(
            age,
            tb_sex[STUNTING_COLUMN].to_numpy(),
            color=color,
            linewidth=1.1,
            linestyle=(0, (2, 2)),
            zorder=4,
        )

        # --- median ---
        ax.plot(age, tb_sex[MEDIAN_COLUMN].to_numpy(), color=color, linewidth=2.4, zorder=5)

        # --- panel title, in the panel's own color ---
        ax.text(0.2, height_max, sex, fontsize=20, color=color, ha="left", va="top")

        ax.set_xlim(-0.45, age_max + 0.45)
        ax.set_ylim(38, height_max + 4)
        ax.set_xticks([0, 2, 5, 10, 15, int(age_max)])
        ax.set_xticklabels(["Birth", "2", "5", "10", "15", f"{int(age_max)}"], fontsize=12, color=TEXT_COLOR)
        ax.tick_params(axis="x", length=0)
        ax.tick_params(axis="y", length=0, labelsize=12, labelcolor=TEXT_COLOR)
        ax.set_xlabel("Age in years", fontsize=13, color=TEXT_COLOR, labelpad=10)

    axes[0].set_yticks(range(40, int(height_max) + 5, 20))
    axes[0].set_yticklabels([f"{t} cm" for t in range(40, int(height_max) + 5, 20)], fontsize=12, color=TEXT_COLOR)

    # --- title & subtitle ---
    fig.suptitle(TITLE, x=0.045, y=1.13, ha="left", fontsize=26, color="#111111")
    crossover_start, crossover_end = find_crossover(tb)
    fig.text(
        0.045,
        1.075,
        "The bands show the range of heights among children of the same age in the World Health Organization's growth reference\n"
        "population: the middle 50% spans the 25th to 75th percentile, the middle 80% the 10th to 90th, and the middle 94% the 3rd\n"
        f"to 97th. Girls are taller than boys, on average, between the ages of about {crossover_start:.0f} and {crossover_end:.0f}.",
        ha="left",
        va="top",
        fontsize=13,
        color="#555555",
    )

    # --- shared legend, so neither panel needs an inset key ---
    handles = [
        Patch(facecolor=tint("#666666", weight), edgecolor="#cccccc", linewidth=0.6, label=label)
        for _, _, weight, label in reversed(BANDS)
    ]
    handles += [
        Line2D([0], [0], color="#666666", linewidth=2.4, label="Median height"),
        Line2D([0], [0], color=GHOST_COLOR, linewidth=1.2, linestyle=(0, (4, 3)), label="Median for the other sex"),
        Line2D(
            [0], [0], color="#666666", linewidth=1.1, linestyle=(0, (2, 2)), label="Stunted below this line (-2 SD)"
        ),
    ]
    fig.legend(
        handles=handles,
        loc="upper left",
        bbox_to_anchor=(0.045, 0.975),
        ncol=6,
        frameon=False,
        fontsize=10.5,
        labelcolor=TEXT_COLOR,
        handlelength=1.8,
        columnspacing=1.8,
    )

    # --- footnote & source ---
    footnote = (
        f"The curves step down slightly at age {breaks[0]:.0f}, where height starts being measured standing up rather "
        f"than lying down, and at age {breaks[1]:.0f}, where WHO's standards for\nunder-fives give way to its reference "
        "for older children. Both steps are in the original data.\n"
        "The under-fives standards show how children grow in good conditions; the reference for older children describes "
        "how an earlier sample did grow."
    )
    fig.text(0.045, 0.02, footnote, ha="left", va="top", fontsize=9.5, color=MUTED_COLOR)
    fig.text(
        0.045,
        -0.075,
        f"{source_citation}\n"
        "This is a visualization from OurWorldInData.org, where you find data and research on how the world is changing.",
        ha="left",
        va="top",
        fontsize=9,
        color="#888888",
    )

    fig.subplots_adjust(top=0.88, bottom=0.14, wspace=0.1)

    return fig
