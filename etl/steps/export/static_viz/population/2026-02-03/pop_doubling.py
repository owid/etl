"""Recreate 'Time it took for the world population to double' chart.

Generates a line chart plotting the number of years it took world population
to double, at each milestone doubling event.  Doubling times are loaded from
the pre-computed garden dataset ``demography/2024-07-18/population_doubling_times``.
Boundary years (historical end, projection end) and the source citation are
inferred from the garden ``demography/2024-07-15/population`` dataset.
"""

import matplotlib
import matplotlib.pyplot as plt
from owid.catalog import Table

from etl.helpers import PathFinder

# Use non-path text so SVGs stay editable in Figma
matplotlib.rcParams["svg.fonttype"] = "none"
# Set deterministic hash for reproducible SVG output
matplotlib.rcParams["svg.hashsalt"] = "owid-static-viz"

paths = PathFinder(__file__)


def run() -> None:
    """Load data, render and save chart."""
    tb_hist_raw, tb_proj_raw = load_world_population()

    tb = load_doubling_times()
    paths.log.info(f"Loaded {len(tb)} population doubling milestones")
    paths.log.info(tb.to_string(index=False))

    source_citation = build_source_citation(tb_hist_raw, tb_proj_raw)
    paths.log.info(f"Source citation: {source_citation}")

    fig = create_visualization(tb, tb_hist_raw, tb_proj_raw, source_citation)

    # Save chart in multiple formats
    paths.export_fig(fig, "pop_doubling", ["png", "svg"], dpi=300, bbox_inches="tight")

    plt.close(fig)


def build_source_citation(tb_historical: Table, tb_projection: Table) -> str:
    # Track seen origins to avoid duplicates
    seen_origins = set()
    source_parts = []
    # Iterate over variables and extract their origins, deduplicating by (producer, title, date)
    variables = [tb_historical["population_historical"], tb_projection["population_projection"]]
    for var in variables:
        for origin in var.metadata.origins:
            key = (origin.attribution_short, origin.title, origin.date_published)
            if key not in seen_origins:
                seen_origins.add(key)
                year = origin.date_published.split("-")[0] if origin.date_published else ""
                source_parts.append(f"{origin.attribution_short} ({year})")
    return "Data sources: " + "; ".join(sorted(source_parts))


def _nice_x_ticks(year_min: int, year_max: int) -> list[int]:
    """Round century ticks that span [year_min, year_max] with padding."""
    lo = (year_min // 100) * 100
    hi = ((year_max + 99) // 100) * 100
    return list(range(lo, hi + 1, 100))


def _fmt_pop(val: float) -> str:
    """Format a population value in billions for labels (e.g. '0.25' or '1')."""
    if val == int(val):
        return str(int(val))
    return f"{val:g}"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_world_population() -> tuple[Table, Table]:
    """Load historical + projection population for World (origins metadata).

    Returns:
        tb_hist_raw     – raw historical Table (for origins metadata)
        tb_proj_raw     – raw projections Table (for origins metadata)
    """
    ds = paths.load_dataset("population")

    tb_hist_raw = ds.read("historical")
    tb_proj_raw = ds.read("projections")

    return tb_hist_raw, tb_proj_raw


# ---------------------------------------------------------------------------
# Data loading – doubling times
# ---------------------------------------------------------------------------


def load_doubling_times() -> Table:
    """Load pre-computed doubling times from the garden dataset.

    Returns a Table with columns:
        year_reached   – year the target population was first reached
        doubling_years – number of years from half-target to target
        from_b         – source population in billions (target / 2)
        to_b           – target population in billions
    """
    ds = paths.load_dataset("population_doubling_times")
    tb = ds.read("population_doubling_times")

    # population_target is in absolute numbers; convert to billions
    tb["to_b"] = tb["population_target"] / 1e9
    tb["from_b"] = tb["to_b"] / 2

    tb = tb.rename(columns={"year": "year_reached", "num_years_to_double": "doubling_years"})
    tb = tb[["year_reached", "doubling_years", "from_b", "to_b"]].sort_values("year_reached").reset_index(drop=True)

    return tb


# ---------------------------------------------------------------------------
# Visualisation
# ---------------------------------------------------------------------------


def create_visualization(
    tb: Table,
    tb_hist_raw: Table,
    tb_proj_raw: Table,
    source_citation: str,
) -> plt.Figure:
    """Build the chart.

    Layout notes:
    - White background, light-grey horizontal grid lines
    - Dark-blue (#0c4387) connected line with square markers
    - Y-axis tick labels include " years" unit
    - X-axis: century ticks derived from the data range
    - First point gets a multi-line callout box
    - Upper-curve points (>= 100 years, excluding first) get a label to the
      right plus a rotated year label below the marker
    - Bottom-right cluster (< 100 years): labels fan out to the right with
      leader lines, ordered smallest-doubling-time at bottom
    - A dashed vertical line at hist_last_year separates historical from
      projected data
    """
    # Infer boundary years from the raw population tables
    # Tables are already reset (no index), so filter by column
    world_hist = tb_hist_raw[tb_hist_raw["country"] == "World"]
    world_proj = tb_proj_raw[tb_proj_raw["country"] == "World"]

    hist_last_year = int(world_hist["year"].max())
    proj_last_year = int(world_proj["year"].max())

    paths.log.info(f"Data boundaries – historical last year: {hist_last_year}, projections last year: {proj_last_year}")

    # --- colours & sizes ---
    line_color = "#0c4387"
    text_color = "#333333"
    grid_color = "#e8e8e8"
    axis_color = "#999999"
    projection_line_color = "#bbbbbb"

    fig, ax = plt.subplots(figsize=(13.65, 9.5))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # --- grid & spines ---
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color=grid_color, linewidth=0.8)
    ax.xaxis.grid(False)
    for spine in ["top", "right", "left"]:
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color(axis_color)
    ax.spines["bottom"].set_linewidth(0.8)

    # --- axes limits & ticks (derived from data) ---
    max_dy = int(tb["doubling_years"].max())
    first_year = int(tb["year_reached"].min())

    x_ticks = _nice_x_ticks(first_year, proj_last_year)
    ax.set_xlim(x_ticks[0] - 10, x_ticks[-1] + 50)
    ax.set_ylim(-50, max_dy + 100)

    ax.set_xticks(x_ticks)
    ax.set_xticklabels([str(y) for y in x_ticks], fontsize=13, color=text_color)
    ax.tick_params(axis="x", length=0)

    # Y-axis ticks at every 100 up to just above the max
    y_tick_max = ((max_dy + 100) // 100) * 100
    y_ticks = list(range(0, int(y_tick_max) + 1, 100))
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([f"{t} years" for t in y_ticks], fontsize=13, color=text_color)
    ax.tick_params(axis="y", length=0)

    # --- projection separator (position inferred from data) ---
    ax.axvline(hist_last_year, color=projection_line_color, linewidth=1, linestyle="--", zorder=1)

    # --- main line + markers ---
    x = tb["year_reached"].values
    y = tb["doubling_years"].values
    ax.plot(x, y, color=line_color, linewidth=2.2, marker="s", markersize=7, markerfacecolor=line_color, zorder=2)

    # --- identify the cluster (doubling_years < 100) and build fan geometry ---
    cluster_mask = tb["doubling_years"] < 100
    n_cluster = int(cluster_mask.sum())

    cluster_df = tb[cluster_mask].copy()
    cluster_df["label_rank"] = cluster_df["doubling_years"].rank(method="first").astype(int) - 1  # 0-based

    cluster_label_x = 2060  # x position where all fan labels start
    cluster_label_y_bot = -30  # y of the bottom-most label
    cluster_label_y_top = 160  # y of the top-most label
    cluster_y_step = (cluster_label_y_top - cluster_label_y_bot) / max(n_cluster - 1, 1)

    # Build a quick lookup: (year_reached, doubling_years) → label_y
    cluster_label_y_map: dict[tuple[int, int], float] = {}
    for _, crow in cluster_df.iterrows():
        rank = int(crow["label_rank"])
        cluster_label_y_map[(int(crow["year_reached"]), int(crow["doubling_years"]))] = (
            cluster_label_y_bot + rank * cluster_y_step
        )

    # ---------------------------------------------------------------------------
    # Label loop
    # ---------------------------------------------------------------------------
    for i, row in tb.iterrows():
        yr = int(row["year_reached"])
        dy = int(row["doubling_years"])
        from_b = row["from_b"]
        to_b = row["to_b"]

        if i == 0:
            # ── first point: rotated year label + callout box ──
            ax.text(yr + 3, dy - 35, str(yr), fontsize=10, color=text_color, rotation=-75, ha="left", va="top")

            callout_text = (
                f"It took {dy} years for the world\n"
                f"population to double – from {_fmt_pop(from_b)} billion\n"
                f"in {int(round(yr - dy))} to {_fmt_pop(to_b)} billion in {yr}."
            )
            ax.annotate(
                callout_text,
                xy=(yr, dy),
                xytext=(yr + 60, dy - 200),
                fontsize=10,
                color=text_color,
                va="top",
                ha="left",
                bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="#cccccc", linewidth=0.8),
                arrowprops=dict(arrowstyle="-", color="#999999", linewidth=0.8),
                annotation_clip=False,
            )

        elif not cluster_mask.iloc[i]:
            # ── upper-curve point (>= 100 years, not first) ──
            ax.text(yr + 3, dy - 30, str(yr), fontsize=10, color=text_color, rotation=-75, ha="left", va="top")

            label = f"{dy} years ({_fmt_pop(from_b)} to {_fmt_pop(to_b)} billion)"
            ax.text(yr + 10, dy + 8, label, fontsize=10, color=text_color, ha="left", va="center")

        else:
            # ── bottom-right cluster: fanned labels with leader lines ──
            label = f"{dy} years ({_fmt_pop(from_b)} to {_fmt_pop(to_b)} billion) – in {yr}"

            label_y = cluster_label_y_map[(yr, dy)]

            ax.annotate(
                label,
                xy=(yr, dy),
                xytext=(cluster_label_x, label_y),
                fontsize=9.5,
                color=text_color,
                ha="left",
                va="center",
                arrowprops=dict(arrowstyle="-", color="#bbbbbb", linewidth=0.7),
                annotation_clip=False,
            )

    # --- title & subtitle (years inferred from data) ---
    fig.suptitle(
        "Time it took for the world population to double",
        x=0.06,
        y=0.95,
        ha="left",
        fontsize=28,
        fontweight="normal",
        color="#111111",
    )
    fig.text(
        0.06,
        0.88,
        f"Historical estimates of the world population until {hist_last_year}"
        f" – and UN projections until {proj_last_year}",
        ha="left",
        fontsize=14,
        color="#555555",
    )

    # --- source footer (built from dataset origins) ---
    fig.text(
        0.06,
        0.015,
        f"{source_citation}\n" "The interactive data visualization is available at OurWorldInData.org.",
        ha="left",
        va="bottom",
        fontsize=9,
        color="#888888",
    )

    fig.subplots_adjust(top=0.82, left=0.07, right=0.92, bottom=0.09)

    return fig
