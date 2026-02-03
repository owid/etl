"""Recreate 'Time it took for the world population to double' chart.

Generates a line chart plotting the number of years it took world population
to double, at each milestone doubling event, using historical data and
UN projections from the garden/demography population dataset.

Milestone doublings are defined by (from_pop, to_pop) pairs at round population
thresholds (e.g. 0.5 → 1 billion).  For each pair the script interpolates the
exact years at which each threshold was crossed, then plots
(year_reached, years_to_double) as a connected line with square markers.

All boundary years (historical end, projection end) and the source citation are
inferred directly from the dataset rather than being hard-coded.
"""

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import structlog
from owid.catalog import Dataset, Table
from scipy.interpolate import interp1d

from etl import paths as etl_paths

# Use non-path text so SVGs stay editable in Figma
matplotlib.rcParams["svg.fonttype"] = "none"

CURRENT_DIR = Path(__file__).parent
SHORT_NAME = "pop_doubling"

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Milestone definitions  (from_billions, to_billions)
# Sorted by from_pop so that year_reached is chronological on the x-axis.
# ---------------------------------------------------------------------------
DOUBLING_PAIRS: list[tuple[float, float]] = [
    (0.25, 0.50),
    (0.50, 1.0),
    (1.0, 2.0),
    (1.5, 3.0),
    (2.0, 4.0),
    (2.5, 5.0),
    (4.0, 8.0),
    (5.0, 10.0),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_source_citation(tb_historical: Table, tb_projection: Table) -> str:
    """Build source citation from column-level origins metadata.

    Mirrors the approach used in world_population_growth.py – collects origins
    from the two key population columns and deduplicates them.
    """
    all_origins = []
    for col_name, tb in [("population_historical", tb_historical), ("population_projection", tb_projection)]:
        col = tb[col_name]
        if hasattr(col.metadata, "origins") and col.metadata.origins:
            all_origins.extend(col.metadata.origins)

    # Deduplicate by (producer, title, date)
    unique_origins: dict[tuple, object] = {}
    for origin in all_origins:
        key = (origin.attribution_short, origin.title, origin.date_published)
        if key not in unique_origins:
            unique_origins[key] = origin

    source_parts = []
    for (producer, _title, date_pub), _origin in sorted(unique_origins.items()):
        year = date_pub.split("-")[0] if date_pub else ""
        source_parts.append(f"{producer} ({year})")

    return "Data sources: " + "; ".join(source_parts)


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


def load_world_population() -> tuple[pd.DataFrame, int, int, Table, Table]:
    """Load and merge historical + projection population for World.

    Returns:
        combined        – DataFrame with ['year', 'population'], sorted, deduped
                          (historical takes priority where they overlap).
        hist_last_year  – last year present in the historical series
        proj_last_year  – last year present in the projections series
        tb_hist_raw     – raw historical Table (for origins metadata)
        tb_proj_raw     – raw projections Table (for origins metadata)
    """
    ds = Dataset(etl_paths.DATA_DIR / "garden/demography/2024-07-15/population")

    tb_hist_raw = ds["historical"]
    tb_proj_raw = ds["projections"]

    tb_hist = tb_hist_raw.reset_index()
    tb_proj = tb_proj_raw.reset_index()

    world_hist = tb_hist[tb_hist["country"] == "World"][["year", "population_historical"]].copy()
    world_hist.columns = ["year", "population"]

    world_proj = tb_proj[tb_proj["country"] == "World"][["year", "population_projection"]].copy()
    world_proj.columns = ["year", "population"]

    # Boundary years – inferred from the data
    hist_last_year = int(world_hist["year"].max())
    proj_last_year = int(world_proj["year"].max())

    # Concat; historical rows come first so drop_duplicates keeps them
    combined = pd.concat([world_hist, world_proj]).drop_duplicates("year").sort_values("year").reset_index(drop=True)

    return combined, hist_last_year, proj_last_year, tb_hist_raw, tb_proj_raw


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------


def compute_doublings(combined: pd.DataFrame) -> pd.DataFrame:
    """Interpolate the year each population threshold was crossed and compute doubling durations.

    Args:
        combined: DataFrame with 'year' and 'population' columns (sorted, unique years).

    Returns:
        DataFrame with columns:
            year_reached   – (int)   year the *target* population was first reached
            doubling_years – (int)   number of years from source to target
            from_b         – (float) source population in billions
            to_b           – (float) target population in billions
    """
    years = combined["year"].values.astype(float)
    pops = combined["population"].values.astype(float)

    # Inverse interpolation: population → year  (population is monotonically increasing)
    f_pop_to_year = interp1d(pops, years, kind="linear", bounds_error=False)

    rows = []
    for from_b, to_b in DOUBLING_PAIRS:
        from_p, to_p = from_b * 1e9, to_b * 1e9

        if to_p > pops.max():
            log.warning(f"Target {to_b}B exceeds max population in dataset ({pops.max()/1e9:.2f}B) – skipping")
            continue

        yr_from = float(f_pop_to_year(from_p))
        yr_to = float(f_pop_to_year(to_p))

        if np.isnan(yr_from) or np.isnan(yr_to):
            log.warning(f"Could not interpolate years for {from_b}B → {to_b}B – skipping")
            continue

        rows.append(
            {
                "year_reached": int(round(yr_to)),
                "doubling_years": int(round(yr_to - yr_from)),
                "from_b": from_b,
                "to_b": to_b,
            }
        )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Visualisation
# ---------------------------------------------------------------------------


def create_visualization(
    df: pd.DataFrame,
    hist_last_year: int,
    proj_last_year: int,
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
    max_dy = int(df["doubling_years"].max())
    first_year = int(df["year_reached"].min())

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
    x = df["year_reached"].values
    y = df["doubling_years"].values
    ax.plot(x, y, color=line_color, linewidth=2.2, marker="s", markersize=7, markerfacecolor=line_color, zorder=2)

    # --- identify the cluster (doubling_years < 100) and build fan geometry ---
    cluster_mask = df["doubling_years"] < 100
    n_cluster = int(cluster_mask.sum())

    cluster_df = df[cluster_mask].copy()
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
    for i, row in df.iterrows():
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
        f"{source_citation}\n"
        "The interactive data visualization is available at OurWorldInData.org.",
        ha="left",
        va="bottom",
        fontsize=9,
        color="#888888",
    )

    fig.subplots_adjust(top=0.82, left=0.07, right=0.92, bottom=0.09)

    return fig


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run() -> None:
    """Entry point: load data, compute doublings, render and save chart."""
    combined, hist_last_year, proj_last_year, tb_hist_raw, tb_proj_raw = load_world_population()

    log.info(
        f"Data boundaries – historical last year: {hist_last_year}, "
        f"projections last year: {proj_last_year}"
    )

    df = compute_doublings(combined)
    log.info(f"Computed {len(df)} population doubling milestones")
    log.info(df.to_string(index=False))

    source_citation = build_source_citation(tb_hist_raw, tb_proj_raw)
    log.info(f"Source citation: {source_citation}")

    fig = create_visualization(df, hist_last_year, proj_last_year, source_citation)

    # --- save PNG ---
    png_path = CURRENT_DIR / "pop_doubling.png"
    fig.savefig(png_path, format="png", dpi=150, bbox_inches="tight")
    log.info(f"Saved chart to {png_path}")

    # --- save SVG ---
    svg_path = CURRENT_DIR / "pop_doubling.svg"
    fig.savefig(svg_path, format="svg", dpi=150, bbox_inches="tight", metadata={"Date": None})
    log.info(f"Saved chart to {svg_path}")

    plt.close(fig)
