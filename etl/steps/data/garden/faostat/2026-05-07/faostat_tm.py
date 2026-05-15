"""FAOSTAT garden step for the Detailed Trade Matrix (faostat_tm)."""

import pandas as pd
import structlog
from owid.catalog import Table

from etl.helpers import PathFinder

log = structlog.get_logger()
paths = PathFinder(__file__)

# Map raw meadow column names → cleaner garden names.
COLUMN_RENAMES = {
    "reporter_countries": "reporter_country",
    "partner_countries": "partner_country",
}

# Final bilateral index for the garden table.
INDEX_COLUMNS = [
    "reporter_country",
    "partner_country",
    "item_code",
    "element_code",
    "year",
]

# Flag definitions.
FLAGS = {
    "A": "Official figure",
    "X": "Figure from external organization",
    "E": "Estimated value",
    "I": "Value imputed by a receiving agency",
}

# Earliest year expected in the FAOSTAT detailed trade matrix.
MIN_YEAR = 1986


def _count_self_trade(tb: Table) -> int:
    """Count rows where reporter and partner are the same country, robust to
    whether the columns are still categoricals (with potentially different
    category sets after harmonization) or have been decategorized to plain
    string columns by `harmonize_names`."""
    rep, par = tb["reporter_country"], tb["partner_country"]
    if isinstance(rep.dtype, pd.CategoricalDtype) and isinstance(par.dtype, pd.CategoricalDtype):
        # Align categories so we can compare the integer codes directly. This
        # avoids materializing 50 M+ Python strings for the comparison.
        cats = sorted(set(rep.cat.categories) | set(par.cat.categories))
        rep_codes = rep.cat.set_categories(cats).cat.codes
        par_codes = par.cat.set_categories(cats).cat.codes
        return int((rep_codes == par_codes).sum())
    return int((rep == par).sum())


def plot_coverage(tb: Table) -> None:
    """Show two side-by-side bars per year: number of rows and number of
    distinct reporting countries. Together they distinguish 'less data
    actually reported' (both drop) from 'less trade activity' (rows drop
    but reporter count stays roughly flat) — useful when picking the
    latest well-covered year."""
    import plotly.graph_objects as go

    grouped = tb.groupby("year", observed=True)
    rows = grouped.size().sort_index()
    reporters = grouped["reporter_country"].nunique().sort_index()
    years = rows.index.astype(int).tolist()

    fig = go.Figure()
    fig.add_bar(x=years, y=rows.values, name="Rows", yaxis="y1", opacity=0.8)
    fig.add_bar(x=years, y=reporters.values, name="Distinct reporters", yaxis="y2", opacity=0.8)
    fig.update_layout(
        title="Detailed trade matrix dataset - Coverage",
        xaxis=dict(title="Year"),
        yaxis=dict(title="Rows", side="left"),
        yaxis2=dict(title="Distinct reporters", side="right", overlaying="y", showgrid=False),
        barmode="group",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.show()


def plot_reporting_coverage_by_year(tb: Table) -> None:
    """Stacked bar chart per year showing what fraction of bilateral flows
    are reported by both sides ("matched"), only by the exporter, or only by
    the importer.

    For every (reporter, partner, item, year) tuple we check whether:
      * the reporter has an "Export quantity" row to that partner, AND
      * the partner has an "Import quantity" row from that reporter
    and bucket the tuple accordingly. The plot is a quick way to see how much
    of the data is one-sided vs symmetric, and whether consistency is
    improving over time.
    """
    import plotly.express as px

    # Build the two key sets. For the importer side, swap reporter ↔ partner
    # so the key (A, B, item, year) means "A→B flow", regardless of which
    # side originally reported it. Cast to str to avoid surprises when
    # merging two categoricals with different category sets.
    qty = tb[tb["element"].isin(["Export quantity", "Import quantity"])][
        ["reporter_country", "partner_country", "item_code", "year", "element"]
    ].copy()
    for col in ("reporter_country", "partner_country"):
        qty[col] = qty[col].astype(str)

    exp_keys = qty.loc[
        qty["element"] == "Export quantity",
        ["reporter_country", "partner_country", "item_code", "year"],
    ].drop_duplicates()
    imp_keys = (
        qty.loc[
            qty["element"] == "Import quantity",
            ["reporter_country", "partner_country", "item_code", "year"],
        ]
        .rename(columns={"reporter_country": "partner_country", "partner_country": "reporter_country"})
        .drop_duplicates()
    )

    # Indicator merge: "both" = matched, "left_only" = exporter-only,
    # "right_only" = importer-only.
    merged = exp_keys.merge(
        imp_keys,
        how="outer",
        indicator=True,
        on=["reporter_country", "partner_country", "item_code", "year"],
    )
    by_year = (
        merged.groupby("year", observed=True)["_merge"]
        .value_counts(normalize=True)
        .unstack(fill_value=0.0)
        .rename(columns={"both": "matched", "left_only": "exporter-only", "right_only": "importer-only"})
    )
    # Order the stack so the "good" category sits at the bottom.
    by_year = by_year[["matched", "exporter-only", "importer-only"]].reset_index()
    long = by_year.melt(id_vars="year", var_name="status", value_name="share")

    fig = px.bar(
        long,
        x="year",
        y="share",
        color="status",
        title="Reporting coverage",
        labels={"year": "Year", "share": "Share of (reporter, partner, item) tuples"},
        category_orders={"status": ["matched", "exporter-only", "importer-only"]},
    )
    fig.update_layout(barmode="stack", yaxis_tickformat=".0%")
    fig.show()


def plot_quantity_mismatch_by_reporter(
    tb: Table, year: int | None = None, top_n: int = 20, include_unmatched: bool = False
) -> None:
    """Stacked horizontal bar showing how bilateral flows distribute across
    quantity-agreement bands, for the top-N reporting countries.

    For every matched (reporter, partner, item) flow we compute the
    intuitive quantity-agreement ratio
        agreement = min(exp_qty, imp_qty) / max(exp_qty, imp_qty)
    which is 1.0 when both sides report the same quantity, 0.5 when one
    side reports twice the other, 0.1 when one side reports ten times the
    other, etc. Flows are binned into bands (<25% / 25–50% / 50–75% /
    75–90% / ≥90%) and we plot, per reporter, the share of flows in each
    band, sorted so the worst reporters (smallest share of ≥90% agreement)
    sit at the top.

    Args:
        year: Year to analyze. Defaults to the latest year with row count
            ≥ 90% of the series maximum (skipping the partial tail year).
        top_n: Number of reporters to include, ranked by total number of
            flows in `year`.
        include_unmatched: If True, also count flows reported by only one
            side (exporter-only or importer-only). Their `agreement` is
            zero by definition (one of the two quantities is missing), so
            they land in a dedicated "Unmatched" band drawn darkest red.
            The chart then captures both coverage *and* quantity-agreement
            problems in one view (otherwise `plot_reporting_coverage_by_year`
            shows the coverage half separately). Default False.

    Intended as a development aid; not called by `run()` by default."""
    import numpy as np
    import plotly.express as px

    # Pick the latest well-covered year if none was specified.
    if year is None:
        rows_per_year = tb.groupby("year", observed=True).size()
        year = int(rows_per_year[rows_per_year >= 0.9 * rows_per_year.max()].index.max())

    # Build per-direction tables for that year.
    qty = tb[(tb["year"] == year) & tb["element"].isin(["Export quantity", "Import quantity"])][
        ["reporter_country", "partner_country", "item_code", "element", "value"]
    ].copy()
    for col in ("reporter_country", "partner_country"):
        qty[col] = qty[col].astype(str)

    exp = qty.loc[
        qty["element"] == "Export quantity",
        ["reporter_country", "partner_country", "item_code", "value"],
    ].rename(columns={"value": "exp_qty"})
    imp = qty.loc[
        qty["element"] == "Import quantity",
        ["reporter_country", "partner_country", "item_code", "value"],
    ].rename(columns={"reporter_country": "partner_country", "partner_country": "reporter_country", "value": "imp_qty"})

    # Inner-merge gives matched flows only; outer-merge keeps exporter-only and
    # importer-only too (an unmatched flow has one quantity NaN before fillna).
    how = "outer" if include_unmatched else "inner"
    merged = exp.merge(imp, on=["reporter_country", "partner_country", "item_code"], how=how)
    merged["exp_qty"] = merged["exp_qty"].fillna(0)
    merged["imp_qty"] = merged["imp_qty"].fillna(0)
    if include_unmatched:
        # Keep rows reported by at least one side. After fillna, a row with one
        # zero side is exactly an "unmatched" flow.
        merged = merged[(merged["exp_qty"] > 0) | (merged["imp_qty"] > 0)].copy()
    else:
        # Matched-only: both sides reported a positive quantity.
        merged = merged[(merged["exp_qty"] > 0) & (merged["imp_qty"] > 0)].copy()

    # Compute the agreement ratio on plain numpy arrays to bypass the owid
    # Variable arithmetic (which combines indicator metadata and gets confused
    # by `np.maximum` / `np.minimum` on filtered Variables).
    exp_arr = merged["exp_qty"].to_numpy()
    imp_arr = merged["imp_qty"].to_numpy()
    max_arr = np.maximum(exp_arr, imp_arr)
    # max_arr is > 0 for every surviving row, so the division is safe.
    merged["agreement"] = np.minimum(exp_arr, imp_arr) / max_arr

    # Bin by agreement. Matched flows land in 5 bands; unmatched flows (only
    # present when include_unmatched=True; both sides are positive in the
    # matched-only branch) get tagged as "Unmatched" via a mask override.
    matched_band_labels = ["<25%", "25–50%", "50–75%", "75–90%", "≥90%"]
    matched_band_colors = ["#d73027", "#fc8d59", "#fee08b", "#91cf60", "#1a9850"]
    band = pd.cut(merged["agreement"], bins=[-0.001, 0.25, 0.50, 0.75, 0.90, 1.001], labels=matched_band_labels)
    if include_unmatched:
        unmatched_mask = (merged["exp_qty"] == 0) | (merged["imp_qty"] == 0)
        band = band.cat.add_categories("Unmatched")
        band[unmatched_mask] = "Unmatched"
        band_labels = ["Unmatched"] + matched_band_labels
        band_colors = ["#67000d"] + matched_band_colors  # darker red for "Unmatched"
    else:
        band_labels = matched_band_labels
        band_colors = matched_band_colors
    merged["band"] = band

    # Top-N reporters by total number of flows in this view.
    top = merged["reporter_country"].value_counts().head(top_n).index.tolist()
    sub = merged[merged["reporter_country"].isin(top)]

    shares = sub.groupby(["reporter_country", "band"], observed=True).size().unstack(fill_value=0)
    shares = shares.div(shares.sum(axis=1), axis=0)[band_labels]
    # Sort with the worst reporters (smallest share of ≥90% agreement) at the top.
    shares = shares.sort_values("≥90%", ascending=True)

    long = shares.reset_index().melt(id_vars="reporter_country", var_name="band", value_name="share")
    title = (
        "Quantity agreement (matched flows + unmatched as 0%)"
        if include_unmatched
        else "Quantity agreement among matched flows"
    )
    x_label = "Share of all flows" if include_unmatched else "Share of matched flows"
    fig = px.bar(
        long,
        x="share",
        y="reporter_country",
        color="band",
        orientation="h",
        title=title,
        labels={"share": x_label, "reporter_country": "Reporter"},
        category_orders={"band": band_labels, "reporter_country": shares.index.tolist()},
        color_discrete_sequence=band_colors,
    )
    fig.update_layout(barmode="stack", xaxis_tickformat=".0%")
    fig.show()


def sanity_check_inputs(tb: Table) -> None:
    """Run cheap, deterministic checks on the harmonized but otherwise
    unprocessed input table. Anything that requires a self-join (e.g.
    quantifying reporting asymmetry between exporter and importer) is left
    to the analysis notebook in ai/faostat_tm/, since the answer depends on
    the year and the threshold is a judgement call."""
    # All flag codes have a known definition.
    missing_flags = set(tb["flag"].cat.categories) - set(FLAGS)
    assert not missing_flags, f"Missing flag definitions: {sorted(missing_flags)}"

    # Values are non-null and non-negative. FAOSTAT trade flows should always
    # be ≥ 0; nulls would indicate a parsing problem upstream.
    assert tb["value"].notna().all(), "Found null values in 'value' column."
    assert (tb["value"] >= 0).all(), "Found negative values in 'value' column."

    # FAOSTAT TM contains a small but non-zero number of self-trade rows (reporter == partner).
    n_self_trade = _count_self_trade(tb)
    self_trade_share = n_self_trade / len(tb) if len(tb) > 0 else 0
    log.info("faostat_tm.self_trade_rows", count=n_self_trade, share=f"{self_trade_share:.2%}")
    assert self_trade_share < 0.01, f"Self-trade rows are {self_trade_share:.2%} of the table."

    # Year range is sensible.
    min_year, max_year = int(tb["year"].min()), int(tb["year"].max())
    assert MIN_YEAR <= min_year <= max_year, f"Unexpected year range: {min_year}-{max_year}"


def run() -> None:
    #
    # Load data.
    #
    # Use `safe_types=False` to save time and memory.
    ds_meadow = paths.load_dataset("faostat_tm")
    tb = ds_meadow.read("faostat_tm", safe_types=False)

    #
    # Process data.
    #
    # Rename columns conveniently.
    tb = tb.rename(columns=COLUMN_RENAMES, errors="raise")

    # Harmonize reporter and partner country names.
    # paths.regions.harmonizer(tb=tb, country_col="partner_country", institution="FAO")
    tb = paths.regions.harmonize_names(tb=tb, country_col="reporter_country", warn_on_unused_countries=False)
    tb = paths.regions.harmonize_names(tb=tb, country_col="partner_country", warn_on_unused_countries=False)

    # Sanity check inputs.
    sanity_check_inputs(tb=tb)

    # Inspect rows + distinct reporters per year (useful to spot the partial tail year).
    # Uncomment for research.
    # plot_coverage(tb=tb)

    # Inspect what fraction of bilateral flows are matched (both sides report) vs. one-sided (exporter-only / importer-only), by year.
    # Uncomment for research.
    # plot_reporting_coverage_by_year(tb=tb)

    # Among matched flows in the latest well-covered year, inspect how much exporter-reported and importer-reported
    # quantities actually agree, broken down by top reporters.
    # Uncomment for research.
    # plot_quantity_mismatch_by_reporter(tb=tb)

    # Map flags.
    tb["flag"] = tb["flag"].cat.rename_categories(FLAGS)
    tb["flag"] = tb["flag"].copy_metadata(tb["value"])

    # Improve table format.
    tb = tb.format(keys=INDEX_COLUMNS)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
