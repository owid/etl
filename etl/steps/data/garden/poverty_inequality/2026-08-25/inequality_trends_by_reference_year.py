"""Is income inequality rising everywhere? A varying-reference-year comparison of PIP and WID.

For every country, year and series in the harmonized PIP/WID distributions, this step computes a
set of inequality metrics directly from the 109-bin distributions: the Gini coefficient and three
members of the Generalized Entropy family, whose parameter α sets how sensitive the index is to
the top versus the bottom of the distribution — GE(0) (the mean log deviation, bottom-sensitive),
GE(1) (the Theil index) and GE(2) (top-sensitive). Computing every metric from the same bins means
the sources are compared on identical definitions, rather than through their own published
headline measures.

It then asks, for each *reference year* compared against the latest year in the data: in how many
countries — and for what share of the covered population — has inequality risen, fallen, or stayed
stable since that reference year? And what was the average change, unweighted and
population-weighted? The reference year is the time axis of the output tables, so a chart of these
indicators reads as "compared to <year>, inequality has risen in N countries".

The classification thresholds and metric set are deliberate parameters (constants below): the
"stable" band defaults to a ±5% relative change, and GE(α) for α < 0 is excluded because the
$0.01/day floor on zero incomes would dominate it.
"""

import numpy as np
import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

log = paths.log

# Metrics computed from the binned distributions. GE(0) uses the same $0.01/day floor on zero
# incomes as the decomposition step (log of zero is undefined); Gini, GE(1) and GE(2) need none.
METRICS = ["gini", "mean_log_deviation", "theil_index", "generalized_entropy_2"]

# Keep in sync with harmonized_income_distributions.ZERO_INCOME_REPLACEMENT.
ZERO_INCOME_REPLACEMENT = 0.01

# Series shown in the reference-year comparison: PIP and WID's two tax concepts. Per-adult chosen
# as WID's native basis — every metric here is scale-invariant, so per-capita gives identical
# values (asserted below).
TREND_SERIES = ["pip", "wid_before_tax_per_adult", "wid_after_tax_per_adult"]

# A country counts as "stable" when the metric's relative change since the reference year is
# within this band; beyond it, "rising" or "falling" by sign.
STABLE_THRESHOLD = 0.05


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("harmonized_income_distributions")
    tb_distributions = ds.read("income_distributions", safe_types=False)
    tb_decomposition_by_country = ds.read("inequality_decomposition_by_country", safe_types=False)

    #
    # Process data.
    #
    tb_metrics = compute_metrics(tb_distributions)

    sanity_check_metrics(tb_metrics, tb_decomposition_by_country)

    tb_population = latest_year_population(tb_distributions)
    tb_changes = changes_by_country(tb_metrics, tb_population)
    tb_trends = aggregate_by_reference_year(tb_changes)

    sanity_check_trends(tb_changes, tb_trends)

    #
    # Save outputs.
    #
    origins = tb_distributions["avg"].metadata.origins
    tables = []
    for tb, keys, short_name in [
        (tb_metrics, ["country", "year", "series"], "inequality_metrics"),
        (tb_changes, ["country", "year", "series", "metric"], "inequality_change_by_country"),
        (tb_trends, ["year", "series", "metric"], "inequality_change_by_reference_year"),
    ]:
        tb = tb.format(keys, short_name=short_name)
        for col in tb.columns:
            tb[col].metadata.origins = list(origins)
        tables.append(tb)

    ds_garden = paths.create_dataset(tables=tables)
    ds_garden.save()


def compute_metrics(tb_distributions: Table) -> Table:
    """All metrics for every (country, year, series), computed row-wise on (country-years x 109)
    matrices of bin averages and normalized population weights."""
    blocks = []
    for series in tb_distributions["series"].unique():
        d = (
            tb_distributions[tb_distributions["series"] == series]
            .sort_values(["country", "year", "p_low"])
            .reset_index(drop=True)
        )
        keys = d.loc[d["p_low"] == 0, ["country", "year"]].reset_index(drop=True)
        assert len(d) == len(keys) * 109, f"Series {series} has country-years without exactly 109 bins."

        x = d["avg"].to_numpy(dtype=float).reshape(-1, 109)
        w = d["pop"].to_numpy(dtype=float).reshape(-1, 109)
        w = w / w.sum(axis=1, keepdims=True)

        block = keys.copy()
        block["series"] = series
        mu = (w * x).sum(axis=1)

        # Gini via the Lorenz curve (trapezoid rule, valid for unequal bin widths). Bins carry no
        # within-bin inequality, so this is a slight underestimate — identically for all series.
        lorenz = np.cumsum(w * x, axis=1) / (w * x).sum(axis=1, keepdims=True)
        lorenz_prev = np.concatenate([np.zeros((len(keys), 1)), lorenz[:, :-1]], axis=1)
        block["gini"] = 1 - ((lorenz + lorenz_prev) * w).sum(axis=1)

        # GE(0) = mean log deviation, with the zero floor.
        x_floored = np.where(x == 0, ZERO_INCOME_REPLACEMENT, x)
        mu_floored = (w * x_floored).sum(axis=1)
        block["mean_log_deviation"] = np.log(mu_floored) - (w * np.log(x_floored)).sum(axis=1)

        # GE(1) = Theil index, with 0·log(0) = 0 (no floor needed).
        ratio = x / mu[:, None]
        block["theil_index"] = (w * np.where(x > 0, ratio * np.log(np.where(x > 0, ratio, 1)), 0.0)).sum(axis=1)

        # GE(2), top-sensitive.
        block["generalized_entropy_2"] = ((w * ratio**2).sum(axis=1) - 1) / 2

        blocks.append(block)
    return pr.concat(blocks, ignore_index=True)


def latest_year_population(tb_distributions: Table) -> Table:
    """Country population at the latest year: WID total population (the common demographic
    yardstick), recovered as the per-capita WID series' bin-population sum."""
    latest_year = int(tb_distributions["year"].max())
    d = tb_distributions[
        (tb_distributions["series"] == "wid_before_tax_per_capita") & (tb_distributions["year"] == latest_year)
    ]
    return d.groupby("country", observed=True)["pop"].sum().rename("population").reset_index()


def changes_by_country(tb_metrics: Table, tb_population: Table) -> Table:
    """Change in each metric from every reference year to the latest year, per country and series,
    classified as rising/falling/stable."""
    latest_year = int(tb_metrics["year"].max())
    latest = tb_metrics[tb_metrics["year"] == latest_year]
    earlier = tb_metrics[(tb_metrics["year"] < latest_year) & tb_metrics["series"].isin(TREND_SERIES)]

    merged = earlier.merge(latest, on=["country", "series"], suffixes=("", "_latest"))

    blocks = []
    for metric in METRICS:
        block = merged[["country", "year", "series"]].copy()
        block["metric"] = metric
        block["value_in_reference_year"] = merged[metric]
        block["value_in_latest_year"] = merged[f"{metric}_latest"]
        blocks.append(block)
    tb = pr.concat(blocks, ignore_index=True)

    tb["change"] = tb["value_in_latest_year"] - tb["value_in_reference_year"]
    tb["relative_change"] = tb["change"] / tb["value_in_reference_year"]
    tb["direction"] = np.select(
        [tb["relative_change"] > STABLE_THRESHOLD, tb["relative_change"] < -STABLE_THRESHOLD],
        ["rising", "falling"],
        default="stable",
    )
    tb["latest_year"] = latest_year

    tb = tb.merge(tb_population, on="country", how="left")
    assert tb["population"].notna().all(), "Missing latest-year population for some country."
    return tb


def aggregate_by_reference_year(tb_changes: Table) -> Table:
    """The slide-level aggregation: counts, population shares and average changes per reference
    year, series and metric."""
    g = tb_changes.groupby(["year", "series", "metric"], observed=True)

    def population_share(direction: str):
        return lambda t: t.loc[t["direction"] == direction, "population"].sum() / t["population"].sum()

    tb = g.apply(
        lambda t: pd.Series(
            {
                "latest_year": t["latest_year"].iloc[0],
                "num_countries_rising": int((t["direction"] == "rising").sum()),
                "num_countries_falling": int((t["direction"] == "falling").sum()),
                "num_countries_stable": int((t["direction"] == "stable").sum()),
                "num_countries": len(t),
                "population_share_rising": population_share("rising")(t),
                "population_share_falling": population_share("falling")(t),
                "population_share_stable": population_share("stable")(t),
                "population_covered": t["population"].sum(),
                "average_change": t["change"].mean(),
                "average_change_population_weighted": np.average(t["change"], weights=t["population"]),
                "average_relative_change": t["relative_change"].mean(),
                "average_relative_change_population_weighted": np.average(
                    t["relative_change"], weights=t["population"]
                ),
            }
        ),
        include_groups=False,
    ).reset_index()
    return tb


def sanity_check_metrics(tb_metrics: Table, tb_decomposition_by_country: Table) -> None:
    assert tb_metrics["gini"].between(0, 1, inclusive="neither").all(), "Gini outside (0, 1)."
    for metric in ["mean_log_deviation", "theil_index", "generalized_entropy_2"]:
        assert (tb_metrics[metric] >= 0).all(), f"Negative {metric}."

    # Scale invariance: per-adult and per-capita variants of the same WID concept differ by a
    # country-level constant, so Gini/Theil/GE(2) must agree exactly. (The mean log deviation is
    # excluded: the $0.01/day floor is absolute, so it interacts with the scale in zero-bin
    # country-years.)
    pa = tb_metrics[tb_metrics["series"] == "wid_before_tax_per_adult"].set_index(["country", "year"])
    pc = tb_metrics[tb_metrics["series"] == "wid_before_tax_per_capita"].set_index(["country", "year"])
    for metric in ["gini", "theil_index", "generalized_entropy_2"]:
        assert np.allclose(pa[metric], pc[metric], atol=1e-6), f"{metric} is not scale-invariant."

    # Cross-step consistency: the mean log deviation must reproduce the decomposition step's
    # within-country MLD (same numbers, independent code path).
    merged = tb_metrics.merge(tb_decomposition_by_country, on=["country", "year", "series"], how="inner")
    assert len(merged) == len(tb_metrics)
    assert np.allclose(merged["mean_log_deviation"], merged["mld_within"], atol=1e-6), (
        "Mean log deviation deviates from the decomposition step's within-country MLD."
    )


def sanity_check_trends(tb_changes: Table, tb_trends: Table) -> None:
    counts = tb_trends[["num_countries_rising", "num_countries_falling", "num_countries_stable"]].sum(axis=1)
    assert (counts == tb_trends["num_countries"]).all(), "Direction counts do not sum to the total."

    shares = tb_trends[["population_share_rising", "population_share_falling", "population_share_stable"]].sum(axis=1)
    assert np.allclose(shares, 1.0), "Population shares do not sum to 1."

    # The panel is balanced upstream, so counts are comparable across the reference-year axis.
    sets_per_year = tb_changes.groupby("year", observed=True)["country"].nunique()
    assert sets_per_year.nunique() == 1, "Country coverage varies across reference years."
