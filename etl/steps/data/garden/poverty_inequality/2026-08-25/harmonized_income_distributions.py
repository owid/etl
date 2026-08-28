"""Harmonized PIP and WID income distributions on a common 109-bin structure, with bridging series
and a between/within-country decomposition of global inequality.

This step ports the verified data pipeline behind Joe Hasell's PIP-vs-WID triangulation work
(github.com/JoeHasell/prague-pip-wid, `data/scripts/`) into ETL, extended from a single year to the
full panel of years both sources cover, so the exercise re-runs on every PIP/WID data update.

What it builds, in order:

1. Five RAW series on an identical 109-bin structure (99 one-percent bins, nine 0.1% bins across
   p99-p99.9, and the top 0.1%): PIP (disposable income or consumption, per capita), and WID
   pre-tax/post-tax national income, each per adult and per capita. PIP's 1000 equal bins nest
   exactly into the 109 bins, so the aggregation is exact; WID arrives on (a superset of) the
   structure. WID values are already PPP-converted upstream (in the snapshot's Stata extraction) —
   here they are only converted from annual to daily and from per-adult to per-capita (using WID's
   own population counts, published as the `population` table of the WID garden dataset).
2. Three DERIVED "bridging" series that walk the two sources toward each other:
   - `pip_income_basis`: PIP with consumption-based countries mapped to an income basis, through a
     per-percentile regression ln(income_p) = alpha_p + beta_p * ln(consumption_p) fitted on the
     country-years where PIP publishes both welfare types.
   - `pip_income_basis_top_adjusted`: the above with the top of the distribution (above the splice
     percentile) rebuilt with the shape of WID's post-tax distribution, anchored at PIP's own level.
   - `wid_after_tax_rescaled`: WID post-tax per capita, rescaled country-by-country to the
     `pip_income_basis_top_adjusted` means — so the two ladders meet at identical country means
     (and, by construction, identical between-country components).
3. The between/within MLD (mean log deviation) decomposition of global inequality, per year and
   series, over the common sample of countries. Two conventions are baked in (decided in the
   source project): country weights come from WID's demography MATCHED TO THE SERIES' BASIS
   (adults for per-adult series, total population otherwise — including for PIP), so the sources'
   demographic disagreements never enter the comparison; and zero incomes are replaced by
   $0.01/day inside the MLD only (log of zero is undefined) — they are RETAINED in the
   distributions table.
"""

import numpy as np
import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from sklearn.isotonic import IsotonicRegression

from etl.helpers import PathFinder

paths = PathFinder(__file__)

log = paths.log

# WID series to use: including extrapolations ("yes") matches the source project (whose fetch did
# not exclude them) and is the only slice with near-complete country coverage (without
# extrapolations WID covers ~17 countries in 2023 vs ~226 with them). PIP's thousand-bins panel is
# itself lined up/extrapolated by the World Bank, so this is the symmetric choice.
EXTRAPOLATED_CHOICE = "yes"

DAYS_PER_YEAR = 365

# First year of the panel (PIP's thousand-bins data starts in 1990).
FIRST_YEAR = 1990

# The top adjustment names a percentile boundary: splice 95 means the ANCHOR bin is p94p95 (which
# keeps its PIP value) and p95p96 is the first bin rebuilt with WID's shape.
SPLICE_PERCENTILE = 95

# $/day; applied ONLY inside the MLD calculation, never stored in the distributions.
ZERO_INCOME_REPLACEMENT = 0.01

# PPP round of the PIP percentiles used to fit the consumption -> income model.
PIP_PPP_VERSION = 2021

# The consumption -> income model fits a separate (alpha, beta) per percentile, and those
# coefficients are not monotone in p: at the sub-$1 consumption levels found at the bottom of poor
# countries, p1's implies a higher income than p2's. So a monotone consumption profile can come out
# non-monotone. True projects the fitted profile back onto the monotone cone by isotonic regression
# (see enforce_monotone). Set False to reproduce the source project's unadjusted output.
ENFORCE_MONOTONE_INCOME_BASIS = True

# How to assign each country's welfare basis (income vs consumption) across the panel:
#   "nearest_survey": the welfare type of the country's nearest national survey year — correct for
#                     a multi-year panel when countries switch basis over time.
#   "latest_survey":  one static type per country, from its most recent survey (what the source
#                     project used for its single reference year) — kept for validation runs.
WELFARE_ASSIGNMENT = "nearest_survey"

# Sanity floor on the common PIP-and-WID country sample.
MIN_COMMON_COUNTRIES = 150

# The eight series, in bridging order (WID side -> meeting point -> PIP side).
SERIES_WID_RAW = [
    # (series name, welfare_type in the WID distribution table, population basis)
    ("wid_before_tax_per_adult", "before tax", "adult"),
    ("wid_before_tax_per_capita", "before tax", "total"),
    ("wid_after_tax_per_adult", "after tax", "adult"),
    ("wid_after_tax_per_capita", "after tax", "total"),
]
SERIES_ORDER = [
    "wid_before_tax_per_adult",
    "wid_before_tax_per_capita",
    "wid_after_tax_per_adult",
    "wid_after_tax_per_capita",
    "wid_after_tax_rescaled",
    "pip_income_basis_top_adjusted",
    "pip_income_basis",
    "pip",
]
# Population basis per series, for the MLD weighting convention (explicit, not name-sniffing).
SERIES_BASIS = {
    "wid_before_tax_per_adult": "adult",
    "wid_before_tax_per_capita": "total",
    "wid_after_tax_per_adult": "adult",
    "wid_after_tax_per_capita": "total",
    "wid_after_tax_rescaled": "total",
    "pip_income_basis_top_adjusted": "total",
    "pip_income_basis": "total",
    "pip": "total",
}
TOP_TAIL_SHAPE_SERIES = "wid_after_tax_per_capita"
TOP_ADJUSTMENT_BASE_SERIES = "pip_income_basis"
RESCALE_MEAN_SERIES = "pip_income_basis_top_adjusted"


def run() -> None:
    #
    # Load inputs.
    #
    ds_bins = paths.load_dataset("thousand_bins_distribution")
    tb_bins = ds_bins.read("thousand_bins_distribution", safe_types=False)

    ds_pip = paths.load_dataset("world_bank_pip")
    tb_percentiles = ds_pip.read("percentiles", safe_types=False)

    ds_wid = paths.load_dataset("world_inequality_database")
    tb_dist = ds_wid.read("distribution", safe_types=False)
    tb_incomes_wid = ds_wid.read("incomes", safe_types=False)

    tb_pop = ds_wid.read("population", safe_types=False)

    origins = {
        "pip_bins": tb_bins["avg"].metadata.origins,
        "pip_percentiles": tb_percentiles["avg"].metadata.origins,
        "wid": tb_dist["avg"].metadata.origins,
        "wid_population": tb_pop["adult_population"].metadata.origins,
    }

    #
    # Process data.
    #
    bins_lookup = make_bin_lookup()

    last_year = int(
        min(
            tb_bins["year"].max(),
            tb_dist.loc[tb_dist["extrapolated"] == EXTRAPOLATED_CHOICE, "year"].max(),
        )
    )
    log.info(f"Common year range: {FIRST_YEAR}-{last_year}")

    tb_wid = process_wid_distributions(tb_dist, tb_pop, bins_lookup, last_year)
    tb_pip = aggregate_pip_bins(tb_bins, bins_lookup, last_year)

    sanity_check_raw_series(tb_wid, tb_pip, tb_dist, tb_pop, tb_incomes_wid)

    # The common sample: countries present in PIP and every raw WID series in every common year
    # (both sources are complete panels over the range, so balancing costs ~nothing — asserted).
    countries = common_sample(tb_wid, tb_pip)
    tb_wid = tb_wid[tb_wid["country"].isin(countries)].reset_index(drop=True)
    tb_pip = tb_pip[tb_pip["country"].isin(countries)].reset_index(drop=True)

    # Consumption -> income model and each country-year's welfare basis.
    tb_model = fit_consumption_income_model(tb_percentiles)
    tb_welfare = build_welfare_basis(tb_percentiles, countries, FIRST_YEAR, last_year)

    # Derived series, in the required order: the top graft needs the income-basis rows, and the
    # rescaled series needs the top-adjusted rows.
    tb_income_basis = adjust_consumption_to_income(tb_pip, tb_model, tb_welfare)
    tb_top_adjusted = graft_top_tail(tb_income_basis, tb_wid)
    tb_rescaled = rescale_wid_to_adjusted_pip_means(tb_wid, tb_top_adjusted)

    tb_distributions = pr.concat([tb_wid, tb_rescaled, tb_top_adjusted, tb_income_basis, tb_pip], ignore_index=True)
    tb_distributions["series"] = tb_distributions["series"].astype("category").cat.set_categories(SERIES_ORDER)

    # Between/within MLD decomposition per (year, series) over the common sample.
    tb_decomposition, tb_decomposition_by_country = mld_decomposition(tb_distributions, tb_pop)

    sanity_check_outputs(tb_distributions, tb_decomposition, tb_model, countries)

    #
    # Save outputs.
    #
    all_origins = sorted(
        {o for key in ["pip_bins", "wid", "wid_population"] for o in origins[key]}, key=lambda o: o.producer
    )
    tables = [
        format_with_origins(
            tb_distributions,
            keys=["country", "year", "series", "percentile"],
            short_name="income_distributions",
            origins=all_origins,
        ),
        format_with_origins(
            tb_model, keys=["percentile"], short_name="consumption_income_model", origins=origins["pip_percentiles"]
        ),
        format_with_origins(
            tb_welfare, keys=["country", "year"], short_name="pip_welfare_basis", origins=origins["pip_percentiles"]
        ),
        format_with_origins(
            tb_decomposition, keys=["year", "series"], short_name="inequality_decomposition", origins=all_origins
        ),
        format_with_origins(
            tb_decomposition_by_country,
            keys=["country", "year", "series"],
            short_name="inequality_decomposition_by_country",
            origins=all_origins,
        ),
    ]

    ds_garden = paths.create_dataset(tables=tables)
    ds_garden.save()


def make_bin_lookup() -> Table:
    """The canonical 109-bin structure: label, bounds (fractions) and a 0-108 position index."""
    labels = wid_bin_labels()
    # "p99.9p100" -> ("99.9", "100"): strip the leading p, split on the separator p.
    bounds = [label[1:].split("p") for label in labels]
    tb = Table(
        {
            "percentile": labels,
            "p_low": [float(lo) / 100 for lo, _ in bounds],
            "p_high": [float(hi) / 100 for _, hi in bounds],
            "bin_index": range(len(labels)),
        }
    )
    assert len(tb) == 109 and abs((tb["p_high"] - tb["p_low"]).sum() - 1) < 1e-9
    return tb


def wid_bin_labels() -> list:
    """The 109 WID percentile labels in ascending order: p0p1 ... p98p99, then the top 1% split
    into nine 0.1% bins and the top 0.1%."""

    def fmt(x: float) -> str:
        return f"{x:g}"

    labels = [f"p{i}p{i + 1}" for i in range(99)]
    tenths = [round(99 + 0.1 * k, 1) for k in range(10)] + [100]
    labels += [f"p{fmt(lo)}p{fmt(hi)}" for lo, hi in zip(tenths[:-1], tenths[1:])]
    return labels


def process_wid_distributions(tb_dist: Table, tb_pop: Table, bins_lookup: Table, last_year: int) -> Table:
    """The four raw WID series: filter to the 109-bin structure, convert annual -> daily and
    per-adult -> per-capita, and attach bin populations from WID's own counts."""
    d = tb_dist[
        (tb_dist["extrapolated"] == EXTRAPOLATED_CHOICE)
        & tb_dist["welfare_type"].isin([w for _, w, _ in SERIES_WID_RAW])
        & tb_dist["percentile"].isin(bins_lookup["percentile"])
        & tb_dist["year"].between(FIRST_YEAR, last_year)
    ][["country", "year", "welfare_type", "percentile", "avg", "share"]].copy()

    # Nullable dtypes make masked math orders of magnitude slower; cast before any computation.
    d["avg"] = d["avg"].astype(np.float64)
    d["share"] = d["share"].astype(np.float64) / 100  # stored as percent upstream
    d["year"] = d["year"].astype(int)
    d["country"] = d["country"].astype(str)

    # A bin with no average cannot enter a series; dropping single bins would silently break the
    # 109-bin structure, so whole (country, year, welfare) groups are dropped and reported.
    incomplete = d.loc[d["avg"].isna(), ["country", "year", "welfare_type"]].drop_duplicates()
    if len(incomplete):
        log.warning(f"Dropping {len(incomplete)} country-year-welfare groups with missing bin averages.")
        d = d.merge(incomplete.assign(_drop=True), on=["country", "year", "welfare_type"], how="left")
        d = d[d["_drop"].isna()].drop(columns="_drop")

    d = d.merge(bins_lookup, on="percentile", how="left")
    d = d.merge(tb_pop, on=["country", "year"], how="left")
    # Countries in the WID distribution but without WID population cannot be used at all; they are
    # excluded from the common sample later, so only log them here.
    no_population = sorted(d.loc[d["total_population"].isna(), "country"].unique())
    if no_population:
        log.info(f"WID distribution countries without WID population (excluded): {no_population}")
        d = d[d["total_population"].notna()]

    # Provenance flag: a WID country-year counts as extrapolated unless it also appears in WID's
    # non-extrapolated series (i.e. it is anchored in observed data).
    observed = (
        tb_dist.loc[tb_dist["extrapolated"] == "no", ["country", "year", "welfare_type"]].drop_duplicates().copy()
    )
    observed["country"] = observed["country"].astype(str)
    observed["year"] = observed["year"].astype(int)
    observed["wid_extrapolated"] = "no"
    d = d.merge(observed, on=["country", "year", "welfare_type"], how="left")
    d["wid_extrapolated"] = d["wid_extrapolated"].fillna("yes")

    # Annual -> daily, to match PIP's clock. No PPP division here: that already happened in the
    # snapshot's Stata extraction, and doing it twice was a historical bug in the source project.
    d["avg_daily_per_adult"] = d["avg"] / DAYS_PER_YEAR

    # Per adult -> per capita. The same total income spread over everyone rather than over adults
    # only, so the per-capita figure is SMALLER by exactly the adult share (~0.76 US, ~0.47 Nigeria).
    # The float64 cast matters: these arrive as nullable dtypes, whose masked arithmetic is orders
    # of magnitude slower on a table this size.
    adult_share = (d["adult_population"] / d["total_population"]).astype(np.float64)
    d["avg_daily_per_capita"] = d["avg_daily_per_adult"] * adult_share

    # Bin populations: each bin covers a known slice of the distribution, so it holds that share of
    # the country. Both bases are built because SERIES_BASIS picks one per series below.
    bin_width = d["p_high"] - d["p_low"]
    d["bin_adult_population"] = d["adult_population"].astype(np.float64) * bin_width
    d["bin_total_population"] = d["total_population"].astype(np.float64) * bin_width

    # The four raw WID series, on the common 109-bin structure, with all the derived columns needed
    blocks = []
    for series, welfare, basis in SERIES_WID_RAW:
        block = d.loc[
            d["welfare_type"] == welfare,
            ["country", "year", "percentile", "p_low", "p_high", "bin_index", "share", "wid_extrapolated"],
        ].copy()
        block["avg"] = d.loc[d["welfare_type"] == welfare, f"avg_daily_per_{'adult' if basis == 'adult' else 'capita'}"]
        block["pop"] = d.loc[d["welfare_type"] == welfare, f"bin_{'adult' if basis == 'adult' else 'total'}_population"]
        block["series"] = series
        blocks.append(block)
    tb = pr.concat(blocks, ignore_index=True)
    return tb[
        [
            "country",
            "year",
            "series",
            "percentile",
            "p_low",
            "p_high",
            "bin_index",
            "pop",
            "avg",
            "share",
            "wid_extrapolated",
        ]
    ]


def aggregate_pip_bins(tb_bins: Table, bins_lookup: Table, last_year: int) -> Table:
    """Aggregate PIP's 1000 equal bins to the 109-bin structure — exactly, since the 0.1% bins nest
    into it (bins 1-10 -> p0p1, ..., 991 -> p99p99.1, ..., 1000 -> p99.9p100)."""
    d = tb_bins[tb_bins["year"].between(FIRST_YEAR, last_year)][["country", "year", "quantile", "avg", "pop"]].copy()
    d["avg"] = d["avg"].astype(np.float64)
    d["pop"] = d["pop"].astype(np.float64)
    d["year"] = d["year"].astype(int)
    d["country"] = d["country"].astype(str)

    counts = d.groupby(["country", "year"], observed=True).size()
    assert (counts == 1000).all(), f"PIP country-years without exactly 1000 bins: {counts[counts != 1000].head()}"

    # Map each 0.1% quantile to its 109-bin position: the first 990 fall ten-to-one into the 99
    # one-percent bins, and 991-1000 map one-to-one onto the ten bins above p99.
    q = d["quantile"].astype(int)
    d["bin_index"] = np.where(q <= 990, (q - 1) // 10, 99 + (q - 991))
    # Aggregate on income, not on averages: a mean of means would ignore the bin populations.
    d["income"] = d["avg"] * d["pop"]

    g = (
        d.groupby(["country", "year", "bin_index"], observed=True)
        .agg(pop=("pop", "sum"), income=("income", "sum"))
        .reset_index()
    )
    g["avg"] = g["income"] / g["pop"]
    g["share"] = g["income"] / g.groupby(["country", "year"], observed=True)["income"].transform("sum")

    # Exactness guard: the aggregation must conserve total income and population per country-year.
    totals_before = d.groupby(["country", "year"], observed=True)[["income", "pop"]].sum()
    totals_after = g.groupby(["country", "year"], observed=True)[["income", "pop"]].sum()
    assert np.allclose(totals_before, totals_after, rtol=1e-9), "PIP bin aggregation lost income or population mass."

    g = g.merge(bins_lookup, on="bin_index", how="left")
    g["series"] = "pip"
    return g[["country", "year", "series", "percentile", "p_low", "p_high", "bin_index", "pop", "avg", "share"]]


def common_sample(tb_wid: Table, tb_pip: Table) -> list:
    """Countries present in PIP and every raw WID series, in every common year (balanced panel)."""
    sets = [set(tb_pip["country"].unique())]
    for series, _, _ in SERIES_WID_RAW:
        sets.append(set(tb_wid.loc[tb_wid["series"] == series, "country"].unique()))
    countries = set.intersection(*sets)

    # Balance across years: with two complete panels this should drop nobody — report if it does.
    n_years = tb_pip["year"].nunique()
    year_counts = (
        pr.concat([tb_pip, tb_wid], ignore_index=True)
        .groupby(["series", "country"], observed=True)["year"]
        .nunique()
        .groupby("country")
        .min()
    )
    unbalanced = sorted(c for c in countries if year_counts.get(c, 0) < n_years)
    if unbalanced:
        log.warning(f"Dropping {len(unbalanced)} countries without the full year panel: {unbalanced}")
        countries -= set(unbalanced)

    pip_only = sorted(set(tb_pip["country"].unique()) - countries)
    wid_only = sorted(set(tb_wid["country"].unique()) - countries)
    log.info(f"Common sample: {len(countries)} countries; PIP-only: {len(pip_only)}; WID-only: {len(wid_only)}")
    assert len(countries) >= MIN_COMMON_COUNTRIES, f"Common sample collapsed to {len(countries)} countries."
    return sorted(countries)


def fit_consumption_income_model(tb_percentiles: Table) -> Table:
    """Per-percentile OLS ln(income_p) = alpha_p + beta_p * ln(consumption_p), fitted across the
    national country-years where PIP publishes both welfare types at the current PPP round.

    KNOWN LIMITATION — the sample is small and skewed away from where the model is applied. As of
    the 2026-06-26 PIP release it is 88 country-years across 19 countries:

        high-income          10 countries, 59 country-years  (Bulgaria, Croatia, Estonia, Hungary,
                                                              Latvia, Lithuania, Poland, Romania,
                                                              Russia, Slovakia)
        upper-middle-income   7 countries, 24 country-years  (Albania, Kosovo, Montenegro,
                                                              Philippines, Saint Lucia, Serbia,
                                                              Turkey)
        lower-middle-income   2 countries,  5 country-years  (Haiti, Nicaragua)
        low-income            0 countries,  0 country-years

    14 of the 19 are European; none is in Sub-Saharan Africa or South Asia, and Poland and Romania
    alone contribute 30 of the 88 observations. The transform is then applied to consumption-based
    country-years, which are overwhelmingly the poor ones — so it is extrapolated onto exactly the
    populations the sample excludes. The fit is also weakest at the bottom (R^2 0.28 at p1), which
    is where ENFORCE_MONOTONE_INCOME_BASIS has to intervene.
    """
    d = national_survey_percentiles(tb_percentiles)

    # The estimation sample: country-years where PIP publishes BOTH welfare types, so income and
    # consumption are observed for the same population.
    dual = d.groupby(["country", "year"], observed=True)["welfare_type"].nunique()
    dual = dual[dual == 2].index
    sample = d.set_index(["country", "year"]).loc[dual].reset_index()
    piv = (
        sample.pivot_table(index=["country", "year", "percentile"], columns="welfare_type", values="avg")
        .reset_index()
        .dropna()
    )
    # Both sides are logged below, so zero and negative values cannot enter.
    piv = piv[(piv["income"] > 0) & (piv["consumption"] > 0)]
    log.info(
        f"Consumption->income estimation sample: {len(dual)} dual country-years, "
        f"{piv['country'].nunique()} countries, {len(piv):,} percentile pairs"
    )

    # One regression per percentile: the mapping differs along the distribution, which is the
    # whole point of fitting it per percentile rather than once for the country.
    rows = []
    for p, g in piv.groupby("percentile"):
        x = np.log(g["consumption"].to_numpy(dtype=float))
        y = np.log(g["income"].to_numpy(dtype=float))
        beta, alpha = np.polyfit(x, y, 1)
        resid = y - (alpha + beta * x)
        rows.append(
            {
                "percentile": int(p),
                "alpha": alpha,
                "beta": beta,
                "num_country_years": len(g),
                "r_squared": 1 - resid.var() / y.var(),
            }
        )
    tb = Table(rows).sort_values("percentile").reset_index(drop=True)
    tb.metadata.short_name = "consumption_income_model"
    return tb


def build_welfare_basis(tb_percentiles: Table, countries: list, first_year: int, last_year: int) -> Table:
    """Each (country, year)'s welfare basis: the welfare type of the country's nearest national
    survey. Years with both types count as income.

    Income wins the tie because it is the TARGET basis of this harmonisation: `pip_income_basis`
    exists to put every country on an income footing, so where PIP observes income directly there
    is nothing to estimate and the fitted transform is skipped (`adjusted=False`). Using the model
    where the real thing is available would only add error.

    Note that this DEVIATES from PIP's own rule, deliberately. PIP prefers consumption: "Due to its
    closer connection to welfare, whenever both income and consumption estimates are available for a
    given reference year, consumption estimates are preferred" (https://datanalytics.worldbank.org/PIP-Methodology/lineupestimates.html#inccon). That is the right call
    for measuring poverty, which is PIP's purpose. It is the wrong one here, where the purpose is to
    express PIP on the same income concept WID uses so the two can be compared — so for the 88 dual
    country-years this series carries PIP's observed INCOME where PIP's own headline estimates carry
    its consumption. PIP publishes both, so nothing is being overridden.

    The same handbook section notes that "interpolations are never done between consumption and
    income aggregates" — PIP declines to bridge the two concepts. The consumption -> income model in
    this step is exactly such a bridge, which is why it is fitted and reported explicitly rather
    than treated as a detail (see fit_consumption_income_model for how thin its sample is).
    """
    d = national_survey_percentiles(tb_percentiles)
    types = (
        d.groupby(["country", "year"], observed=True)["welfare_type"]
        .agg(lambda s: "income" if "income" in set(s) else "consumption")
        .reset_index()
        .rename(columns={"year": "survey_year_used"})
    )
    types = types[types["country"].isin(countries)]

    # A complete country x year grid, so every panel year gets a basis even without a survey.
    years = Table({"year": range(first_year, last_year + 1)})
    grid = types[["country"]].drop_duplicates().merge(years, how="cross").sort_values(["country", "year"])

    # NOTE: merge_asof(direction="nearest") has NO distance limit, so a panel year far from any
    # survey still takes that survey's basis — up to 32 years away in the current data. Flagged in
    # ai/adversarial-review-harmonized_income_distributions-2026-08-26.md, not yet capped.
    if WELFARE_ASSIGNMENT == "nearest_survey":
        tb = pd.merge_asof(
            grid.sort_values("year"),
            types.sort_values("survey_year_used"),
            left_on="year",
            right_on="survey_year_used",
            by="country",
            direction="nearest",
        ).sort_values(["country", "year"])
    else:
        latest = types.sort_values("survey_year_used").groupby("country", observed=True).tail(1)
        tb = grid.merge(latest, on="country", how="left")

    # `adjusted` drives the transform downstream: only consumption country-years get mapped.
    tb["adjusted"] = tb["welfare_type"] == "consumption"
    n_switch = tb.groupby("country", observed=True)["welfare_type"].nunique().gt(1).sum()
    missing = sorted(set(countries) - set(tb["country"].unique()))
    log.info(
        f"Welfare basis ({WELFARE_ASSIGNMENT}): {tb['country'].nunique()} countries in the lookup, "
        f"{n_switch} switch basis across the panel; not in the lookup (passed through): {len(missing)}"
    )
    tb = Table(tb)
    tb.metadata.short_name = "pip_welfare_basis"
    return tb[["country", "year", "welfare_type", "survey_year_used", "adjusted"]].reset_index(drop=True)


def national_survey_percentiles(tb_percentiles: Table) -> Table:
    """National income/consumption survey percentiles at the current PPP round."""
    d = tb_percentiles[
        (tb_percentiles["ppp_version"] == PIP_PPP_VERSION)
        & (tb_percentiles["reporting_level"] == "national")
        & tb_percentiles["welfare_type"].isin(["income", "consumption"])
    ][["country", "year", "welfare_type", "percentile", "avg"]].copy()
    d["country"] = d["country"].astype(str)
    d["welfare_type"] = d["welfare_type"].astype(str)
    d["avg"] = d["avg"].astype(np.float64)
    d["year"] = d["year"].astype(np.int64)
    return d


def adjust_consumption_to_income(tb_pip: Table, tb_model: Table, tb_welfare: Table) -> Table:
    """The income-basis PIP series: consumption country-years mapped through the fitted model
    (income countries and countries absent from the welfare lookup pass through unchanged)."""
    tb = tb_pip.copy()
    tb["series"] = TOP_ADJUSTMENT_BASE_SERIES

    # The model's percentile for each bin follows from its position: 1% bins map 1:1, and every
    # bin inside the top 1% uses the p=100 coefficients.
    k = np.minimum(tb["bin_index"].to_numpy() + 1, 100)
    alpha = tb_model.set_index("percentile")["alpha"]
    beta = tb_model.set_index("percentile")["beta"]
    assert (beta > 0).all(), "Non-positive beta in the consumption->income model."

    consumption = tb.merge(tb_welfare[["country", "year", "adjusted"]], on=["country", "year"], how="left")[
        "adjusted"
    ].fillna(False)
    consumption = consumption.to_numpy(dtype=bool)

    avg = tb["avg"].to_numpy(dtype=float)
    adjusted_avg = np.exp(alpha.loc[k].to_numpy()) * avg ** beta.loc[k].to_numpy()
    tb["avg"] = np.where(consumption, adjusted_avg, avg)

    non_monotone = check_monotone_within_groups(tb.loc[consumption])
    if non_monotone:
        if ENFORCE_MONOTONE_INCOME_BASIS:
            tb = enforce_monotone(tb, mask=consumption)
            still_bad = check_monotone_within_groups(tb.loc[consumption])
            assert not still_bad, f"Monotonicity not restored for {still_bad[:5]}"
            log.info(f"Enforced monotonicity on {len(non_monotone)} income-basis country-years")
        else:
            log.warning(
                f"Non-monotone income-basis distributions (kept, as in the source project): {non_monotone[:10]}"
            )

    tb = recompute_shares(tb)
    return tb


def enforce_monotone(tb: Table, mask: np.ndarray) -> Table:
    """Make bin averages non-decreasing across percentiles, for the masked rows only.

    Isotonic regression (pool-adjacent-violators): the least-squares projection of the fitted
    profile onto the monotone cone, weighted by bin population. A run of violating bins is replaced
    by its weighted average, so the mean is preserved and every non-violating bin keeps its fitted
    value.

    A cumulative maximum would be simpler but is the wrong choice here. The inversion comes from the
    model's own coefficients: p1 is the only percentile with beta below 1 (0.776 against 1.09-1.22
    above it) and it has the worst fit (R^2 0.28), so at the sub-$1 consumption levels where this
    bites it implies a HIGHER income than p2 does. A running maximum would broadcast that single
    least-reliable estimate across the whole violating run; measured over the affected country-years
    it shifts the mean by +0.11% and the within-country MLD by -0.011, against +0.000% and -0.00035
    here. Averaging the violators instead down-weights the outlier, which is what it deserves.
    """
    # `mask` is positional against `tb` as passed, so reset the index (which preserves order) and
    # carry the sort permutation explicitly rather than reordering the table under the mask.
    tb = tb.reset_index(drop=True)
    order = tb.sort_values(["country", "year", "bin_index"]).index.to_numpy()

    avg = tb["avg"].to_numpy(dtype=float)
    pop = tb["pop"].to_numpy(dtype=float)
    n_groups = len(avg) // 109
    assert n_groups * 109 == len(avg), "Rows do not divide into 109-bin country-years."

    grid = avg[order].reshape(n_groups, 109)
    weights = pop[order].reshape(n_groups, 109)
    rows = np.asarray(mask, dtype=bool)[order].reshape(n_groups, 109).any(axis=1)

    # Only the country-years that actually violate need fitting; the rest are already monotone.
    violating = rows & (np.diff(grid, axis=1) < -1e-9).any(axis=1)
    percentiles = np.arange(109)
    fitter = IsotonicRegression(increasing=True)
    for i in np.flatnonzero(violating):
        grid[i] = fitter.fit_transform(percentiles, grid[i], sample_weight=weights[i])

    out = avg.copy()
    out[order] = grid.reshape(-1)
    tb["avg"] = out
    return tb


def graft_top_tail(tb_base: Table, tb_wid: Table) -> Table:
    """The top-adjusted PIP series: above the splice bin, the base series' top is rebuilt with the
    WID post-tax shape, anchored at the base series' own level; the anchor bin and everything
    below keep their base values."""
    anchor = SPLICE_PERCENTILE - 1  # bin p94p95

    # Both series as (country-years x 109) matrices, so the graft is one vectorised expression.
    base_keys, base_avg = to_matrix(tb_base, tb_base["series"].iloc[0])
    shape_keys, shape_avg = to_matrix(tb_wid, TOP_TAIL_SHAPE_SERIES)
    assert base_keys.equals(shape_keys), "Base and shape series cover different country-years."

    assert (shape_avg[:, anchor] > 0).all(), "WID anchor-bin value is zero for some country-year."
    # WID's top as a ratio to its own anchor bin. Dividing by the anchor makes it scale-free, so
    # only WID's SHAPE crosses over — any per-country factor in WID's levels (PPP vintage, price
    # base, per-adult vs per-capita) cancels here.
    ratio = shape_avg / shape_avg[:, [anchor]]
    # Rescale that shape to the base series' own anchor level, above the anchor only; the anchor
    # bin and everything below keep their base values, so the splice is continuous by construction.
    col = np.arange(base_avg.shape[1])
    adjusted = np.where(col > anchor, base_avg[:, [anchor]] * ratio, base_avg)
    assert (np.diff(adjusted[:, anchor:], axis=1) >= 0).all(), "Non-monotone top after the graft."

    tb = tb_base.sort_values(["country", "year", "bin_index"]).reset_index(drop=True)
    tb["avg"] = adjusted.reshape(-1)
    tb["series"] = "pip_income_basis_top_adjusted"
    tb = recompute_shares(tb)
    return tb


def rescale_wid_to_adjusted_pip_means(tb_wid: Table, tb_top_adjusted: Table) -> Table:
    """WID post-tax per capita rescaled, country-year by country-year, to the top-adjusted PIP
    means (each mean computed with its own series' population weights). Multiplying by a constant
    preserves the shape exactly, so this series isolates 'WID's shapes at PIP-side levels'."""
    tb = tb_wid[tb_wid["series"] == TOP_TAIL_SHAPE_SERIES].copy()

    def weighted_means(t: Table) -> Table:
        g = t.assign(income=t["avg"] * t["pop"]).groupby(["country", "year"], observed=True)[["income", "pop"]].sum()
        return (g["income"] / g["pop"]).rename("mean").reset_index()

    # Each side's mean uses its OWN population weights, then the ratio is the per-country factor.
    factors = weighted_means(tb_top_adjusted).merge(
        weighted_means(tb), on=["country", "year"], suffixes=("_target", "_wid")
    )
    assert (factors["mean_wid"] > 0).all(), "Non-positive WID mean for some country-year."
    factors["factor"] = factors["mean_target"] / factors["mean_wid"]

    tb = tb.merge(factors[["country", "year", "factor"]], on=["country", "year"], how="left")
    assert tb["factor"].notna().all()
    tb["avg"] = tb["avg"] * tb["factor"]
    tb = tb.drop(columns="factor")
    tb["series"] = "wid_after_tax_rescaled"
    tb = recompute_shares(tb)
    return tb


def to_matrix(tb: Table, series: str):
    """One series as a (country-years x 109) numpy matrix of bin averages, plus its sorted keys."""
    d = tb[tb["series"] == series].sort_values(["country", "year", "bin_index"]).reset_index(drop=True)
    keys = d.loc[d["bin_index"] == 0, ["country", "year"]].reset_index(drop=True)
    assert len(d) == len(keys) * 109, f"Series {series} has country-years without exactly 109 bins."
    return keys, d["avg"].to_numpy(dtype=float).reshape(-1, 109)


def recompute_shares(tb: Table) -> Table:
    income = tb["avg"] * tb["pop"]
    tb["share"] = income / income.groupby([tb["country"], tb["year"]], observed=True).transform("sum")
    return tb


def check_monotone_within_groups(tb: Table) -> list:
    """Country-years whose bin averages are not non-decreasing (tolerance for float noise)."""
    d = tb.sort_values(["country", "year", "bin_index"])
    diffs = d.groupby(["country", "year"], observed=True)["avg"].diff()
    bad = d.loc[diffs < -1e-9, ["country", "year"]].drop_duplicates()
    return list(bad.itertuples(index=False, name=None))


def mld_decomposition(tb_distributions: Table, tb_pop: Table):
    """Between/within MLD decomposition per (year, series) over the common sample.

    Conventions (from the source project, non-negotiable): country weights are WID's demography
    matched to the series' basis — adult population for per-adult series, total population
    otherwise, INCLUDING for PIP and the derived series; zero incomes are replaced by
    $0.01/day inside this calculation only. The exact decomposition identity
    total = within + between is asserted for every (year, series).
    """
    d = tb_distributions[["country", "year", "series", "p_low", "p_high", "avg"]].copy()
    d = d.merge(tb_pop, on=["country", "year"], how="left")
    assert d["total_population"].notna().all(), "Missing WID population for some country-year in the common sample."

    # Country weights come from WID for every series (see the docstring), so the bin weight is the
    # basis-matched national population times the bin's width.
    basis_is_adult = d["series"].map(SERIES_BASIS).eq("adult").to_numpy()
    ref_pop = np.where(
        basis_is_adult, d["adult_population"].to_numpy(dtype=float), d["total_population"].to_numpy(dtype=float)
    )
    w = ref_pop * (d["p_high"] - d["p_low"]).to_numpy(dtype=float)

    # log(0) is undefined, so zero incomes take the floor — here only, never in the stored
    # distributions. `zero_bin` carries the count so the report can quantify the substitution.
    x = d["avg"].to_numpy(dtype=float)
    zero = x == 0
    x = np.where(zero, ZERO_INCOME_REPLACEMENT, x)

    d["w"] = w
    d["wx"] = w * x
    d["wlnx"] = w * np.log(x)
    d["zero_bin"] = zero

    # Per country: the weighted mean, and MLD within it as ln(mean) - mean of ln(x).
    by_country = d.groupby(["year", "series", "country"], observed=True)[["w", "wx", "wlnx"]].sum()
    by_country["mean"] = by_country["wx"] / by_country["w"]
    by_country["mld_within"] = np.log(by_country["mean"]) - by_country["wlnx"] / by_country["w"]

    total_sums = by_country.groupby(["year", "series"], observed=True)[["w", "wx", "wlnx"]].sum()
    grand_mean = total_sums["wx"] / total_sums["w"]

    g = by_country.reset_index().merge(grand_mean.rename("grand_mean").reset_index(), on=["year", "series"], how="left")
    g = g.merge(total_sums["w"].rename("w_total").reset_index(), on=["year", "series"], how="left")
    # The decomposition itself: both components are population-share-weighted sums, the between
    # term over each country's distance from the global mean, the within term over its internal MLD.
    g["population_share"] = g["w"] / g["w_total"]
    g["between_term"] = g["population_share"] * np.log(g["grand_mean"] / g["mean"])
    g["within_term"] = g["population_share"] * g["mld_within"]

    tb_decomposition = (
        g.groupby(["year", "series"], observed=True)
        .agg(
            mld_between=("between_term", "sum"),
            mld_within=("within_term", "sum"),
            grand_mean=("grand_mean", "first"),
            num_countries=("country", "nunique"),
            population=("w", "sum"),
        )
        .reset_index()
    )
    # The total, computed independently from the bin level.
    totals = np.log(grand_mean) - total_sums["wlnx"] / total_sums["w"]
    tb_decomposition = tb_decomposition.merge(
        totals.rename("mld_total").reset_index(), on=["year", "series"], how="left"
    )
    zero_counts = (
        d.groupby(["year", "series"], observed=True)["zero_bin"].sum().rename("num_zero_bins_replaced").reset_index()
    )
    tb_decomposition = tb_decomposition.merge(zero_counts, on=["year", "series"], how="left")

    # Exact decomposition identity — if this trips, the code is wrong.
    gap = (tb_decomposition["mld_total"] - tb_decomposition["mld_between"] - tb_decomposition["mld_within"]).abs()
    assert gap.max() < 1e-9, f"Decomposition identity violated (max gap {gap.max():.2e})."
    tb_decomposition["between_share"] = tb_decomposition["mld_between"] / tb_decomposition["mld_total"]

    tb_decomposition = Table(
        tb_decomposition[
            [
                "year",
                "series",
                "mld_total",
                "mld_between",
                "mld_within",
                "between_share",
                "grand_mean",
                "num_countries",
                "num_zero_bins_replaced",
                "population",
            ]
        ]
    )
    tb_decomposition.metadata.short_name = "inequality_decomposition"

    tb_by_country = Table(
        g.rename(columns={"w": "population_weight"})[
            ["country", "year", "series", "mean", "mld_within", "population_weight"]
        ]
    )
    tb_by_country.metadata.short_name = "inequality_decomposition_by_country"
    return tb_decomposition, tb_by_country


def sanity_check_raw_series(tb_wid: Table, tb_pip: Table, tb_dist: Table, tb_pop: Table, tb_incomes_wid: Table) -> None:
    labels = set(wid_bin_labels())

    for tb, name in [(tb_wid, "WID"), (tb_pip, "PIP")]:
        counts = tb.groupby(["series", "country", "year"], observed=True).size()
        assert (counts == 109).all(), f"{name} country-years without exactly 109 bins."
        assert set(tb["percentile"].unique()) == labels, f"{name} bin labels do not match the canonical 109."
        widths = (
            tb.assign(width=tb["p_high"] - tb["p_low"])
            .groupby(["series", "country", "year"], observed=True)["width"]
            .sum()
        )
        assert np.allclose(widths, 1.0), f"{name} bin widths do not sum to 1."
        assert tb["avg"].notna().all() and (tb["avg"] >= 0).all(), f"{name} has missing or negative bin averages."
        assert (tb["pop"] > 0).all(), f"{name} has non-positive bin populations."

    # Shares as fetched/computed sum to ~1 per country-year.
    share_sums = tb_wid.groupby(["series", "country", "year"], observed=True)["share"].sum()
    assert (share_sums - 1).abs().max() < 0.005, "WID income shares do not sum to 1."
    share_sums = tb_pip.groupby(["country", "year"], observed=True)["share"].sum()
    assert (share_sums - 1).abs().max() < 1e-9, "PIP income shares do not sum to 1."

    # Guard against the historical missing-PPP-conversion bug: values must be plausible daily
    # dollar amounts (the bug produced ~$200M/day top incomes).
    top = tb_wid[tb_wid["percentile"] == "p99.9p100"]
    assert top["avg"].max() < 500_000, f"Implausibly high WID top incomes: max {top['avg'].max():,.0f} $/day."

    # Per-adult -> per-capita conversion: the ratio must equal the adult share of population.
    pa = tb_wid[tb_wid["series"] == "wid_before_tax_per_adult"].set_index(["country", "year", "percentile"])["avg"]
    pc = tb_wid[tb_wid["series"] == "wid_before_tax_per_capita"].set_index(["country", "year", "percentile"])["avg"]
    ratio = (pc / pa).dropna().reset_index().merge(tb_pop, on=["country", "year"])
    adult_share = ratio["adult_population"] / ratio["total_population"]
    assert np.allclose(ratio["avg"], adult_share, rtol=1e-6), "Per-capita conversion does not match the adult share."

    # DINA identity: post-tax national income redistributes everything, so country totals match
    # pre-tax totals. Over a long extrapolated panel small gaps exist; only their share is capped.
    def country_totals(series: str) -> pd.Series:
        t = tb_wid[tb_wid["series"] == series]
        return t.assign(income=t["avg"] * t["pop"]).groupby(["country", "year"], observed=True)["income"].sum()

    pre = country_totals("wid_before_tax_per_adult")
    post = country_totals("wid_after_tax_per_adult")
    gap = ((post - pre).abs() / pre).dropna()
    share_violating = (gap > 0.005).mean()
    assert share_violating < 0.01, f"Post-tax != pre-tax national income in {share_violating:.1%} of country-years."

    # Spot-check ~20 sampled country-year means against WID's own published average (p0p100,
    # per adult, daily), from the independently processed `incomes` table.
    published = tb_incomes_wid[
        (tb_incomes_wid["welfare_type"] == "before tax")
        & (tb_incomes_wid["extrapolated"] == EXTRAPOLATED_CHOICE)
        & (tb_incomes_wid["period"] == "day")
        & tb_incomes_wid["mean"].notna()
    ][["country", "year", "mean"]]
    computed = (
        tb_wid[tb_wid["series"] == "wid_before_tax_per_adult"]
        .assign(income=lambda t: t["avg"] * (t["p_high"] - t["p_low"]))
        .groupby(["country", "year"], observed=True)["income"]
        .sum()
        .reset_index()
    )
    matched = computed.merge(published, on=["country", "year"])
    assert len(matched) > 100, "Too few country-years to cross-check WID means against published values."
    sample = matched.sample(20, random_state=42)
    rel = (sample["income"] - sample["mean"]).abs() / sample["mean"]
    assert rel.max() < 0.01, f"Computed WID means deviate from published means (max {rel.max():.2%})."


def sanity_check_outputs(tb_distributions: Table, tb_decomposition: Table, tb_model: Table, countries: list) -> None:
    counts = tb_distributions.groupby(["series", "country", "year"], observed=True).size()
    assert (counts == 109).all(), "Output country-years without exactly 109 bins."
    assert set(tb_distributions["country"].unique()) == set(countries)

    share_sums = tb_distributions.groupby(["series", "country", "year"], observed=True)["share"].sum()
    assert (share_sums - 1).abs().max() < 0.005, "Output income shares do not sum to 1."

    assert (tb_decomposition["between_share"] > 0).all() and (tb_decomposition["between_share"] < 1).all()

    # By construction the rescaled WID series has the top-adjusted PIP series' country means,
    # hence (nearly) its between component — a free, powerful check. Exactness is broken only by
    # the $0.01/day floor applied inside the MLD: the rescaled series has zero bins (inherited
    # from WID) while the PIP-side series has none, so the floored means differ microscopically.
    piv = tb_decomposition.pivot(index="year", columns="series", values="mld_between")
    gap = (piv["wid_after_tax_rescaled"] - piv[RESCALE_MEAN_SERIES]).abs()
    log.info(f"Rescaled-vs-{RESCALE_MEAN_SERIES} between-component max gap (floor artifact): {gap.max():.2e}")
    assert gap.max() < 1e-3, f"Rescaled series' between component deviates from {RESCALE_MEAN_SERIES}'s."

    assert tb_model["beta"].between(0.5, 2).all(), "Consumption->income betas outside the plausible range."
    assert tb_model["r_squared"].median() > 0.8, "Consumption->income fit quality collapsed."
    assert len(tb_model) == 100


def format_with_origins(tb: Table, keys: list, short_name: str, origins) -> Table:
    tb = tb.copy()
    if "bin_index" in tb.columns:
        tb = tb.drop(columns="bin_index")
    tb = tb.format(keys, short_name=short_name)
    for col in tb.columns:
        tb[col].metadata.origins = list(origins)
    return tb
