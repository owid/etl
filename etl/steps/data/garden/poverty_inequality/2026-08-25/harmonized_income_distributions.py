"""Harmonized PIP and WID income distributions on a common 109-bin structure, with bridging series
and a between/within-country decomposition of global inequality.

This step began as a port of the data pipeline behind Joe Hasell's PIP-vs-WID triangulation work
(github.com/JoeHasell/prague-pip-wid, `data/scripts/`), extended from a single year to the full panel
of years both sources cover, so the exercise re-runs on every PIP/WID data update.

It is no longer a faithful port: four deliberate departures make the output differ from that
project, each documented where it happens and in
ai/adversarial-review-harmonized_income_distributions-2026-08-26.md.

  - The welfare basis of a country-year follows PIP's own decision tree. The source project
    preferred INCOME where PIP publishes both concepts, which mislabelled the 88 country-years
    whose PIP distribution is in fact consumption (see build_welfare_basis).
  - Fitted income profiles are made monotone (see ENFORCE_MONOTONE_INCOME_BASIS).
  - WID is converted at the PPP year WID prices its series in, not a fixed one (in the snapshot).
  - Countries are weighted by an independent demographic yardstick — OWID's population dataset and
    UN WPP's adults aged 20+ — rather than by WID's own counts (see load_reference_population).
    Measured to move every 2023 between share by at most 0.015pp, since WID's counts are UN WPP too.

Exactly reproducing the source project is therefore no longer possible from a switch, and is not
meant to be: two of the four departures fix errors in its numbers.

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
   series, over the common sample of countries. Two conventions are baked in: country weights come
   from one demographic yardstick independent of both sources (OWID population, UN WPP adults 20+),
   MATCHED TO THE SERIES' BASIS (adults for per-adult series, total population otherwise —
   including for PIP), so the sources' demographic disagreements never enter the comparison; and
   zero incomes are replaced by
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
# extrapolations WID covers 54 countries in 2023 vs 226 with them). PIP's thousand-bins panel is
# itself lined up/extrapolated by the World Bank, so this is the symmetric choice.
# NOTE: that 54 was 17 before wid/2026-09-02. WID retired the extrapolation flag the "no" slice used
# to come from, so it is now defined by WID's own data-quality score (income scored >= 3); the
# argument for "yes" is unchanged, but re-measure the counts whenever the WID version moves.
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
# (see enforce_monotone). False keeps the raw fitted profile, inversions included, as the source
# project did — useful for isolating this one effect, though it no longer reproduces that
# project's output on its own, since the welfare basis differs too.
ENFORCE_MONOTONE_INCOME_BASIS = True

# How to assign each country's welfare basis (income vs consumption) across the panel. PIP's
# lined-up values for a non-survey year were themselves produced by choosing a concept, so the goal
# is to RECOVER that choice rather than approximate it:
#   "pip_decision_tree": PIP's own rule — the surveyed concept at a survey year, the concept whose
#                        surveys bracket the gap where one does, the nearest survey's otherwise.
#                        Both branches are verified against PIP's published distributions; see
#                        assign_by_pip_decision_tree.
#   "nearest_survey":    always the nearest survey's concept. Differs from the tree on 24
#                        country-years across 8 countries, and the evidence goes against it in all
#                        24 (they are interpolated years, where the bracketing concept is the only
#                        one PIP could have used). Kept to measure that sensitivity.
#   "latest_survey":     one static type per country, from its most recent survey — the shape the
#                        source project used for its single reference year. Kept for comparison, but
#                        it does NOT reproduce that project: it preferred income where both concepts
#                        exist, which is the mislabelling this step corrects.
WELFARE_ASSIGNMENT = "pip_decision_tree"

# Equidistant-tie anchors pinned by measurement where the earlier-survey rule disagrees with the
# published bins. Kyrgyzstan 1999 sits one year from a 1998 income survey and one from a 2000
# consumption survey, and its bins carry the 2000 consumption survey's Gini (to 4.7e-10) — almost
# certainly a survey reference period the whole-year distance cannot see. The other four
# identifiable ties all went to the earlier survey, so the general rule stays "earlier" and this
# one exception is pinned as (welfare_type, survey_year_used). sanity_check_welfare_basis
# re-verifies it against the bins on every run, so a PIP revision that changes the anchor fails
# the build rather than silently outdating this entry.
TIE_OVERRIDES = {("Kyrgyzstan", 1999): ("consumption", 2000)}

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
# WID's "adults" are ages 20+. UN WPP publishes no 20+ group, so it is the sum of these 17 groups —
# NOT "15+ minus 15-19": the open-ended 15+/18+/65+ groups omit the 100+ bucket.
ADULT_AGE_GROUPS = [
    "20-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55-59", "60-64",
    "65-69", "70-74", "75-79", "80-84", "85-89", "90-94", "95-99", "100+",
]  # fmt: skip
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

    # WID's own counts, kept for one job: converting its per-adult series to per capita.
    tb_pop_wid = ds_wid.read("population", safe_types=False)

    # UN WPP population by age, the source of the adult (20+) yardstick; total population comes
    # from OWID's population dataset through paths.regions inside load_reference_population.
    ds_un_wpp = paths.load_dataset("un_wpp")
    tb_un_wpp = ds_un_wpp.read("population", safe_types=False)

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

    # The demographic yardstick every series is weighted by (WID's column names, so consumers do
    # not care which source it came from). The 12.9M-row UN table is released right after.
    candidates = sorted(set(tb_dist["country"].astype(str)) | set(tb_bins["country"].astype(str)))
    tb_pop = load_reference_population(tb_un_wpp, candidates, FIRST_YEAR, last_year)
    un_wpp_origins = unique_origins(tb_un_wpp["population"].metadata.origins)
    del tb_un_wpp

    origins = {
        "pip_bins": tb_bins["avg"].metadata.origins,
        "pip_percentiles": tb_percentiles["avg"].metadata.origins,
        "wid": tb_dist["avg"].metadata.origins,
        "wid_population": tb_pop_wid["adult_population"].metadata.origins,
        "population": tb_pop["total_population"].metadata.origins,
        "un_wpp": un_wpp_origins,
    }

    tb_wid = process_wid_distributions(tb_dist, tb_pop, tb_pop_wid, bins_lookup, last_year)
    tb_pip = aggregate_pip_bins(tb_bins, bins_lookup, last_year)

    sanity_check_raw_series(tb_wid, tb_pip, tb_dist, tb_pop, tb_pop_wid, tb_incomes_wid)

    # The common sample: countries present in PIP and every raw WID series in every common year
    # (both sources are complete panels over the range, so balancing costs ~nothing — asserted).
    countries = common_sample(tb_wid, tb_pip)
    assert set(countries) <= set(tb_pop["country"]), "A common-sample country has no reference population."
    tb_wid = tb_wid[tb_wid["country"].isin(countries)].reset_index(drop=True)
    tb_pip = tb_pip[tb_pip["country"].isin(countries)].reset_index(drop=True)

    # Consumption -> income model and each country-year's welfare basis.
    tb_model = fit_consumption_income_model(tb_percentiles)
    tb_welfare = build_welfare_basis(tb_percentiles, countries, FIRST_YEAR, last_year)
    sanity_check_welfare_basis(tb_welfare, tb_percentiles, tb_pip)

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
    # Every input, PIP's percentiles included: the income-basis series are built on their regression
    # and welfare labels. One origin per snapshot (see unique_origins) — WID's distributional and
    # population snapshots are different inputs and both stay cited.
    all_origins = sorted(
        unique_origins([o for key in origins for o in origins[key]]),
        key=lambda o: o.producer,
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


def load_reference_population(tb_un_wpp: Table, countries: list, first_year: int, last_year: int) -> Table:
    """The demographic yardstick every series is weighted by: for each country and panel year, adults
    aged 20+ from UN World Population Prospects and total population from OWID's population dataset
    (itself UN WPP for these years), returned as `adult_population` and `total_population` — WID's
    column names, so the consumers do not care which source they got.

    Why a yardstick independent of both sources: the MLD decomposition weights countries by
    population for EVERY series, PIP's included, so that the sources' demographic disagreements stay
    out of the PIP-vs-WID comparison; taking that yardstick from neither source makes the point
    cleanly. WID's per-adult series are still converted to per capita with WID's OWN adult share
    (see process_wid_distributions): their incomes are defined against WID's adult counts, so only
    WID's ratio keeps the per-capita series consistent with WID's national income.

    In practice WID's own counts are UN WPP too: across the 211-country panel the ratio of these
    counts to WID's has a median of 1.0000 and a 99th percentile of 1.002. Two differences are
    material. Togo's count UN cut by 12-14% in a January-2026 interim update that WID's vintage
    predates. And WID's France is 3.2% larger, by exactly the population of the five overseas
    departments (Réunion, Guadeloupe, Martinique, French Guiana, Mayotte), which WID folds into
    France and UN WPP lists separately. Switching the weights moved every between share in the
    panel by at most 0.02pp.
    """
    # STEP 1 — the UN slice: both sexes, estimates (to 2023) then the medium projection (2024 on,
    # no overlapping year), the panel years, and only the ages needed. Compare the categoricals
    # directly (casting them to str first costs seconds); cast keys and counts afterwards.
    un = tb_un_wpp[
        (tb_un_wpp["sex"] == "all")
        & tb_un_wpp["variant"].isin(["estimates", "medium"])
        & tb_un_wpp["year"].between(first_year, last_year)
        & tb_un_wpp["age"].isin(ADULT_AGE_GROUPS + ["all"])
    ]
    un = pd.DataFrame(
        {
            "country": un["country"].astype(str),
            "year": un["year"].astype(int),
            "age": un["age"].astype(str),
            "population": un["population"].astype(np.float64),
        }
    )

    # Countries only. UN WPP and OWID both carry aggregates (UN development groups, World Bank income
    # groups, OWID regions) whose compositions differ between the two, so they would fail the totals
    # check below; none is needed, since the yardstick is only ever joined onto country rows.
    un = un[un["country"].isin(countries)]

    # STEP 2 — adults 20+: the 17 groups summed, and every country-year must have all 17.
    adults = (
        un[un["age"] != "all"]
        .groupby(["country", "year"])["population"]
        .agg(n="size", adult_population="sum")
        .reset_index()
    )
    assert (adults["n"] == 17).all(), "UN WPP is missing an adult age group for some country-year."
    assert not adults.duplicated(["country", "year"]).any(), "UN WPP adults are not unique on (country, year)."

    # STEP 3 — total population from OWID's population dataset, through the standard helper (a left
    # join on country and year that also attaches the dataset's collapsed origin). UN aggregates
    # and entities OWID does not track come back empty and are dropped, by name.
    tb = paths.regions.add_population(
        Table(adults[["country", "year", "adult_population"]]),
        population_col="total_population",
        warn_on_missing_countries=False,
    )
    missing = sorted(tb.loc[tb["total_population"].isna(), "country"].unique())
    if missing:
        log.info(f"UN WPP entities without OWID population (dropped from the yardstick): {missing}")
        tb = tb[tb["total_population"].notna()].reset_index(drop=True)

    # STEP 4 — the two sources must agree on totals, or the adult share below mixes demographies.
    un_total = un[un["age"] == "all"].set_index(["country", "year"])["population"]
    matched = un_total.reindex(pd.MultiIndex.from_frame(tb[["country", "year"]])).to_numpy()
    total = tb["total_population"].to_numpy(dtype=float)
    assert np.isfinite(matched).all(), "A yardstick country-year has no UN WPP total."
    assert (np.abs(total / matched - 1) < 1e-3).all(), "OWID population and UN WPP totals disagree by >0.1%."

    # STEP 5 — plausibility: the adult share of a population sits between roughly 0.36 (Rwanda in
    # the mid-1990s) and 0.92 (the Vatican).
    adult_share = tb["adult_population"].to_numpy(dtype=float) / total
    assert ((adult_share > 0.3) & (adult_share < 0.95)).all(), "Implausible adult share in the reference population."

    # STEP 6 — float64 counts, keeping the metadata the helper attached to total_population.
    for col in ("adult_population", "total_population"):
        meta = tb[col].metadata
        tb[col] = tb[col].astype(np.float64)
        tb[col].metadata = meta
    return tb[["country", "year", "adult_population", "total_population"]]


def unique_origins(origins: list) -> list:
    """One origin per snapshot: (producer, title, title_snapshot, date_published).

    A column assembled from several inputs can carry the same snapshot's origin several times (UN
    WPP's population table lists its "Estimates 1950-2023" file twice), which this collapses. It
    must NOT collapse different snapshots of one product: WID's distributional data and WID's
    population counts share producer, title and date and differ only in title_snapshot, and both
    are inputs here, so both stay cited."""
    return list({(o.producer, o.title, o.title_snapshot, o.date_published): o for o in origins}.values())


def process_wid_distributions(
    tb_dist: Table, tb_pop: Table, tb_pop_wid: Table, bins_lookup: Table, last_year: int
) -> Table:
    """The four raw WID series: filter to the 109-bin structure, convert annual -> daily and
    per-adult -> per-capita (with WID's own adult share), and attach bin populations from the
    reference yardstick (`tb_pop`)."""
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
    # Two populations per country-year: the reference yardstick (for bin populations, i.e. the
    # weights) and WID's own counts (for the per-adult -> per-capita conversion only).
    d = d.merge(tb_pop, on=["country", "year"], how="left")
    d = d.merge(
        tb_pop_wid.rename(
            columns={"adult_population": "wid_adult_population", "total_population": "wid_total_population"}
        ),
        on=["country", "year"],
        how="left",
    )
    # Countries in the WID distribution but missing either population cannot be used at all (WID's
    # regions, historical states, and a few territories); none is in PIP, so the common sample is
    # unaffected — logged, not asserted.
    unusable = d["total_population"].isna() | d["wid_total_population"].isna()
    no_population = sorted(d.loc[unusable, "country"].unique())
    if no_population:
        log.info(f"WID distribution countries without reference or WID population (excluded): {no_population}")
        d = d[~unusable]

    # Provenance flag: a WID country-year counts as extrapolated unless it also appears in WID's
    # non-extrapolated series (i.e. it is anchored in data WID rates as directly supporting it).
    # Since wid/2026-09-02 that slice is defined by WID's data-quality score (income scored >= 3)
    # rather than by WID's retired extrapolation flag, so the flag is stricter for some country-years
    # and looser for others; the counts above record the net effect on 2023.
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
    # WID's OWN adult share, deliberately: WID's per-adult incomes are defined against WID's adult
    # counts, so only WID's ratio keeps the per-capita series consistent with WID's national income
    # ("apples to apples"). The float64 cast matters: these arrive as nullable dtypes, whose masked
    # arithmetic is orders of magnitude slower on a table this size.
    adult_share = (d["wid_adult_population"] / d["wid_total_population"]).astype(np.float64)
    d["avg_daily_per_capita"] = d["avg_daily_per_adult"] * adult_share

    # Bin populations: each bin covers a known slice of the distribution, so it holds that share of
    # the country — counted with the REFERENCE yardstick, since these become the weights. Both bases
    # are built because SERIES_BASIS picks one per series below.
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


def resolve_survey_concepts(tb_surveys: Table) -> pd.Series:
    """One welfare concept per (country, survey year): consumption where PIP publishes both.

    That is §5.4's preference, and it is what the thousand-bins values ARE for the dual years —
    verified, not assumed: their bin means sit 0.03% from PIP's published consumption mean and 18%
    from its income mean, for all 88 of them. Every welfare-assignment mode works from this series,
    so the rule lives in exactly one place.
    """
    offered = tb_surveys.groupby(["country", "year"], observed=True)["welfare_type"].agg(set)
    return offered.apply(lambda t: "consumption" if "consumption" in t else "income")


def assign_by_pip_decision_tree(resolved: pd.Series, grid: Table) -> Table:
    """PIP's income/consumption decision tree, per country-year.

        is there a survey AT this year?
          yes -> its concept, taking consumption if the year offers both (handbook §5.4)
          no  -> a survey of ONE concept both before and after? (consumption checked first)
                   yes -> that concept: PIP interpolates between those two surveys
                   no  -> the nearest survey's concept: PIP extrapolates from it
                          (equidistant surveys go to the earlier one)

    https://datanalytics.worldbank.org/PIP-Methodology/lineupestimates.html#inccon

    The label has to be inferred because PIP publishes none for a filled-in year: the bins are
    labelled "welfare (income or consumption)", and only survey years carry a `welfare_type` in
    PIP's `complete_series`.

    TWO TRAPS, neither visible in the published diagram:

    1. "Both sides" tests each survey year's RESOLVED concept — consumption where PIP publishes
       both, which is what the `resolved` input from resolve_survey_concepts encodes — never raw
       availability, since a year publishing both is a consumption endpoint only.
       On raw availability a dual-concept year anchors an income interpolation PIP never performed:
       it calls Haiti 2007-2011 income, where the bins carry the 2012 consumption survey's shape.
    2. The test scans ALL earlier and later surveys, not the adjacent pair. Belize 2000-2017 sits
       between a 1999 income and a 2018 consumption survey, but a 1995 consumption survey spans
       those years too, and PIP interpolates accordingly.

    WHY EACH BRANCH IS TRUSTED. The bins are an inversion of PIP's lined-up poverty curve (Mahler,
    Yonzan & Lakner 2022, WP 10198, `01-QueryPIP.do`), and its lineup extrapolates by scaling ONE
    survey but interpolates by averaging TWO. Scaling preserves that survey's Gini exactly;
    averaging preserves neither. So each branch leaves its own fingerprint, measured here on the
    2026-03 vintage:

        the year is ...                 country-years   carries one survey's Gini   branch
        spanned by one concept                  1,878          0     (  0%)         interpolated
        in a gap, spanned by neither               78         71     ( 91%)         nearest survey
        outside the survey range                2,166      1,612     ( 74%)         nearest survey

    Ties resolve to the earlier survey because that is where PIP puts them: across a concept change
    the Gini forms two flat plateaus, and the equidistant year falls in the earlier one (Namibia
    1998, Saint Lucia 2005, Nicaragua 2007). The one measured exception is pinned in TIE_OVERRIDES
    rather than bent into the rule.
    """
    # STEP 1 — the panel to label: every (country, year) in the grid, as plain pandas. The catalog
    # Table's groupby does not yield (key, group) pairs, so it cannot drive the loop below.
    panel = pd.DataFrame({"country": grid["country"].astype(str), "year": grid["year"].astype(int)})
    known = set(resolved.index.get_level_values("country"))

    rows = []
    for country, block in panel.groupby("country", observed=True):
        # A country with no survey at all gets no label; build_welfare_basis passes it through.
        if country not in known:
            continue

        # STEP 2 — this country's surveys as two aligned arrays, sorted by year: when each survey
        # is, and the one concept it resolves to. Sorted order is what makes STEP 3's tie-break and
        # STEP 5's before/after masks work.
        surveys = np.array(sorted(resolved.loc[country].index))
        concepts = np.array([resolved.loc[(country, int(y))] for y in surveys])
        years = block["year"].to_numpy()

        # STEP 3 — the nearest survey for every panel year at once, as an index into `surveys`.
        # Two properties come free from argmin on a sorted array: it returns the FIRST minimum, so
        # equidistant surveys resolve to the earlier one (which is what PIP does, TIE_OVERRIDES
        # aside); and a year that IS a survey year has distance 0, a unique minimum, so it
        # resolves to itself.
        nearest = np.abs(surveys[None, :] - years[:, None]).argmin(axis=1)

        for year, near_i in zip(years, nearest):
            # STEP 4 — start from the nearest survey. For a survey year that IS the answer (its own
            # concept, per STEP 3), and for a year PIP extrapolated to it is the fallback branch.
            survey_used, welfare = int(surveys[near_i]), concepts[near_i]

            # STEP 5 — for a non-survey year, the interpolation branch overrides that fallback if it
            # applies: one concept having a survey both before and after means PIP interpolated
            # within that concept, so the year carries it. Consumption is tested first (§5.4), and
            # the masks span ALL earlier and ALL later surveys rather than the adjacent pair — trap
            # 2 in the docstring. Note this beats proximity: a nearer survey of the other concept
            # does not win, because PIP was interpolating the spanning concept's series through
            # this year.
            if year not in surveys:
                earlier, later = surveys < year, surveys > year
                for concept in ("consumption", "income"):
                    spans = concepts == concept
                    if (spans & earlier).any() and (spans & later).any():
                        welfare = concept
                        break

            # `survey_used` stays the nearest survey either way. It is published as
            # `survey_year_used`, and its metadata says so: for an interpolated year the concept
            # came from the spanning pair instead, not from this survey.
            rows.append((country, int(year), welfare, survey_used))

    out = pd.DataFrame(rows, columns=["country", "year", "welfare_type", "survey_year_used"])

    # The measured tie exceptions (see TIE_OVERRIDES): the anchor the bins identify, not the rule's.
    for (country, year), (concept, survey) in TIE_OVERRIDES.items():
        out.loc[(out["country"] == country) & (out["year"] == year), ["welfare_type", "survey_year_used"]] = [
            concept,
            survey,
        ]

    return Table(out)


def build_welfare_basis(tb_percentiles: Table, countries: list, first_year: int, last_year: int) -> Table:
    """Each (country, year)'s welfare basis — income or consumption — by PIP's own decision tree
    (see assign_by_pip_decision_tree). Survey years offering both count as CONSUMPTION.

    Why the label matters: it decides whether the consumption -> income transform runs on that
    country-year, so it has to describe what the thousand-bins values ARE, not what would be
    convenient. Getting it backwards either leaves consumption sitting in an income series or
    transforms income that needed no transforming.

    Returns one row per (country, year) for every country with at least one national PIP survey:

    - `welfare_type` — the concept the PIP thousand-bins values are on for this country-year,
      "income" or "consumption". A statement about the data, not the country: a country's rows can
      carry different values in different years, following its surveys.
    - `survey_year_used` — the nearest national PIP survey to this year, equidistant ties to the
      earlier one (TIE_OVERRIDES excepted). Its meaning depends on the branch that labelled the year: for a survey year it
      is the year itself, and for an extrapolated year it is the survey PIP scaled the estimate
      from — in both cases also the source of `welfare_type`. For an INTERPOLATED year the concept
      comes from the pair of surveys spanning the gap instead, so there this column only measures
      how far the year sits from data, and consumers filtering on survey distance is exactly why
      it is published.
    - `adjusted` — True where `welfare_type` is "consumption"; the flag
      adjust_consumption_to_income keys on when deciding which rows to map through the
      consumption -> income model. Published so which country-years were mapped stays visible.

    Two cases, and PIP's data settles both:

    - A SURVEY year offering both concepts. The bins are consumption, verified rather than assumed:
      their means sit 0.03% from PIP's published consumption mean and 18% from its income mean, for
      all 88 such country-years, with the two concepts a median 19.5% apart so none is a close call.
      That matches PIP's stated rule, handbook §5.4: "Due to its closer connection to welfare,
      whenever both income and consumption estimates are available for a given reference year,
      consumption estimates are preferred"
      (https://datanalytics.worldbank.org/PIP-Methodology/lineupestimates.html#inccon).
    - A NON-SURVEY year. PIP attaches no concept to these at all, so it has to be inferred, and
      PIP's decision tree is the rule — with both of its branches verified against the published
      distributions rather than taken on trust. See assign_by_pip_decision_tree.

    This is where the step departs from the source project, which labelled the dual survey years
    income. That skipped the transform and carried consumption into `pip_income_basis` as though it
    were already income, roughly 18% low; labelling them consumption moves the 2023 between share by
    -0.24pp against that project's figure.

    A country's basis is therefore assigned per country-year, not once per country: 29 countries
    here switch concept across the panel, because their anchor surveys do. That is consistent with
    PIP, whose handbook §5.2 rule "Interpolations are never done between consumption and income
    aggregates" is narrower than it first sounds — it sits in a section about interpolating a survey
    MEAN between two surveys with national-accounts growth, so it forbids one such calculation
    spanning the two concepts, not a series changing concept over time.

    What PIP never does is CONVERT one concept into the other — it selects between measured series,
    it does not estimate an income distribution from a consumption one. That is what this step's
    model does, and it sits outside PIP's methodology rather than extending it, which is why its fit
    and its thin estimation sample are reported rather than buried (see
    fit_consumption_income_model).

    sanity_check_welfare_basis asserts the label-matches-the-bins invariant on every run, twice
    over: survey years against PIP's published per-concept means, and extrapolated years against
    the Gini fingerprint of the survey they were scaled from. Interpolated years carry no
    fingerprint; their labels rest on the spanning logic in assign_by_pip_decision_tree, whose
    evidence is in its docstring.
    """
    d = national_survey_percentiles(tb_percentiles)

    # One concept per survey year, resolved once for every mode below (see resolve_survey_concepts).
    resolved = resolve_survey_concepts(d)

    # A complete country x year grid, so every panel year gets a basis even without a survey. Only
    # countries with at least one survey can be labelled; the rest are passed through downstream.
    years = Table({"year": range(first_year, last_year + 1)})
    grid = (
        d.loc[d["country"].isin(countries), ["country"]]
        .drop_duplicates()
        .merge(years, how="cross")
        .sort_values(["country", "year"])
    )

    # No mode caps the distance to the survey it draws on — up to 32 years. That is deliberate, and
    # measured: PIP's own published country-years sit up to the same 32 years from a survey (median
    # 1, 90th percentile 9, 99th 23), because this panel IS PIP's coverage, and PIP picks the concept
    # from that same anchor. Capping would make this step diverge from the source, not follow it.
    # `survey_year_used` is published so consumers can filter if they want to.
    if WELFARE_ASSIGNMENT == "pip_decision_tree":
        tb = assign_by_pip_decision_tree(resolved, grid)
    else:
        # The two comparison modes work from the survey years as a flat table.
        types = Table(resolved.reset_index().rename(columns={"year": "survey_year_used"}))
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

    # `adjusted` is the flag the consumption->income transform keys on:
    # adjust_consumption_to_income left-joins it onto the PIP bins and maps `avg` through the
    # fitted model only where it is True. It is published in the table, not kept internal, so
    # readers can see exactly which country-years were mapped.
    tb["adjusted"] = tb["welfare_type"] == "consumption"

    # Two diagnostics for the log, so a PIP update that moves either is noticed at build time:
    # countries whose basis switches concept across the panel (29 on the 2026-06 PIP vintage —
    # expected, since a country's anchor surveys can change concept over time), and countries in
    # the common sample with no national PIP survey at all (40). The latter get no row here;
    # downstream, the transform's left-join fills their flag with False, so their PIP values pass
    # through untouched.
    n_switch = tb.groupby("country", observed=True)["welfare_type"].nunique().gt(1).sum()
    missing = sorted(set(countries) - set(tb["country"].unique()))
    log.info(
        f"Welfare basis ({WELFARE_ASSIGNMENT}): {tb['country'].nunique()} countries in the lookup, "
        f"{n_switch} switch basis across the panel; not in the lookup (passed through): {len(missing)}"
    )

    # The assignment modes hand back plain pandas objects; re-wrap as a Table and set the
    # short_name so create_dataset can attach this table's .meta.yml metadata.
    tb = Table(tb)
    tb.metadata.short_name = "pip_welfare_basis"

    # A fixed column selection and order — the modes build `tb` with different shapes (merge_asof
    # leaves join leftovers) — and a clean RangeIndex for the .format() call in run().
    return tb[["country", "year", "welfare_type", "survey_year_used", "adjusted"]].reset_index(drop=True)


def sanity_check_welfare_basis(tb_welfare: Table, tb_percentiles: Table, tb_pip: Table) -> None:
    """The assigned welfare type must match what the PIP bins actually contain. Two parts:

    1. SURVEY years, via the mean: where PIP publishes both concepts, compare the year's bin mean
       against the mean each concept's percentiles imply, and check the assigned label is the
       closer one (only where the two concepts differ enough to discriminate).
    2. EXTRAPOLATED years, via the Gini: PIP fills these by scaling one survey's distribution,
       which leaves its Gini untouched — so wherever a non-survey year's Gini equals exactly one
       concept's survey Gini, the label must be that concept. This guards the tree's fallback
       branch and its tie-break on every run; it is the check that fails on a label drawn from the
       wrong side of a gap (e.g. Haiti 2007-2011 labelled income when the bins carry the 2012
       consumption survey's shape). Interpolated years match no survey and are skipped — their
       labels rest on the spanning logic in assign_by_pip_decision_tree, whose evidence is in its
       docstring.
    """
    p = national_survey_percentiles(tb_percentiles)
    per_type = p.groupby(["country", "year", "welfare_type"], observed=True)["avg"].mean().unstack("welfare_type")
    per_type = per_type.dropna(subset=["consumption", "income"], how="any")
    if per_type.empty:
        return

    bins = tb_pip.assign(w=tb_pip["avg"] * tb_pip["pop"])
    bin_mean = bins.groupby(["country", "year"], observed=True)[["w", "pop"]].sum().pipe(lambda t: t["w"] / t["pop"])

    d = per_type.join(bin_mean.rename("bins"), how="inner").dropna()
    # Only where the two types are far enough apart for the comparison to mean anything.
    d = d[(d["income"] - d["consumption"]).abs() / d["consumption"] > 0.02]
    if d.empty:
        return

    closer = np.where((d["bins"] - d["income"]).abs() < (d["bins"] - d["consumption"]).abs(), "income", "consumption")
    assigned = tb_welfare.set_index(["country", "year"])["welfare_type"].reindex(d.index).to_numpy(dtype=object)
    mismatch = (assigned != closer) & (assigned != None)  # noqa: E711 - country-years absent from the lookup
    if mismatch.any():
        examples = [
            f"{c} {int(y)}: labelled {a}, bins match {b}"
            for (c, y), a, b in zip(d.index[mismatch], assigned[mismatch], closer[mismatch])
        ][:5]
        raise AssertionError(
            f"Welfare basis disagrees with the PIP bins for {int(mismatch.sum())} of {len(d)} testable "
            f"survey years — the consumption->income transform would run on the wrong data. {examples}"
        )
    log.info(f"Welfare basis matches the PIP bins for all {len(d)} testable survey years")

    # --- Part 2: extrapolated years, fingerprinted by the Gini -----------------------------------
    # Scaling every income by one growth factor leaves the Lorenz curve, and so the Gini, exactly
    # where it was — and it commutes with the fixed-quantile 109-bin aggregation, so the equality
    # survives here. Plain pandas and float64 throughout: the comparison is at 1e-6.
    b = pd.DataFrame(
        {
            "country": tb_pip["country"].astype(str),
            "year": tb_pip["year"].astype(int),
            "avg": tb_pip["avg"].astype(float),
            "pop": tb_pip["pop"].astype(float),
        }
    ).sort_values(["country", "year", "avg"], kind="stable")
    grouped = b.groupby(["country", "year"], observed=True)
    b["cum_pop"] = grouped["pop"].cumsum() / grouped["pop"].transform("sum")
    b["income"] = b["avg"] * b["pop"]
    b["cum_income"] = grouped["income"].cumsum() / grouped["income"].transform("sum")
    b["area"] = (b["cum_pop"] - grouped["cum_pop"].shift(fill_value=0.0)) * (
        b["cum_income"] + grouped["cum_income"].shift(fill_value=0.0)
    )
    gini = (1 - grouped["area"].sum()).rename("gini").reset_index()

    # The candidate anchors: each survey year's Gini and resolved concept.
    # Plain pandas: resolve_survey_concepts hands back a catalog Series, and a Table refuses to
    # merge with the plain frames built above.
    resolved = pd.DataFrame(resolve_survey_concepts(p).rename("survey_concept").reset_index())
    resolved["country"] = resolved["country"].astype(str)
    resolved["year"] = resolved["year"].astype(int)
    anchors = resolved.merge(gini.rename(columns={"gini": "survey_gini"}), on=["country", "year"]).rename(
        columns={"year": "survey_year"}
    )

    # Non-survey years (year != survey_year_used exactly when the year has no survey), each paired
    # with every survey of its country whose Gini it carries to within tolerance.
    filled = pd.DataFrame(tb_welfare[tb_welfare["year"] != tb_welfare["survey_year_used"]])
    filled = filled.astype({"country": str, "year": int, "welfare_type": str})
    filled = filled.merge(gini, on=["country", "year"]).merge(anchors, on="country")
    hits = filled[(filled["gini"] - filled["survey_gini"]).abs() < 1e-6]

    # Identified = the matching surveys all carry ONE concept (ambiguous matches are skipped).
    concepts = hits.groupby(["country", "year"], observed=True).agg(
        matched_concepts=("survey_concept", "nunique"),
        survey_concept=("survey_concept", "first"),
        welfare_type=("welfare_type", "first"),
    )
    identified = concepts[concepts["matched_concepts"] == 1]
    wrong = identified[identified["welfare_type"] != identified["survey_concept"]]
    if not wrong.empty:
        examples = [
            f"{c} {int(y)}: labelled {r.welfare_type}, but the bins carry a {r.survey_concept} survey's Gini"
            for (c, y), r in wrong.head(5).iterrows()
        ]
        raise AssertionError(
            f"Welfare basis disagrees with the PIP bins for {len(wrong)} of {len(identified)} "
            f"Gini-identifiable extrapolated years — a label was drawn from the wrong side of a "
            f"survey gap. {examples}"
        )
    log.info(f"Welfare basis matches the PIP bins for all {len(identified)} Gini-identifiable extrapolated years")


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
    # STEP 1 — start from a copy of the raw PIP rows, relabelled as the new series. The raw `pip`
    # series is untouched; this one becomes `pip_income_basis`, the base the top adjustment builds
    # on. (Column origins are attached explicitly at the end of run(), in format_with_origins, so
    # the numpy assignments below are free to replace column contents.)
    tb = tb_pip.copy()
    tb["series"] = TOP_ADJUSTMENT_BASE_SERIES

    # STEP 2 — pick each bin's regression. The model fits one (alpha, beta) per whole percentile
    # 1..100, so the 99 one-percent bins map 1:1 (bin_index 0 = p0p1 -> percentile 1, ...) and the
    # ten finer bins inside the top 1% all share the p=100 coefficients. A non-positive beta would
    # let the mapping reverse the ranking of two bins, so it is asserted away.
    k = np.minimum(tb["bin_index"].to_numpy() + 1, 100)
    alpha = tb_model.set_index("percentile")["alpha"]
    beta = tb_model.set_index("percentile")["beta"]
    assert (beta > 0).all(), "Non-positive beta in the consumption->income model."

    # STEP 3 — which rows to transform: the welfare lookup's `adjusted` flag, joined per
    # (country, year), marks the consumption country-years. The left join keeps tb's row order
    # (the lookup is unique on its key), and countries absent from the lookup — no national PIP
    # survey — get NaN, filled to False: their values pass through unchanged.
    consumption = tb.merge(tb_welfare[["country", "year", "adjusted"]], on=["country", "year"], how="left")[
        "adjusted"
    ].fillna(False)
    consumption = consumption.to_numpy(dtype=bool)

    # STEP 4 — the mapping itself, in levels: ln(income) = alpha + beta * ln(consumption)
    # exponentiates to income = e^alpha * consumption^beta, applied bin by bin with that bin's own
    # coefficients, and written only where the mask is True.
    avg = tb["avg"].to_numpy(dtype=float)
    adjusted_avg = np.exp(alpha.loc[k].to_numpy()) * avg ** beta.loc[k].to_numpy()
    tb["avg"] = np.where(consumption, adjusted_avg, avg)

    # STEP 5 — repair the profiles the mapping breaks. The coefficients are not monotone in the
    # percentile (p1's beta is the only one below 1, on the worst fit of the hundred), so at the
    # sub-$1 levels found at the bottom of poor countries a monotone consumption profile can come
    # out non-monotone. Isotonic regression projects each violating country-year back onto the
    # monotone cone, population-weighted and mean-preserving (see enforce_monotone), and the
    # re-check asserts nothing slipped through. The False branch keeps the raw fitted profile, as
    # the source project did.
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

    # STEP 6 — `avg` changed, so each bin's share of its country-year's total income is stale;
    # rebuild it from avg * pop.
    tb = recompute_shares(tb)
    return tb


def enforce_monotone(tb: Table, mask: np.ndarray) -> Table:
    """Make each masked country-year's bin averages non-decreasing across its 109 bins, changing
    as little as possible and leaving its mean exactly where it was.

    THE PROBLEM. A bin's average can never sit below the bin beneath it: bin p1p2 is by definition
    the 1st-2nd percentile, so its mean cannot exceed p2p3's. The raw PIP series respects this
    everywhere. The consumption -> income mapping can break it, because every percentile has its
    own (alpha, beta) and those are not monotone in the percentile: at the sub-$1 consumption
    levels found at the bottom of poor countries, p1's coefficients imply a HIGHER income than
    p2's, so the mapped profile dips where the consumption profile did not. 495 country-years are
    affected, almost all in their bottom five bins.

    WHAT IT DOES. For each violating country-year, replace its 109 values with the closest
    non-decreasing sequence — closest in the least-squares sense, each bin weighted by its
    population. That is isotonic regression, and the algorithm behind it (pool-adjacent-violators)
    is easy to picture: walk the bins from the bottom up; whenever a bin sits below the one before
    it, merge the two into a pool and give both the pool's population-weighted average; keep
    merging the pool backwards while it is still lower than its predecessor. Only bins inside such
    a pool change.

    Three properties follow, and they are why this method was chosen:

      - the result is non-decreasing by construction (the caller re-checks and asserts it);
      - the country-year's total income, and so its mean, is preserved exactly — a pool's weighted
        average carries precisely the income of the bins it replaces;
      - every bin outside a pool keeps its fitted value to the last digit.

    WORKED EXAMPLE — Benin 2015, the worst case in the panel. PIP reports its bottom three
    percentiles at an identical $0.28/day, so the mapped values there are set purely by the
    coefficients, and they fall as the percentile rises:

        bin      fitted    after
        p0p1     0.2096    0.1523
        p1p2     0.1438    0.1523
        p2p3     0.1322    0.1523
        p3p4     0.1295    0.1523
        p4p5     0.1463    0.1523
        p5p6     0.1661    0.1661   (unchanged: already above the pool)

    0.1523 is the plain average of the five fitted values — these bins hold equal population — so
    the bottom 5% keep exactly the income the model gave them, now spread evenly across the five.

    WHY NOT A CUMULATIVE MAXIMUM. Lifting each bin to the highest value seen so far is simpler, but
    here it would set all five bins to p0p1's 0.2096 — broadcasting the single least reliable
    estimate in the model (percentile 1 has the only beta below 1, 0.776, and the worst fit, R^2
    0.28) across the run, and adding income that was never there. Measured over the 495 affected
    country-years, a cumulative maximum shifts the mean by +0.11% and the within-country MLD by
    -0.011; isotonic regression shifts them by +0.000% and -0.00035.
    """
    # STEP 1 — work positionally. `mask` is aligned with tb's rows as passed, so drop any index
    # labels (reset_index keeps the row order) and compute the permutation that sorts rows into
    # (country, year, bin_index). It is applied to the arrays, never to the table, so the mask stays
    # aligned and the result can be written back in the caller's row order.
    tb = tb.reset_index(drop=True)
    order = tb.sort_values(["country", "year", "bin_index"]).index.to_numpy()

    # STEP 2 — one matrix row per country-year, one column per bin in ascending order. Every
    # country-year has exactly 109 bins, which is what lets the sorted arrays reshape cleanly.
    avg = tb["avg"].to_numpy(dtype=float)
    pop = tb["pop"].to_numpy(dtype=float)
    n_groups = len(avg) // 109
    assert n_groups * 109 == len(avg), "Rows do not divide into 109-bin country-years."
    grid = avg[order].reshape(n_groups, 109)  # the values to make monotone
    weights = pop[order].reshape(n_groups, 109)  # bin populations: a 0.1% bin counts a tenth of a 1% bin
    rows = np.asarray(mask, dtype=bool)[order].reshape(n_groups, 109).any(axis=1)  # country-years in the mask

    # STEP 3 — fit only where needed: masked country-years with at least one decrease. The 1e-9
    # tolerance keeps float noise from counting as a violation. Everything else is already monotone
    # and is left exactly as it is.
    violating = rows & (np.diff(grid, axis=1) < -1e-9).any(axis=1)

    # STEP 4 — the isotonic fit, one country-year at a time. x is just the bin position: the fit
    # constrains y to be non-decreasing in x, so only the ORDER of x matters. sample_weight is the
    # bin population, which makes each pool's replacement value a population-weighted average and
    # so conserves the country-year's total income.
    percentiles = np.arange(109)
    fitter = IsotonicRegression(increasing=True)
    for i in np.flatnonzero(violating):
        grid[i] = fitter.fit_transform(percentiles, grid[i], sample_weight=weights[i])

    # STEP 5 — write back through the same permutation, so every value lands on the row it came
    # from and the caller's row order (and mask alignment) is untouched.
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
    """Split global inequality into a between-country and a within-country part, per (year, series),
    using the mean log deviation (MLD), and return both the per-(year, series) decomposition and the
    per-country pieces it is built from.

    THE MEASURE. The MLD — Theil's L, the generalised entropy index GE(0) — of a population is

        MLD = sum_i f_i * ln(mu / x_i)          f_i: person i's share of the population, mu: the mean

    the average log-distance of incomes below the mean. Zero for perfect equality, unbounded above.

    THE DECOMPOSITION. Partition the population into countries k with population share v_k, mean
    mu_k and internal MLD_k. Then, exactly,

        MLD = sum_k v_k * MLD_k        +      sum_k v_k * ln(mu / mu_k)
              \_______ within _______/        \________ between ________/

    The within term is the population-share-weighted average of each country's own inequality; the
    between term is the MLD of the "smoothed" world where everyone earns their country's mean. Both
    use POPULATION shares — that is what singles the MLD out among the GE family: the general within
    weight is v_k^(1-a) * s_k^a (s_k the income share), which is v_k at a=0 and s_k for Theil's T at
    a=1. References: Mookherjee & Shorrocks (1982); Jenkins' `ineqdeco` (GE_W(a) = sum_k v_k^(1-a)
    s_k^a GE_k(a), GE_B(a) the index of the smoothed distribution); Haughton & Khandker (2009, World
    Bank Handbook on Poverty and Inequality, ch. 6). The between share, between / MLD, is the headline
    number on the deck's slides.

    VERIFIED, NOT ASSUMED. Run on a two-country synthetic panel with a hand-computable answer
    (A: everyone at 1, pop 100; B: half at 2, half at 8, pop 300 -> within = 3/4 ln 5/4, between =
    1/4 ln 4 + 3/4 ln 4/5), the function reproduces all three components to 6e-17. Re-derived from
    the published bins with fresh code, all 280 (year, series) pairs agree to <1e-6 — the residual is
    float32 storage of the bins, not arithmetic. Substituting income-share weights in the between term
    (the Theil-T convention) would give 0.4723 instead of 0.4803 for PIP in 2023, so the two are
    distinguishable and the published value is the MLD one.

    HOW BINS STAND IN FOR PEOPLE. A bin is treated as (p_high - p_low) * N people all earning `avg`,
    so f_i is the bin's population share and inequality WITHIN a bin is ignored. Every bin-based
    index understates the true one for that reason (the World Bank's own note on the binned data says
    the same); the 109-bin structure with 0.1% bins across the top limits the loss where incomes are
    most dispersed.

    TWO CONVENTIONS.

    - Country weights come from ONE demographic yardstick for EVERY series, matched to the series'
      basis: UN WPP adults aged 20+ for per-adult series, OWID's population otherwise — including
      for PIP and the derived series, whose own `pop` column is deliberately NOT used. This keeps
      the sources' demographic disagreements out of the comparison, so a PIP-vs-WID gap in the
      between share is about incomes, not headcounts. (It is also why `income_distributions.pop`
      and the `population_weight` published here differ for PIP countries.) The source project
      used WID's own counts for this; the yardstick is now independent of both sources, which
      moved every 2023 between share by at most 0.015pp (see load_reference_population).
    - ln(0) is undefined, so zero incomes take ZERO_INCOME_REPLACEMENT ($0.01/day) inside this
      calculation only; the stored distributions keep their zeros. The floor also enters the means,
      which lifts them by at most 0.0008% (WID pre-tax per capita, 2023, the worst case) — negligible,
      and reported per (year, series) as `num_zero_bins_replaced`. Because the floor is a dollar
      amount, its effect on the MLD depends on the price level of the series; see the handover for
      the sensitivity.

    The exact identity total = within + between is asserted for every (year, series), with the total
    computed independently from the bin level.
    """
    # STEP 1 — attach the reference population to every bin row. Both guards matter for the weights:
    # a duplicate (country, year) in the population table would double a country through the left
    # join, and an unmapped series would silently fall back to total population.
    assert not tb_pop.duplicated(subset=["country", "year"]).any(), (
        "Reference population is not unique on (country, year)."
    )
    unmapped = set(tb_distributions["series"].unique()) - set(SERIES_BASIS)
    assert not unmapped, f"Series without a declared population basis: {sorted(unmapped)}"
    d = tb_distributions[["country", "year", "series", "p_low", "p_high", "avg"]].copy()
    d = d.merge(tb_pop, on=["country", "year"], how="left")
    assert d["total_population"].notna().all(), (
        "Missing reference population for some country-year in the common sample."
    )

    # STEP 2 — the weight of a bin is the number of people in it: the series' basis population
    # (adults or everyone, from the reference yardstick) times the bin's width. Widths sum to exactly 1 per country-year,
    # so a country's weights sum to its basis population.
    basis_is_adult = d["series"].map(SERIES_BASIS).eq("adult").to_numpy()
    ref_pop = np.where(
        basis_is_adult, d["adult_population"].to_numpy(dtype=float), d["total_population"].to_numpy(dtype=float)
    )
    w = ref_pop * (d["p_high"] - d["p_low"]).to_numpy(dtype=float)

    # STEP 3 — the income of a bin, with the zero floor. `zero_bin` keeps count so the output can
    # say how many bins were floored.
    x = d["avg"].to_numpy(dtype=float)
    zero = x == 0
    x = np.where(zero, ZERO_INCOME_REPLACEMENT, x)

    # STEP 4 — everything downstream is a weighted sum of three quantities, so compute them once per
    # bin: the weight, the weighted income (for means) and the weighted log income (for the MLD).
    d["w"] = w
    d["wx"] = w * x
    d["wlnx"] = w * np.log(x)
    d["zero_bin"] = zero

    # STEP 5 — per country: its mean and its own MLD. Since MLD = mean of ln(mu/x) = ln(mu) - mean of
    # ln(x), it falls straight out of the three sums.
    by_country = d.groupby(["year", "series", "country"], observed=True)[["w", "wx", "wlnx"]].sum()
    by_country["mean"] = by_country["wx"] / by_country["w"]
    by_country["mld_within"] = np.log(by_country["mean"]) - by_country["wlnx"] / by_country["w"]

    # STEP 6 — the world: the same three sums over all countries give the grand mean.
    total_sums = by_country.groupby(["year", "series"], observed=True)[["w", "wx", "wlnx"]].sum()
    grand_mean = total_sums["wx"] / total_sums["w"]

    # STEP 7 — the two components, both as population-share-weighted sums over countries: the
    # between term weights each country's log-distance from the world mean, the within term weights
    # its internal MLD.
    g = by_country.reset_index().merge(grand_mean.rename("grand_mean").reset_index(), on=["year", "series"], how="left")
    g = g.merge(total_sums["w"].rename("w_total").reset_index(), on=["year", "series"], how="left")
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

    # STEP 8 — the total, computed INDEPENDENTLY from the bin level (ln of the grand mean minus the
    # world's mean log income), so the identity below is a real check and not a tautology.
    totals = np.log(grand_mean) - total_sums["wlnx"] / total_sums["w"]
    tb_decomposition = tb_decomposition.merge(
        totals.rename("mld_total").reset_index(), on=["year", "series"], how="left"
    )
    zero_counts = (
        d.groupby(["year", "series"], observed=True)["zero_bin"].sum().rename("num_zero_bins_replaced").reset_index()
    )
    tb_decomposition = tb_decomposition.merge(zero_counts, on=["year", "series"], how="left")

    # STEP 9 — the identity holds exactly in algebra, so anything beyond float noise is a bug.
    gap = (tb_decomposition["mld_total"] - tb_decomposition["mld_between"] - tb_decomposition["mld_within"]).abs()
    assert gap.max() < 1e-9, f"Decomposition identity violated (max gap {gap.max():.2e})."
    tb_decomposition["between_share"] = tb_decomposition["mld_between"] / tb_decomposition["mld_total"]

    # STEP 10 — two tables: the decomposition per (year, series), and the per-country pieces behind
    # it. `population_weight` is the country's basis population — the weight actually used — which
    # for PIP countries is the reference yardstick's count, not PIP's.
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


def sanity_check_raw_series(
    tb_wid: Table, tb_pip: Table, tb_dist: Table, tb_pop: Table, tb_pop_wid: Table, tb_incomes_wid: Table
) -> None:
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

    # Per-adult -> per-capita conversion: the ratio must equal WID's OWN adult share of population.
    pa = tb_wid[tb_wid["series"] == "wid_before_tax_per_adult"].set_index(["country", "year", "percentile"])["avg"]
    pc = tb_wid[tb_wid["series"] == "wid_before_tax_per_capita"].set_index(["country", "year", "percentile"])["avg"]
    ratio = (pc / pa).dropna().reset_index().merge(tb_pop_wid, on=["country", "year"])
    adult_share = ratio["adult_population"] / ratio["total_population"]
    assert np.allclose(ratio["avg"], adult_share, rtol=1e-6), "Per-capita conversion does not match WID's adult share."

    # Bin populations are the weights, so per country-year they must sum to the reference population
    # of the series' basis — exactly, since widths sum to 1.
    pop_sums = tb_wid.groupby(["series", "country", "year"], observed=True)["pop"].sum().reset_index()
    pop_sums = pop_sums.merge(tb_pop, on=["country", "year"], how="left")
    expected = np.where(
        pop_sums["series"].map(SERIES_BASIS).eq("adult"), pop_sums["adult_population"], pop_sums["total_population"]
    )
    assert np.allclose(pop_sums["pop"], expected, rtol=1e-9), (
        "WID bin populations do not sum to the reference population."
    )

    # Cross-source agreement: the reference yardstick's adult share against WID's own, over the WID
    # panel. They are the same UN WPP vintage in all but a handful of country-years, so the median
    # ratio must be 1; anything off by more than 2% is named (expected: Togo, revised by UN in 2026).
    both = tb_pop.merge(
        tb_pop_wid.rename(columns={"adult_population": "wid_adult", "total_population": "wid_total"}),
        on=["country", "year"],
        how="inner",
    )
    both = both[both["country"].isin(tb_wid["country"].unique())]
    share_ratio = (both["adult_population"] / both["total_population"]) / (both["wid_adult"] / both["wid_total"])
    assert abs(share_ratio.median() - 1) < 1e-3, "Reference and WID adult shares disagree systematically."
    off = both.loc[(share_ratio - 1).abs() > 0.02, ["country", "year"]]
    if len(off):
        log.warning(
            f"Reference and WID demographies differ by >2% in {len(off)} country-years: "
            f"{sorted(off['country'].unique())}"
        )

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
