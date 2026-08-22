"""Assemble the data behind the 'long-run history of child mortality' chart.

Five tables, one per layer of the chart, so that every figure the chart states can be read out of a
dataset rather than typed into a drawing:

- ``historical_societies`` - the 21 pre-modern societies the chart plots: 19 from Volk & Atkinson's
  Table 2, plus Sweden computed from Human Mortality Database life tables and a Bavarian village
  computed from Knodel (1970).
- ``hunter_gatherer_societies`` - Volk & Atkinson's Table 1, the societies carrying a child
  mortality rate.
- ``global_child_mortality`` - one global series for the modern era, spliced from UN WPP life tables
  before 1990 and UN IGME from 1990 on.
- ``country_extremes`` - the highest and lowest national rates in the latest year.
- ``summary`` - the averages and counts the chart states in words.

Two things about the sources this step must not smooth over.

**Volk & Atkinson's own summary rows do not follow from the rates above them.** Their Table 1 gives a
mean child mortality rate of 48.8% over 17 societies, which recomputes to 47.3%; Table 2 gives 46.2%
over 24, which recomputes to 47.0%. In both tables the infant mortality N is one lower than the number
of printed values. So the hunter-gatherer average is carried through **as published** and never
recomputed, while the historical average is OWID's own computation over the 21 selected societies.
Both are written to ``summary`` and both are asserted, which is what stops a future transcription
error from hiding inside the gap between them.

**The rate is not measured at one fixed age.** Volk & Atkinson take it at, or immediately before,
puberty - generally around 15 years, varying between the studies they pool. The modern series is
measured at exactly 15. That belongs in the chart's subtitle, not in a footnote.
"""

import re

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Volk & Atkinson report the share of children failing to survive to puberty. The modern sources
# measure survival to exactly this age.
AGE_CUTOFF = 15

# Entities in the UN IGME data that are aggregates rather than countries, so the highest and lowest
# national rates are not read off a continent or an income group.
AGGREGATES = set(geo.REGIONS) | {"World"}

# Smallest population a country needs before it can carry the "lowest rate" label. Under-fifteen
# mortality in a microstate is a handful of deaths a year, so the ranking there is noise: San Marino,
# Andorra and Monaco all sit below every country in this chart's label without meaning much.
MINIMUM_POPULATION = 1_000_000

# Rows of Volk & Atkinson's Table 2 that carry a child mortality rate and are still left out of the
# chart's historical set, keyed by (culture, time) exactly as the paper prints them. Each reason is a
# property of the row rather than a guess about intent.
EXCLUDED_HISTORICAL = {
    ("Germany", "1692–1899 A.D."): (
        "Pools marriage cohorts from 1692, which Knodel reports as unreliable because parish registers "
        "omitted infant and child deaths before 1750; the 1750-1799 cohorts of the same study are used instead."
    ),
    ("Germany", "1700–1800 A.D."): (
        "A second German estimate covering a period the chart already represents with the Bavarian village."
    ),
    ("Finland", "1749–1773 A.D."): "Measured at age 10 rather than at the end of puberty.",
    ("Finland and Sweden", "1751–1800 A.D."): (
        "Pools two countries; Sweden alone is taken from the Human Mortality Database over an overlapping period."
    ),
    ("Afghanistan", "1950 A.D."): "Falls inside the modern era that the chart's global series covers.",
}

# Volk & Atkinson name several studies by modern country where the study is of a single site, and the
# site is what lets a reader place the point.
DISPLAY_NAMES = {
    # Alesan, Malgosa & Simo (1999) study an Iron Age necropolis on Mallorca.
    ("Spain", "400–200 B.C."): "Mallorca (Spain)",
    # Storey (1985) studies Teotihuacan, in the Basin of Mexico.
    ("Teotihuacan", "300–550 A.D."): "Teotihuacan (Mexico)",
    ("Teotihuacan", "550–700 A.D."): "Teotihuacan (Mexico)",
}

# Sweden's Human Mortality Database life tables begin in 1751, so this is the first three decades they
# cover. The published chart labelled the same point 1750-80.
SWEDEN_COHORT = (1751, 1780)

# Which of Knodel's five marriage cohorts the Bavarian point uses. The 1692-1749 cohort is the one
# Knodel reports as unreliable - parish registers omitted infant and child deaths before 1750, which
# inflates its surviving-children figure - so this is the earliest he stands behind.
KNODEL_COHORT = (1750, 1799)

# UN IGME estimates a global rate from this year on; UN WPP life tables carry the years before it.
SPLICE_YEAR = 1990

# How a printed rate is read. A range becomes its midpoint and keeps its bounds; an open-ended value
# ("40%+") becomes its lower bound, which is what the chart labels "higher than 40%".
RATE_RANGE = re.compile(r"^(\d+(?:\.\d+)?)%?-(\d+(?:\.\d+)?)%$")
RATE_OPEN_ENDED = re.compile(r"^(\d+(?:\.\d+)?)%\+$")
RATE_POINT = re.compile(r"^(\d+(?:\.\d+)?)%")

PERIOD_PATTERNS = (
    (re.compile(r"^(\d+)-(\d+) B\.C\.$"), -1, -1),
    (re.compile(r"^(\d+) B\.C\.-(\d+) A\.D\.$"), -1, 1),
    (re.compile(r"^(\d+)-(\d+) A\.D\.$"), 1, 1),
)
PERIOD_SINGLE_YEAR = re.compile(r"^(\d+) A\.D\.$")


def run() -> None:
    #
    # Load inputs.
    #
    tb_volk = paths.load_dataset("volk_atkinson_2013").read("volk_atkinson_2013")
    tb_knodel = paths.load_dataset("knodel_1970").read("knodel_1970")
    tb_life_tables = paths.load_dataset("hmd").read("life_tables", safe_types=False)
    tb_igme = paths.load_dataset("igme").read("igme_under_fifteen_mortality")
    tb_wpp = paths.load_dataset("un_wpp_lt").read("un_wpp_lt", safe_types=False)

    #
    # Process data.
    #
    tb_volk["row_type"] = classify_printed_rows(tb_volk)

    tb_historical = build_historical_societies(tb_volk, tb_knodel, tb_life_tables)
    tb_hunter_gatherer = build_hunter_gatherer_societies(tb_volk)
    tb_global = build_global_series(tb_igme, tb_wpp)
    tb_extremes = build_country_extremes(tb_igme)
    tb_summary = build_summary(tb_volk, tb_historical, tb_hunter_gatherer, compute_splice_difference(tb_igme, tb_wpp))
    tb_summary = _splice_columns_cite_the_global_sources(tb_summary, tb_global)

    sanity_check_outputs(tb_historical, tb_hunter_gatherer, tb_global, tb_extremes, tb_summary)

    tables = [
        tb_historical.format(["society", "period_start", "period_end"], short_name="historical_societies"),
        tb_hunter_gatherer.format(["society"], short_name="hunter_gatherer_societies"),
        tb_global.format(["year"], short_name="global_child_mortality"),
        tb_extremes.format(["role", "country"], short_name="country_extremes"),
        tb_summary.format(["chart"], short_name="summary"),
    ]

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=tables, check_variables_metadata=True)
    ds_garden.save()


def classify_printed_rows(tb_volk: Table) -> list[str]:
    """Label each transcribed row as a society, one of the tables' own summary rows, or the footer.

    Both tables end with an N row and a Mean row, and Table 2 with a modern-comparison row whose
    infant mortality cell is printed ambiguously. None of the three is a society.
    """
    row_types = []
    for culture, time in zip(tb_volk["culture"].astype(str), tb_volk["time"].astype(str)):
        if culture == "N":
            row_types.append("count")
        elif culture == "Mean":
            row_types.append("mean")
        elif time == "Modern":
            row_types.append("modern_reference")
        else:
            row_types.append("society")
    return row_types


def parse_rate(printed: str) -> tuple[float | None, float | None, float | None]:
    """Read a rate as Volk & Atkinson print it, returning (low, high, point) in percent.

    ``high`` is None for an open-ended value, because the paper gives no upper bound for one.
    """
    text = (printed or "").strip().replace("–", "-")
    if text in {"", "n/a"}:
        return None, None, None
    if match := RATE_RANGE.match(text):
        low, high = float(match.group(1)), float(match.group(2))
        return low, high, (low + high) / 2
    if match := RATE_OPEN_ENDED.match(text):
        low = float(match.group(1))
        return low, None, low
    if match := RATE_POINT.match(text):
        value = float(match.group(1))
        return value, value, value
    raise ValueError(f"Cannot read {printed!r} as a mortality rate")


def parse_period(printed: str) -> tuple[int, int]:
    """Read a period as Volk & Atkinson print it, returning (start, end) as signed years."""
    text = (printed or "").strip().replace("–", "-")
    for pattern, start_sign, end_sign in PERIOD_PATTERNS:
        if match := pattern.match(text):
            return start_sign * int(match.group(1)), end_sign * int(match.group(2))
    if match := PERIOD_SINGLE_YEAR.match(text):
        year = int(match.group(1))
        return year, year
    raise ValueError(f"Cannot read {printed!r} as a period")


def period_label(start: int, end: int) -> str:
    """Write a period the way the chart labels it.

    A span inside one century abbreviates its end to two digits (1816-50); one that crosses a century
    keeps all four (1650-1800). That rule reproduces every period label on the published chart.
    """
    if start < 0 and end < 0:
        return f"{-start}–{-end} BCE"
    if start < 0:
        return f"{-start} BCE–{end}"
    if start == end:
        return "around the year 0" if start == 0 else f"{start}"
    if start // 100 == end // 100:
        return f"{start}–{end % 100:02d}"
    return f"{start}–{end}"


def build_historical_societies(tb_volk: Table, tb_knodel: Table, tb_life_tables: Table) -> Table:
    """The 21 pre-modern societies the chart plots, from three sources."""
    tb = pr.concat(
        [
            _historical_from_volk_atkinson(tb_volk),
            _sweden_from_life_tables(tb_life_tables),
            _bavaria_from_knodel(tb_knodel),
        ],
        ignore_index=True,
    )
    # The chart plots each society at the middle of the period it covers.
    tb["period_mid"] = (tb["period_start"] + tb["period_end"]) / 2
    tb["period_label"] = [period_label(start, end) for start, end in zip(tb["period_start"], tb["period_end"])]
    # Both are derived from the period columns, so they cite what those cite. A list comprehension
    # returns a plain Series, which arrives with no metadata of its own.
    for column in ("period_mid", "period_label"):
        tb[column].metadata.origins = list(tb["period_start"].metadata.origins)
    return tb.sort_values(["period_mid", "society"]).reset_index(drop=True)


def _historical_from_volk_atkinson(tb_volk: Table) -> Table:
    """Volk & Atkinson's Table 2, restricted to the societies the chart plots."""
    tb = tb_volk[(tb_volk["table"] == 2) & (tb_volk["row_type"] == "society")]
    tb = tb[_has_rate(tb["cmr"])]

    keys = list(zip(tb["culture"].astype(str), tb["time"].astype(str)))
    unknown = set(EXCLUDED_HISTORICAL) - set(keys)
    assert not unknown, f"Rows named for exclusion are not in the table: {sorted(unknown)}"

    kept = [key not in EXCLUDED_HISTORICAL for key in keys]
    tb = tb[kept]
    keys = [key for key, keep in zip(keys, kept) if keep]

    rates = [parse_rate(value) for value in tb["cmr"].astype(str)]
    periods = [parse_period(time) for _, time in keys]

    out = Table(
        {
            "society": [DISPLAY_NAMES.get(key, key[0]) for key in keys],
            "period_start": [start for start, _ in periods],
            "period_end": [end for _, end in periods],
            "share_dying_before_15": [point for _, _, point in rates],
            "share_dying_before_15_low": [low for low, _, _ in rates],
            "share_dying_before_15_high": [high for _, high, _ in rates],
            "rate_as_published": tb["cmr"].astype(str).tolist(),
            "study": tb["source"].astype(str).tolist(),
            "data_source": "Volk and Atkinson (2013)",
        }
    )
    return _carry_origins(out, tb_volk["cmr"])


def _sweden_from_life_tables(tb_life_tables: Table) -> Table:
    """Sweden's earliest three decades, from Human Mortality Database period life tables."""
    first, last = SWEDEN_COHORT
    swedish = tb_life_tables[
        (tb_life_tables["country"] == "Sweden")
        & (tb_life_tables["type"] == "period")
        & (tb_life_tables["sex"] == "total")
        & (tb_life_tables["year"] >= first)
        & (tb_life_tables["year"] <= last)
    ]
    share_by_year = _share_dying_before_cutoff(swedish)
    assert len(share_by_year) == last - first + 1, (
        f"Expected {last - first + 1} years of Swedish life tables, got {len(share_by_year)}"
    )
    share = float(share_by_year.mean())

    out = Table(
        {
            "society": ["Sweden"],
            "period_start": [first],
            "period_end": [last],
            "share_dying_before_15": [share],
            "share_dying_before_15_low": [share],
            "share_dying_before_15_high": [share],
            "rate_as_published": [f"{share:.1f}%"],
            "study": ["Human Mortality Database"],
            "data_source": ["Human Mortality Database"],
        }
    )
    return _carry_origins(out, tb_life_tables["number_survivors"])


def _bavaria_from_knodel(tb_knodel: Table) -> Table:
    """A Bavarian village, from Knodel's counts of children born and children surviving.

    Knodel's Table 11 reports how many children a couple bore and how many **survived** to their
    fifteenth birthday, so the share who died is births minus survivors, over births. For the cohort
    used here the two columns coincide - 5.6 born, 2.8 surviving, so 2.8 died - which is exactly why
    the subtraction is written out rather than the survivor column being read as deaths.

    Knodel reports his earliest cohort as unreliable because the parish registers omitted infant and
    child deaths before 1750, so KNODEL_COHORT selects the first one he stands behind.
    """
    cohort = tb_knodel[tb_knodel["marriage_cohort_start"] == KNODEL_COHORT[0]]
    assert len(cohort) == 1, f"Expected one marriage cohort starting in {KNODEL_COHORT[0]}, found {len(cohort)}"
    row = cohort.iloc[0]
    assert int(row["marriage_cohort_end"]) == KNODEL_COHORT[1], (
        f"The cohort starting in {KNODEL_COHORT[0]} now ends in {int(row['marriage_cohort_end'])}, "
        f"not {KNODEL_COHORT[1]}"
    )

    born = float(row["children_ever_born"])
    surviving = float(row["children_surviving_to_age_15"])
    share = (born - surviving) / born * 100

    out = Table(
        {
            "society": [f"{row['region']} (Germany)"],
            "period_start": [int(row["marriage_cohort_start"])],
            "period_end": [int(row["marriage_cohort_end"])],
            "share_dying_before_15": [share],
            "share_dying_before_15_low": [share],
            "share_dying_before_15_high": [share],
            "rate_as_published": [f"{surviving:g} of {born:g} children surviving"],
            "study": ["Knodel, 1970"],
            "data_source": ["Knodel (1970)"],
        }
    )
    return _carry_origins(out, tb_knodel["children_surviving_to_age_15"])


def build_hunter_gatherer_societies(tb_volk: Table) -> Table:
    """Volk & Atkinson's Table 1, restricted to the societies carrying a child mortality rate."""
    tb = tb_volk[(tb_volk["table"] == 1) & (tb_volk["row_type"] == "society")]
    tb = tb[_has_rate(tb["cmr"])]
    rates = [parse_rate(value) for value in tb["cmr"].astype(str)]

    out = Table(
        {
            "society": tb["culture"].astype(str).tolist(),
            "share_dying_before_15": [point for _, _, point in rates],
            "share_dying_before_15_low": [low for low, _, _ in rates],
            "share_dying_before_15_high": [high for _, high, _ in rates],
            "rate_as_published": tb["cmr"].astype(str).tolist(),
            "study": tb["source"].astype(str).tolist(),
        }
    )
    out = _carry_origins(out, tb_volk["cmr"])
    return out.sort_values("society").reset_index(drop=True)


def build_global_series(tb_igme: Table, tb_wpp: Table) -> Table:
    """One global series for the modern era, spliced at 1990.

    UN IGME is OWID's source for child mortality but only estimates a global rate from 1990. UN WPP
    life tables reach back to 1950, and the two agree to about a fifth of a percentage point where
    they meet, so the join is a step small enough to footnote rather than a break in the series.
    """
    igme = tb_igme[
        (tb_igme["country"] == "World")
        & (tb_igme["indicator"] == "Under-fifteen mortality rate")
        & (tb_igme["year"] >= SPLICE_YEAR)
    ]
    tb_recent = Table(
        {
            "year": igme["year"].astype(int).tolist(),
            "share_dying_before_15": igme["observation_value"].astype(float).tolist(),
            "data_source": "UN IGME",
        }
    )
    tb_recent = _carry_origins(tb_recent, tb_igme["observation_value"])

    world = tb_wpp[
        (tb_wpp["location"] == "World")
        & (tb_wpp["sex"] == "total")
        & (tb_wpp["variant"] == "Medium")
        & (tb_wpp["year"] < SPLICE_YEAR)
    ]
    share_by_year = _share_dying_before_cutoff(world)
    tb_early = Table(
        {
            "year": [int(year) for year in share_by_year.index],
            "share_dying_before_15": share_by_year.tolist(),
            "data_source": "UN WPP",
        }
    )
    tb_early = _carry_origins(tb_early, tb_wpp["number_survivors"])

    return pr.concat([tb_early, tb_recent], ignore_index=True).sort_values("year").reset_index(drop=True)


def compute_splice_difference(tb_igme: Table, tb_wpp: Table) -> float:
    """How far the two global sources disagree, in percentage points, in the year they both cover.

    Compared **within** the splice year, not across it. The step between the last UN WPP year and the
    first UN IGME year is a real one-year fall plus the difference between the sources, so quoting it
    as "how much the sources disagree" overstates the disagreement by about a factor of two.
    """
    igme = tb_igme[
        (tb_igme["country"] == "World")
        & (tb_igme["indicator"] == "Under-fifteen mortality rate")
        & (tb_igme["year"] == SPLICE_YEAR)
    ]
    assert len(igme) == 1, f"Expected one UN IGME global rate for {SPLICE_YEAR}, found {len(igme)}"

    world = tb_wpp[
        (tb_wpp["location"] == "World")
        & (tb_wpp["sex"] == "total")
        & (tb_wpp["variant"] == "Medium")
        & (tb_wpp["year"] == SPLICE_YEAR)
    ]
    wpp_rate = float(_share_dying_before_cutoff(world).iloc[0])
    return abs(wpp_rate - float(igme["observation_value"].iloc[0]))


def build_country_extremes(tb_igme: Table) -> Table:
    """The highest and lowest national rates in the latest year UN IGME estimates.

    "Lowest" is every country whose rate rounds to the lowest rounded rate, because the chart labels
    one figure and several countries share it once rounded to a tenth of a percentage point.
    """
    rates = tb_igme[tb_igme["indicator"] == "Under-fifteen mortality rate"]
    latest_year = int(rates["year"].max())

    tb = rates[(rates["year"] == latest_year) & ~rates["country"].isin(AGGREGATES)]
    assert rates["country"].isin(AGGREGATES).any(), "No aggregate entities found, so none were excluded"
    tb = tb.rename(columns={"observation_value": "share_dying_before_15"})[["country", "year", "share_dying_before_15"]]

    tb = paths.regions.add_population(tb, warn_on_missing_countries=False)
    tb = tb[tb["population"] > MINIMUM_POPULATION].drop(columns=["population"])

    highest = tb.nlargest(1, "share_dying_before_15")
    lowest_rounded = tb["share_dying_before_15"].round(1).min()
    lowest = tb[tb["share_dying_before_15"].round(1) == lowest_rounded].sort_values("share_dying_before_15")

    highest["role"] = "highest"
    lowest["role"] = "lowest"
    return pr.concat([highest, lowest], ignore_index=True)


def build_summary(tb_volk: Table, tb_historical: Table, tb_hunter_gatherer: Table, splice_difference: float) -> Table:
    """The averages and counts the chart states in words.

    The hunter-gatherer average is Volk & Atkinson's published figure, not a recomputation - see the
    module docstring. Both travel here so the gap between them stays visible.
    """
    table_1 = tb_volk[tb_volk["table"] == 1]
    published_mean = parse_rate(str(table_1.loc[table_1["row_type"] == "mean", "cmr"].iloc[0]))[2]
    published_n = int(table_1.loc[table_1["row_type"] == "count", "cmr"].iloc[0])

    out = Table(
        {
            "chart": ["long_run_child_mortality"],
            "hunter_gatherer_mean_published": [published_mean],
            "hunter_gatherer_mean_recomputed": [float(tb_hunter_gatherer["share_dying_before_15"].mean())],
            "hunter_gatherer_societies": [published_n],
            "historical_mean": [float(tb_historical["share_dying_before_15"].mean())],
            "historical_societies": [len(tb_historical)],
            "global_splice_year": [SPLICE_YEAR],
            "global_splice_difference": [splice_difference],
        }
    )
    out = _carry_origins(out, tb_volk["cmr"])
    # The historical average is computed over three sources, so it must cite all three rather than
    # only the paper the majority of its rows come from.
    for column in ("historical_mean", "historical_societies"):
        out[column].metadata.origins = list(tb_historical["share_dying_before_15"].metadata.origins)
    return out


def _splice_columns_cite_the_global_sources(tb_summary: Table, tb_global: Table) -> Table:
    """The splice figures describe the global series, so they cite it rather than the paper."""
    for column in ("global_splice_year", "global_splice_difference"):
        tb_summary[column].metadata.origins = list(tb_global["share_dying_before_15"].metadata.origins)
    return tb_summary


def sanity_check_outputs(
    tb_historical: Table,
    tb_hunter_gatherer: Table,
    tb_global: Table,
    tb_extremes: Table,
    tb_summary: Table,
) -> None:
    """Check the claims the chart makes, not only the schema."""
    # --- the historical set the chart plots ---
    assert len(tb_historical) == 21, f"The chart plots 21 historical societies, built {len(tb_historical)}"
    assert tb_historical["share_dying_before_15"].between(20, 70).all(), "A historical rate is outside 20-70%"
    assert (tb_historical["period_start"] <= tb_historical["period_end"]).all(), "A period runs backwards"
    assert tb_historical["period_label"].notna().all(), "A society has no period label"

    mean = float(tb_historical["share_dying_before_15"].mean())
    assert abs(mean - 48.1) < 0.1, f"The historical average should be 48.1%, got {mean:.3f}%"
    assert round(mean) == 48, f"The historical average should round to 48%, got {round(mean)}"

    sources = tb_historical["data_source"].value_counts().to_dict()
    assert sources == {"Volk and Atkinson (2013)": 19, "Human Mortality Database": 1, "Knodel (1970)": 1}, (
        f"The historical set should be 19 rows from the paper plus one each from HMD and Knodel, got {sources}"
    )

    # --- hunter-gatherers: the published mean is not recoverable from the published rates, and that
    # gap is asserted rather than hidden, so a transcription error cannot pass itself off as it ---
    assert len(tb_hunter_gatherer) == 17, f"Table 1 has 17 child mortality rates, built {len(tb_hunter_gatherer)}"
    published = float(tb_summary["hunter_gatherer_mean_published"].iloc[0])
    recomputed = float(tb_summary["hunter_gatherer_mean_recomputed"].iloc[0])
    assert published == 48.8, f"Volk & Atkinson publish 48.8%, read {published}"
    assert abs(recomputed - 47.3) < 0.1, f"The recomputation should be 47.3%, got {recomputed:.3f}%"
    assert published - recomputed > 1.0, (
        "The published hunter-gatherer mean used to exceed the recomputation by more than a point; it "
        f"now differs by {published - recomputed:.3f}. Either the transcription changed or the "
        "discrepancy the docstring documents has gone - check the paper before relaxing this."
    )

    # --- the global series and its one splice ---
    years = tb_global["year"].tolist()
    assert years == sorted(set(years)), "The global series has duplicate or unsorted years"
    assert years[0] == 1950, f"The global series should start in 1950, starts in {years[0]}"
    assert years == list(range(years[0], years[-1] + 1)), "The global series has a gap"
    assert float(tb_global["share_dying_before_15"].iloc[0]) > 20, "Global child mortality in 1950 should exceed 20%"
    assert float(tb_global["share_dying_before_15"].iloc[-1]) < 10, "Global child mortality now should be below 10%"

    switches = tb_global.index[
        tb_global["data_source"].eq("UN IGME") & tb_global["data_source"].shift().eq("UN WPP")
    ].tolist()
    assert len(switches) == 1, f"The global series should switch source exactly once, it switches {len(switches)} times"
    assert tb_global["data_source"].nunique() == 2, "The global series should draw on exactly two sources"
    join = switches[0]
    assert int(tb_global.loc[join, "year"]) == SPLICE_YEAR, f"The splice is at {tb_global.loc[join, 'year']}"
    # Measured within the splice year, not across it - see compute_splice_difference.
    difference = float(tb_summary["global_splice_difference"].iloc[0])
    assert 0 < difference < 0.5, (
        f"The two global sources differ by {difference:.3f} points in {SPLICE_YEAR}, which is too much to footnote "
        "as a join rather than treat as a break in the series"
    )
    assert int(tb_summary["global_splice_year"].iloc[0]) == SPLICE_YEAR, "The recorded splice year is wrong"

    # --- the highest and lowest national rates ---
    assert (tb_extremes["role"] == "highest").sum() == 1, "There should be exactly one highest country"
    assert (tb_extremes["role"] == "lowest").sum() >= 1, "There should be at least one lowest country"
    highest = tb_extremes.loc[tb_extremes["role"] == "highest", "share_dying_before_15"]
    lowest = tb_extremes.loc[tb_extremes["role"] == "lowest", "share_dying_before_15"]
    assert float(highest.iloc[0]) > float(lowest.max()), "The highest rate is not above the lowest"
    assert 5 < float(highest.iloc[0]) < 25, f"The highest national rate, {float(highest.iloc[0]):.2f}%, is implausible"
    assert lowest.max() < 1, f"The lowest national rates should be below 1%, got {float(lowest.max()):.2f}%"
    assert lowest.round(1).nunique() == 1, "The countries sharing the lowest label do not share a rounded rate"
    # Every country here must sit outside the global series' range at both ends, which is the
    # comparison the chart's labels make.
    latest_global = float(tb_global["share_dying_before_15"].iloc[-1])
    assert float(highest.iloc[0]) > latest_global > float(lowest.max()), (
        "The highest and lowest national rates should straddle the latest global rate"
    )


def _has_rate(column) -> list[bool]:
    """Whether each printed cell carries a rate at all, as opposed to "n/a" or nothing."""
    return [str(value) not in {"n/a", ""} for value in column]


def _share_dying_before_cutoff(tb: Table):
    """Share of life-table survivors lost before AGE_CUTOFF, by year.

    Both the radix and the survivors at the cut-off come from the table itself, so a change in a life
    table's conventions cannot be mistaken for a change in mortality.
    """
    at_birth = tb[tb["age"].astype(str) == "0"].set_index("year")["number_survivors"].astype(float)
    at_cutoff = tb[tb["age"].astype(str) == str(AGE_CUTOFF)].set_index("year")["number_survivors"].astype(float)
    assert not at_birth.empty and not at_cutoff.empty, "Life tables are missing age 0 or the cut-off age"
    assert at_birth.index.is_unique, "More than one life table per year at birth; the filter is too loose"
    assert at_cutoff.index.is_unique, "More than one life table per year at the cut-off; the filter is too loose"
    assert at_birth.index.equals(at_cutoff.index), "Life tables cover different years at birth and at the cut-off"
    return ((1 - at_cutoff / at_birth) * 100).sort_index()


def _carry_origins(tb: Table, source_column) -> Table:
    """Put the origins of the column the values came from onto every column of a built table."""
    origins = list(source_column.metadata.origins)
    assert origins, "The source column carries no origins, so the built table would cite nothing"
    for column in tb.columns:
        tb[column].metadata.origins = list(origins)
    return tb
