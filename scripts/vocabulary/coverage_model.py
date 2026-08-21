"""What the all-charts block shows for a topic, and how terms are chosen to cover it.

The suggestion line under a topic's search box should reveal as much of that
topic's chart list as a few terms can, most-revealing first, with charts counted
by how much they are actually viewed. That makes term selection a weighted
maximum-coverage problem, and this module holds the three pieces it needs: the
topic's records ("the universe"), a weight per record, and which records each
candidate term would actually put on screen.

Everything here models the *rendered* list rather than Algolia's matching, which
is looser in every direction that matters (synonyms, typo tolerance, and `tags` /
`availableEntities` / `slug` all being searchable). The block intersects Algolia's
hits with its own per-row test, so that test is the binding constraint and the
one worth reproducing.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from urllib.parse import parse_qsl

import httpx

from etl.config import OWID_ENV  # ty: ignore

# The charts index the block searches, and the public search-only credentials the
# site ships in its own client bundle (owid-grapher commits the same pair in
# functions/api/search/searchApi.integration.test.ts). Override to measure
# against a staging index.
ALGOLIA_INDEX = "explorer-views-and-charts"
ALGOLIA_APP_ID = os.getenv("ALGOLIA_ID") or "ASCB5XMYF2"
ALGOLIA_SEARCH_KEY = os.getenv("ALGOLIA_SEARCH_KEY") or "bafe9c4659e5657bf750a38fbee5c269"
# Algolia's per-request maximum. Every topic fits in one request — the largest
# (Climate Change) has ~714 records.
ALGOLIA_HITS_PER_PAGE = 1000
ALGOLIA_TIMEOUT_SECONDS = 30

# What the block retrieves and renders. `datasetProducers` is the row's
# "Source:" line and is matchable, which is why the universe comes from Algolia
# directly: /api/search does not return it.
UNIVERSE_ATTRIBUTES = [
    "title",
    "subtitle",
    "datasetProducers",
    "slug",
    "queryParams",
    "type",
    "chartConfigId",
]

DEFAULT_VIEWS_COLUMN = "views_365d"


class EmptyAnalyticsError(Exception):
    """Raised when there is no view data to weight charts by."""


# ---------------------------------------------------------------- text matching

# Digits written as sub/superscripts in chart titles ("CO₂"), which the site
# folds to plain digits so that typing "co2" finds them.
_DIGIT_VARIANTS = str.maketrans("₀₁₂₃₄₅₆₇₈₉⁰¹²³⁴⁵⁶⁷⁸⁹", "01234567890123456789")


def split_into_match_words(text: str) -> list[str]:
    """Split text into the same words the site matches on.

    A transcription of `slugify` + `splitIntoMatchWords` in owid-grapher
    (packages/@ourworldindata/utils Util.ts, site/search/searchUtils.tsx) rather
    than a nicer slugifier: that one strips anything outside ASCII
    `[A-Za-z0-9_]`, so "Côte" becomes "cte", and a Python-idiomatic
    unicode-aware version would disagree with the site on exactly the rows this
    is meant to predict.
    """
    slug = re.sub(r"\s+", " ", text.translate(_DIGIT_VARIANTS).lower()).strip()
    slug = re.sub(r"\s*\*.+\*", "", slug)
    slug = re.sub(r"[^\w\- /]+", "", slug, flags=re.ASCII)
    slug = re.sub(r" +", "-", slug).replace("/", "")
    return [word for word in slug.split("-") if word]


def record_row_texts(record: dict) -> list[str]:
    """The texts a row actually displays: its title, subtitle and each producer.

    Kept as separate strings rather than joined, because the site never satisfies
    a query from words gathered across two of them.
    """
    texts = [record.get("title") or "", record.get("subtitle") or ""]
    texts.extend(record.get("datasetProducers") or [])
    return [text for text in texts if text]


def record_matches_term(record: dict, term: str) -> bool:
    """Whether the block would show this row for this term.

    Every word of the term must appear in one of the row's own texts, in any
    order, with the last word allowed to match a prefix — see
    `textContainsAllQueryWords` in site/search/searchUtils.tsx.
    """
    term_words = split_into_match_words(term)
    if not term_words:
        return True
    leading, last = term_words[:-1], term_words[-1]
    for text in record_row_texts(record):
        text_words = split_into_match_words(text)
        if all(word in text_words for word in leading) and any(
            word.startswith(last) for word in text_words
        ):
            return True
    return False


def record_identity(record: dict) -> str:
    """The key the site uses to tell two rows apart: slug plus query params.

    Mirrors `getChartHitIdentity`. Multi-dim and explorer views share a slug with
    their siblings and differ only in `queryParams`, and a Featured Metric record
    shares both with the plain record it was copied from.
    """
    return f"{record.get('slug') or ''}{record.get('queryParams') or ''}"


# -------------------------------------------------------------------- universe


async def fetch_topic_universe(
    client: httpx.AsyncClient, topic_name: str, semaphore: asyncio.Semaphore
) -> list[dict]:
    """Every record the all-charts block lists for a topic.

    The same facets the block's baseline query uses (see `buildChartsFacetFilters`
    and `baseSearchState` in site/AllChartsBlock.tsx): the topic tag, and the
    income-group exclusion that applies while no country is selected. The
    Featured Metric exclusion is deliberately *not* applied, because the block's
    own default list doesn't apply it either — the FM flavour of a row carries
    the same title, subtitle and producers as its plain twin, so it matches
    identically and keys identically.
    """
    async def fetch_page(page: int) -> dict:
        async with semaphore:
            response = await client.post(
                f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query",
                headers={
                    "X-Algolia-Application-Id": ALGOLIA_APP_ID,
                    "X-Algolia-API-Key": ALGOLIA_SEARCH_KEY,
                },
                json={
                    "query": "",
                    "facetFilters": [
                        [f"tags:{topic_name}"],
                        "isIncomeGroupSpecificFM:false",
                    ],
                    "hitsPerPage": ALGOLIA_HITS_PER_PAGE,
                    "page": page,
                    "attributesToRetrieve": UNIVERSE_ATTRIBUTES,
                },
                timeout=ALGOLIA_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()

    first = await fetch_page(0)
    hits = list(first.get("hits") or [])
    # Most topics fit in one request; the largest (Food Supply, ~2,000 views of
    # its explorers) need two or three.
    pages = first.get("nbPages") or 1
    if pages > 1:
        for payload in await asyncio.gather(
            *(fetch_page(page) for page in range(1, pages))
        ):
            hits.extend(payload.get("hits") or [])

    if first.get("nbHits", 0) > len(hits):
        raise RuntimeError(
            f"{topic_name}: {first['nbHits']} records but only {len(hits)} fetched"
        )

    # One record per identity. Where an FM copy and its plain twin both appear,
    # either will do: they render the same text.
    return list({record_identity(hit): hit for hit in hits}.values())


# --------------------------------------------------------------------- weights


@dataclass
class ViewAnalytics:
    """Chart view counts, keyed the two ways records can be resolved."""

    by_config_id: dict[str, int]
    # Average views per view of an explorer or multi-dim, for records whose own
    # view can't be identified.
    average_by_slug: dict[str, int]
    by_slug: dict[str, int]


def load_view_analytics(views_column: str = DEFAULT_VIEWS_COLUMN) -> ViewAnalytics:
    """Load chart view counts from grapher MySQL.

    `analytics_chart_views` is the only table with per-view granularity:
    `analytics_pageviews` strips query strings entirely, and
    `analytics_popularity` has one row per chart. Restricted to the latest
    snapshot day — the primary key allows several, and summing across them would
    silently multiply every weight.
    """
    rows = OWID_ENV.read_sql(
        f"""
        SELECT chart_slug, view_config_id, type, {views_column} AS views
        FROM analytics_chart_views
        WHERE day = (SELECT MAX(day) FROM analytics_chart_views)
        """
    )
    if rows.empty:
        raise EmptyAnalyticsError(
            "analytics_chart_views is empty, so charts cannot be weighted by how much "
            "they are viewed.\n"
            "  It ships only in the private data dump: run `make refresh.private` in "
            "owid-grapher, or point OWID_ENV at a staging/production database.\n"
            "  To generate without popularity weighting, pass --weighting uniform."
        )

    by_config_id: dict[str, int] = {}
    by_slug: dict[str, int] = {}
    slug_totals: dict[str, list[int]] = {}
    for row in rows.itertuples(index=False):
        views = int(row.views or 0)
        if row.view_config_id:
            by_config_id[row.view_config_id] = views
            slug_totals.setdefault(row.chart_slug, []).append(views)
        elif row.chart_slug:
            by_slug[row.chart_slug] = views

    average_by_slug = {
        slug: round(sum(counts) / len(counts)) for slug, counts in slug_totals.items()
    }
    return ViewAnalytics(
        by_config_id=by_config_id, average_by_slug=average_by_slug, by_slug=by_slug
    )


def load_mdim_view_config_ids() -> dict[tuple[str, frozenset], str]:
    """Map a multi-dim view's (slug, dimension choices) to its chart config id.

    The record's `queryParams` is those same choices, sorted and url-encoded
    (`dimensionsToSortedQueryStr`), and analytics are keyed by the config id — so
    this table is what connects the two. Keyed on the parsed choices rather than
    the encoded string, which sidesteps every encoding discrepancy between
    Python's urlencode and the browser's URLSearchParams.
    """
    rows = OWID_ENV.read_sql(
        "SELECT slug, config FROM multi_dim_data_pages WHERE published = 1"
    )
    mapping: dict[tuple[str, frozenset], str] = {}
    for row in rows.itertuples(index=False):
        config = json.loads(row.config) if isinstance(row.config, str) else row.config
        for view in config.get("views") or []:
            dimensions = view.get("dimensions") or {}
            config_id = view.get("fullConfigId")
            if not config_id:
                continue
            mapping[(row.slug, frozenset(dimensions.items()))] = config_id
    return mapping


@dataclass
class WeightedUniverse:
    """A topic's records, each with a weight and a note on how it was resolved."""

    topic_name: str
    records: dict[str, dict]
    weights: dict[str, float]
    resolution: dict[str, str]  # identity -> "per-view" | "averaged" | "unmatched"

    @property
    def total_weight(self) -> float:
        return sum(self.weights.values())

    def resolution_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for how in self.resolution.values():
            counts[how] = counts.get(how, 0) + 1
        return counts


def weigh_universe(
    topic_name: str,
    records: list[dict],
    analytics: ViewAnalytics | None,
    mdim_config_ids: dict[tuple[str, frozenset], str],
) -> WeightedUniverse:
    """Attach a view count to every record in a topic's universe.

    Three resolutions, in order of preference:

    - *per-view* — the record's own view count. Plain charts join on their slug;
      multi-dim views join through their config id.
    - *averaged* — the average views per view of that explorer or multi-dim, used
      where a record's own view can't be identified. Explorer views always land
      here: their records carry no config id, and `explorer_views` is keyed by a
      slugified view id that can't be reconstructed from `queryParams`. Within
      one explorer every view therefore weighs the same, which understates its
      popular views and overstates its ignored ones.
    - *unmatched* — no analytics row at all, weight zero. Usually a record
      indexed since the last analytics snapshot.

    With `analytics` omitted every record weighs 1, so coverage degrades to a
    plain count of charts.
    """
    weights: dict[str, float] = {}
    resolution: dict[str, str] = {}
    by_identity = {record_identity(record): record for record in records}

    for identity, record in by_identity.items():
        if analytics is None:
            weights[identity] = 1.0
            resolution[identity] = "uniform"
            continue

        slug = record.get("slug") or ""
        config_id = record.get("chartConfigId")
        if not config_id and record.get("queryParams"):
            choices = frozenset(parse_qsl(record["queryParams"].lstrip("?")))
            config_id = mdim_config_ids.get((slug, choices))

        views = analytics.by_config_id.get(config_id) if config_id else None
        if views is None and not record.get("queryParams"):
            views = analytics.by_slug.get(slug)

        if views is not None:
            weights[identity] = float(views)
            resolution[identity] = "per-view"
            continue

        averaged = analytics.average_by_slug.get(slug)
        if averaged is not None:
            weights[identity] = float(averaged)
            resolution[identity] = "averaged"
        else:
            weights[identity] = 0.0
            resolution[identity] = "unmatched"

    return WeightedUniverse(
        topic_name=topic_name,
        records=by_identity,
        weights=weights,
        resolution=resolution,
    )


def match_identities(universe: WeightedUniverse, term: str) -> frozenset[str]:
    """The records the block would show for a term, within this topic."""
    return frozenset(
        identity
        for identity, record in universe.records.items()
        if record_matches_term(record, term)
    )


# ------------------------------------------------------------------- selection


@dataclass
class TermCoverage:
    """One selected term and what it reveals."""

    term: str
    own_weight: float
    own_count: int
    marginal_weight: float
    marginal_count: int
    cumulative_weight: float

    def own_share(self, total: float) -> float:
        return self.own_weight / total if total else 0.0

    def marginal_share(self, total: float) -> float:
        return self.marginal_weight / total if total else 0.0

    def cumulative_share(self, total: float) -> float:
        return self.cumulative_weight / total if total else 0.0


@dataclass
class TopicSelection:
    topic_name: str
    selected: list[TermCoverage] = field(default_factory=list)
    near_misses: list[TermCoverage] = field(default_factory=list)
    total_weight: float = 0.0
    total_count: int = 0
    uncovered: list[tuple[str, float]] = field(default_factory=list)
    # What the topic's own name alone would reach. A reader on the page has
    # already applied it, so it is the share of the topic that no suggestion can
    # narrow — and the reason a topic like Life Expectancy, half of whose traffic
    # is one chart called "Life expectancy", cannot score highly however good its
    # terms are. Reported rather than subtracted: the coverage number stays a
    # plain share of the topic, and this says how much of it was reachable.
    topic_name_share: float = 0.0

    @property
    def covered_weight(self) -> float:
        return self.selected[-1].cumulative_weight if self.selected else 0.0


def offerable_candidates(candidates: list[str], topic_name: str) -> list[str]:
    """Drop the candidates the site would refuse to show anyway.

    Two rules, both copied from rankSuggestedKeywords in owid-grapher: a term the
    topic's own name already contains narrows nothing for a reader already on
    that page, and places are never suggested. Applying them here rather than
    only there matters because the site drops them *after* truncating to five, so
    a term it will discard otherwise costs a slot and shortens the line.

    Deliberately no rule against a term merely resembling the topic name.
    "Child mortality" on "Child & Infant Mortality" is that topic's single most
    important term — it names the charts holding half its traffic — and only the
    exact-containment test above can be trusted to remove a term that truly
    narrows nothing.
    """
    lower_topic = topic_name.lower()
    return [
        candidate
        for candidate in candidates
        if candidate.lower() not in lower_topic and not _is_place_name(candidate)
    ]


# Region names are the site's business, and it filters them itself; here we only
# need the obvious ones out of the way so they don't win a slot on coverage. The
# site's check (getRegionByNameOrVariantName) knows every variant name, so this is
# a cheap first pass rather than a replacement for it.
_COMMON_PLACE_WORDS = frozenset(
    {"world", "africa", "asia", "europe", "america", "oceania", "antarctica"}
)


def _is_place_name(term: str) -> bool:
    return term.strip().lower() in _COMMON_PLACE_WORDS


def select_terms_by_coverage(
    universe: WeightedUniverse,
    candidates: list[str],
    max_terms: int,
    min_marginal_share: float,
) -> TopicSelection:
    """Pick the terms that reveal the most of a topic, most-revealing first.

    Greedy weighted maximum coverage: take the term covering the most, then
    repeatedly take whichever term adds the most that is still uncovered. That is
    the requirement stated directly — the first term has the largest coverage and
    each next one the largest addition — and it comes with the standard
    (1 - 1/e) approximation bound. It does *not* find the optimal set: a pair of
    terms that between them cover everything can be missed because one of them
    lost the first round.

    Two things fall out of it that used to need a second LLM pass. A term whose
    rows are all covered already has zero marginal weight and is never taken, so
    near-synonyms can't stack up — the old line offering "missing women",
    "sex-selective abortion" and "excess female mortality", three terms and two
    charts. And a term the block would show nothing for covers nothing, so it
    can't be taken at all.
    """
    candidates = offerable_candidates(candidates, universe.topic_name)
    matches = {term: match_identities(universe, term) for term in candidates}
    total_weight = universe.total_weight
    total_count = len(universe.records)
    weight_of = universe.weights

    def weight(identities: frozenset[str]) -> float:
        return sum(weight_of.get(identity, 0.0) for identity in identities)

    self_named = match_identities(universe, universe.topic_name)
    selection = TopicSelection(
        topic_name=universe.topic_name,
        total_weight=total_weight,
        total_count=total_count,
        topic_name_share=(weight(self_named) / total_weight if total_weight else 0.0),
    )
    covered: set[str] = set()
    remaining = list(candidates)
    floor = min_marginal_share * total_weight

    while remaining and len(selection.selected) < max_terms:
        scored = []
        for index, term in enumerate(remaining):
            new = matches[term] - covered
            if not new:
                continue
            scored.append(
                (
                    -weight(new),  # most additional weight
                    -len(new),  # then most additional charts
                    -weight(matches[term]),  # then describes more of the list
                    len(split_into_match_words(term)),  # then fewer words
                    index,  # then the order the candidates arrived in
                    term,
                )
            )
        if not scored:
            break
        scored.sort()
        best = scored[0]
        term = best[-1]
        new = matches[term] - covered
        marginal = weight(new)

        # A term revealing less than the floor isn't worth one of five slots on a
        # single line. Stop without recording it here — it stays in `remaining`,
        # so the near-miss pass below reports it (first, since it has the largest
        # remaining gain) along with whatever else just missed out.
        if marginal < floor and selection.selected:
            break

        selection.selected.append(
            TermCoverage(
                term=term,
                own_weight=weight(matches[term]),
                own_count=len(matches[term]),
                marginal_weight=marginal,
                marginal_count=len(new),
                cumulative_weight=selection.covered_weight + marginal,
            )
        )
        covered |= new
        remaining.remove(term)

    # The best few terms that didn't make it, for the same reason.
    for _, _, _, _, _, term in sorted(
        (
            (
                -weight(matches[term] - covered),
                -len(matches[term] - covered),
                0.0,
                0,
                index,
                term,
            )
            for index, term in enumerate(remaining)
            if matches[term] - covered
        )
    )[:3]:
        new = matches[term] - covered
        selection.near_misses.append(
            TermCoverage(
                term=term,
                own_weight=weight(matches[term]),
                own_count=len(matches[term]),
                marginal_weight=weight(new),
                marginal_count=len(new),
                cumulative_weight=selection.covered_weight + weight(new),
            )
        )

    selection.uncovered = sorted(
        (
            (universe.records[identity].get("title") or identity, weight_of[identity])
            for identity in universe.records
            if identity not in covered
        ),
        key=lambda pair: -pair[1],
    )
    return selection
