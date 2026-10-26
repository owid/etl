"""Ask a model whether a published chart would mislead a reader.

The prompt is doing the work here, so what it must contain is worth stating:

1. **Today's date.** Without it the model reasons from its own sense of "now" and flags recent
   data as future-dated. That produced a false positive on ``weekly-growth-covid-deaths``
   (data to 2026-07-19, reviewed on 2026-08-31) and another on a 2025 UN figure. One sentence
   removes the whole class.
2. **Permission to return nothing.** Free-text prompts produce commentary on every chart they
   are shown. A structured output plus an explicit "an ordinary chart has no issues" is what
   makes an empty result mean something.
3. **The world-events paragraph.** Telling the model that wars, epidemics and methodology
   breaks legitimately move series reads like a damper, but measured against three charts with
   known findings it went the other way: 3/3 with it, 1/3 without. It seems to license the model
   to make a judgement rather than hedge.
4. **A reader-impact field.** A finding that cannot name what a reader would wrongly conclude
   is usually noise, and asking for it up front suppresses more of it than any threshold.
5. **That our own documentation of the data outranks the model's suspicion.** The bundle carries
   the indicator's key facts and processing notes, which is where a genuine-but-alarming value is
   explained; a discontinuity the metadata accounts for is documented, not a finding. This is the
   route a false positive is closed by — see ``COLUMN_FIELDS`` in ``bundle.py``. Deliberately
   stated as a rule about the metadata and not as a list of cases: an earlier draft named the
   Central African Republic here, which suppressed that one chart by name and would have taught
   nothing about the next one.
6. **The one case that is named.** The post-Soviet break in producers' aggregates is not a false
   positive — the 1992 step really is there and really does mislead — but it is in the source, in
   every aggregate built from it, and the answer every time it is raised is that nothing will be
   done. It is named exactly because the rule above would be the wrong shape for it: generalising
   it to coverage discontinuities at large would silence the ones we did cause and could fix.

What this catches that statistical detection cannot: an error that is stable over time and
statistically unremarkable, where the only tell is knowing something about the world. The
motivating case is the UN swapping female and male across SDG 16.2.2 — smooth, internally
consistent, and wrong through three consecutive annual imports. Time-series and distribution
tests are ``apps/anomalist``'s job and are deliberately not duplicated here.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_ai import Agent, BinaryContent

from apps.chart_critic.bundle import Bundle

# Sonnet 5.5 at effort high (see ANTHROPIC_SETTINGS) replaced gemini-3.8-flash as the default.
# On charts with known answers it caught 8 of 9 passes on real findings against Gemini's 6, never
# raised a false positive Gemini raised twice, and stayed quiet on clean charts. It costs about
# $0.04 a chart with caching against Gemini's $0.035. Haiku 5.5 caught none, with or without the
# render, so the small Claude tier is not a cheap substitute.
#
# Opus 5.5 replaced Sonnet 5.5 as the default. A side-by-side showed little difference, but at
# twice Sonnet's price it is still only about $0.08 a chart, the daily run costs a few dollars a
# month, and the team relies on the findings.
DEFAULT_MODEL = "anthropic:claude-opus-5-5"

# Settings for Claude models, which get none of them by default:
# - Thinking is switched on explicitly: answering without it took about 40 output tokens a pass and
#   found nothing. (pydantic-ai v2 returns structured output natively rather than through a forced
#   tool call, which Anthropic allows no thinking alongside.)
# - Effort "high": on charts with known answers, it was the Sonnet 5.5 setting that raised nothing
#   on clean charts; at low and medium it flagged them nearly every pass.
# - Caching the user turn lets passes 2 and 3 read the prompt from cache, about a third off the
#   bill. Only the input is cached; each pass still generates its own answer, so the repeat
#   passes keep their value.
ANTHROPIC_SETTINGS = {
    "anthropic_thinking": {"type": "adaptive"},
    "anthropic_effort": "high",
    "anthropic_cache_messages": True,
    "max_tokens": 32000,
}


class Issue(BaseModel):
    """One thing wrong with a chart."""

    severity: Literal["high", "medium", "low"] = Field(
        description=(
            "Decides whether anyone sees the finding: 'medium' and 'high' are posted for OWID's data "
            "team to check, 'low' only goes into a report. Rate the reader's loss — how wrong it is "
            "and how many readers it reaches — not how odd the defect looks. "
            "'high': it changes what someone concludes about a country or region people care "
            "about, in a period people look at — a wrong unit, an impossible value, a series "
            "that contradicts the chart's own text. "
            "'medium': real and visible, but narrower — one minor series, an old period nobody "
            "revisits, or text that disagrees with the data without changing the headline reading. "
            "'low': technically true but nobody acts on it — a typo that misleads no one, a "
            "naming inconsistency, or a defect confined to a micro-territory or a range that "
            "ends decades ago; a correct observation about Tuvalu in 1965 is 'low', not 'high'. "
            "Readers mostly see the chart as it opens and then look up large countries, so a "
            "defect in a small country that the default view does not show reaches few of them, "
            "and so does any defect on a chart that has been published for a while and is rarely "
            "read. Those are usually 'low'. But a serious error, such as a wrong unit or an "
            "impossible value, matters however few people have read the chart so far, and a "
            "recently published chart has not yet had time to collect readers. A discontinuity "
            "that looks like an artefact of how the producer measured the data sits in every chart "
            "and download of that series, so rate it by how much it misleads, not by who looks at "
            "that country."
        )
    )
    kind: Literal["data", "chart"] = Field(
        description=(
            "'data' if the indicator's values or metadata are wrong — which also affects every "
            "other chart on that indicator, the data page, the API and every download. "
            "'chart' if only this chart's configuration or text is wrong."
        )
    )
    claim: str = Field(description="One sentence: what is wrong.")
    evidence: str = Field(description="The specific numbers or strings relied on.")
    reader_impact: str = Field(description="What a reader would wrongly conclude.")
    confidence: Literal["high", "medium", "low"] = Field(
        description=(
            "How sure you are that this is actually wrong — a separate question from how much it "
            "matters. 'high' when what you were shown is enough to establish it, 'low' when it "
            "would need checking against the source or an expert. Say 'low' rather than dropping "
            "a suspicion."
        )
    )
    found_in: Literal[
        "rendered_chart",
        "chart_text",
        "indicator_metadata",
        "data_summary",
        "chart_config",
        "contradiction_between_fields",
    ] = Field(
        default="data_summary",
        description=(
            "Which part of what you were shown carried this finding; "
            "'contradiction_between_fields' when two parts disagree with each other."
        ),
    )
    chart_params: str = Field(
        default="",
        description=(
            "Grapher query parameters that put this problem on screen — the entities and time range "
            "it is about — so a reviewer lands on it instead of the default view. Entities are "
            "joined with ~: countries by ISO3 code, anything else (World, a region, an income "
            "group, a category) by its name as it appears in the data, with spaces as %20. E.g. "
            "'country=~CAF&time=2000..latest' or 'country=~World~GBR&time=1945..1960&tab=line'. "
            "Leave empty if the default view already shows it."
        ),
    )


class Review(BaseModel):
    issues: list[Issue] = Field(default_factory=list)


INSTRUCTIONS = """You are reviewing a published Our World in Data chart for anything that would
mislead its readers.

Today's date is {today}. Data up to this date is current, not future-dated.

Look for anything that would make a reader believe something false:
- values that are implausible given what you know about the world
- text that does not match the data
- a unit, denominator or scale that is wrong
- a breakdown pointing the wrong way (a share, sex, age or income split that is inverted)
- coverage or aggregation that misleads
- a chart that renders empty, or stops before its data does

Judge the numbers against what you know actually happened. A war, an epidemic or a policy
change can move a series enormously and legitimately; say nothing about those.

A change in how something was *measured* is different, and worth flagging: a survey redesign,
a rebased index or a source switch produces a jump that a reader will read as a real-world
trend. Say so when a discontinuity looks like an artefact of measurement rather than an event,
unless the indicator's metadata explains it (see below).

One discontinuity is the exception, because it is already known and nothing will be done about
it: producers' world and regional aggregates exclude the Soviet Union before 1992 and count its
successor states from 1992 onwards, so a global or European series can step up around 1991-1992.
Say nothing about that step. This covers the post-Soviet coverage break and nothing else — any
other change in which countries a series covers is still worth flagging.

The indicator's key facts and processing notes are our own documentation of the data, written
for readers. They are where a value that looks impossible is explained — a crisis adjustment
added on top of a modelled series, a source switch, a deliberate aggregation. An extreme value
or a discontinuity that the metadata accounts for is documented rather than wrong: say nothing
about it. Judge it against what the metadata actually says, not against whether the number looks
extreme — an explanation that does not cover the value you are looking at is not a licence to
dismiss it.

Your knowledge of definitions and thresholds may be out of date, and the chart's own metadata
is more current than you are. Do not flag a definition, a poverty line, a classification or a
unit convention merely because it differs from what you remember — flag it only when the chart
contradicts itself or contradicts its own data. (A real example of getting this wrong: objecting
that the International Poverty Line is not $3 per day, when it was revised to exactly that.)

Do not invent problems. An ordinary chart has no issues, and returning an empty list is the
expected outcome for most charts."""


# How much of a claim's significant words two findings must share to be the same finding. The
# model rewords freely — "the subtitle states prices are in constant 2023 US$" and "the chart
# subtitle states prices are measured in constant 2023 US$" are one claim — so identity has to
# survive rewording rather than be a hash of the sentence.
#
# Used only to merge the repeat passes of a single run, where the passes describe the same bundle
# and overlap is high. The digest used the same test to recognise across days what it had already
# posted, and there it failed: after an edit the model rewords a finding far more, and one ticked
# on Monday came back on Tuesday at 0.30. Across days the digest now remembers the chart, not the
# claim — see ``digest.COOLDOWN_DAYS``.
CLAIM_OVERLAP = 0.4


def claim_tokens(claim: str) -> set[str]:
    """The significant words of a claim, crudely singular-folded.

    Without the folding, "the unit for all three indicators is incorrectly set to 'doses'" and
    "the indicator unit is incorrectly set to 'doses'" scored below the threshold and were
    reported as two findings.
    """
    return {w.rstrip("s") for w in re.findall(r"[a-z0-9]{4,}", claim.lower())}


def same_claim(tokens: set[str], other: set[str]) -> bool:
    """Whether two claims, reduced to :func:`claim_tokens`, say the same thing."""
    return len(tokens & other) / max(len(tokens | other), 1) >= CLAIM_OVERLAP


def issue_params(target_params: str, issue: dict) -> str:
    """The query string for the view a finding is about.

    Two things narrow a view and both have to survive: the params the chart was *reviewed* at (an
    mdim view, or a ``--slugs`` entry carrying its own query string) and the params the model
    attached to the finding. Dropping the first sends a reader to the multi-dim default, which is
    a different chart than the one the claim is about.
    """
    return "&".join(x for x in (target_params, issue.get("chart_params", "")) if x)


def format_views(views_365d: int | float | None) -> str:
    """Readership as views per day, which is how OWID states it.

    A decimal below ten, because "0 views/day" reads as "nobody looks at this" when the real
    figure is a few hundred a year.
    """
    if not views_365d:
        return ""
    per_day = views_365d / 365
    return f"{per_day:,.0f} views/day" if per_day >= 10 else f"{per_day:,.1f} views/day"


def readership_line(
    views_365d: int | float | None,
    published: dt.datetime | dt.date | None,
    typical_views_365d: int | float | None = None,
) -> str:
    """How much the chart is read, for the model to weigh in its severity call.

    Per day over the days the chart has been live, not over a flat year: a chart published last
    week has a few hundred views in its annual total and would otherwise read as one nobody opens.
    Rounded to one significant figure, so the daily drift in pageviews does not change what the
    model is shown and throw away its cached reviews.

    ``typical_views_365d`` is the median chart's, because "rarely read" needs a reference point:
    most charts get a few views a day, which the model has no way to know.
    """
    today = dt.date.today()
    day = published.date() if isinstance(published, dt.datetime) else published
    live_days = min(max((today - day).days, 1), 365) if day else 365
    if views_365d:
        per_day = float(f"{views_365d / live_days:.1g}")
        read = f"about {per_day:g} view{'' if per_day == 1 else 's'} a day"
    else:
        read = "no recorded views"
    since = f", published {day.isoformat()}" if day else ""
    typical = (
        f" (a typical chart gets about {float(f'{typical_views_365d / 365:.1g}'):g} a day)"
        if typical_views_365d
        else ""
    )
    return f"Readership: {read}{since}{typical}."


def build_agent(model: str = DEFAULT_MODEL) -> Agent[None, Review]:
    """An agent with no system instructions — see :func:`prompt_parts` for why.

    No sampling settings: current models reject them. The model raises a genuine finding on some
    passes and not others, so repeat passes are the recall dial — see ``--repeat``. Claude models
    get :data:`ANTHROPIC_SETTINGS`; any other ``--model`` is used with its defaults.
    """
    if not model.startswith("anthropic:"):
        return Agent[None, Review](model=model, output_type=Review, retries=2)
    # The critic's own key when one is set; see etl.config.
    from pydantic_ai.models.anthropic import AnthropicModel
    from pydantic_ai.providers.anthropic import AnthropicProvider

    from etl import config

    if not config.ANTHROPIC_API_KEY_CHART_CRITIC:
        raise RuntimeError(f"{model} needs ANTHROPIC_API_KEY_CHART_CRITIC (or ANTHROPIC_API_KEY) in .env")
    claude = AnthropicModel(
        model.removeprefix("anthropic:"), provider=AnthropicProvider(api_key=config.ANTHROPIC_API_KEY_CHART_CRITIC)
    )
    return Agent[None, Review](model=claude, output_type=Review, retries=2, model_settings=ANTHROPIC_SETTINGS)  # ty: ignore[invalid-argument-type]


def prompt_parts(
    bundle: Bundle, extra_views: list[tuple[str, bytes]] | None = None, readership: str = "", params: str = ""
) -> list[str | BinaryContent]:
    """The user turn: the render, then the metadata, numbers and readership, then the instruction.

    The instruction goes **here rather than in the agent's system instructions**, and the
    difference is large. Measured over three passes each on two charts with known findings and
    one clean control:

    ================================  ===================  ===================
    finding                           system instructions  instruction in turn
    ================================  ===================  ===================
    COVID vaccination baseline        0/3                  2/3
    misspelled subtitle               0/3                  3/3
    clean control (``literacy``)      0/3                  0/3
    ================================  ===================  ===================

    So recall roughly trebles with no new false positives. Keep it in the user turn.
    """
    parts: list[str | BinaryContent] = []
    if bundle.png:
        # A reviewed view (an mdim view, a --params run) is not what a reader first sees.
        if params:
            parts.append(f"Image 1 — the view under review, at ?{params}:")
        else:
            parts.append("Image 1 — the chart's default view, as a reader first sees it:")
        parts.append(BinaryContent(data=bundle.png, media_type="image/png"))
    # Extra views matter because the default view is usually not where a problem is visible.
    # In #we-need-to-correct-it almost every reported chart link carries query parameters.
    for i, (label, png) in enumerate(extra_views or [], start=2):
        parts.append(f"Image {i} — {label}:")
        parts.append(BinaryContent(data=png, media_type="image/png"))
    parts.append(bundle.summary)
    if readership:
        parts.append(readership)
    parts.append(INSTRUCTIONS.format(today=dt.date.today().isoformat()))
    return parts
