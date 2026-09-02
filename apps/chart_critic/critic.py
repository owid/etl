"""Ask a model whether a published chart would mislead a reader.

The prompt is doing the work here, so the three things it must contain are worth stating:

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

What this catches that statistical detection cannot: an error that is stable over time and
statistically unremarkable, where the only tell is knowing something about the world. The
motivating case is the UN swapping female and male across SDG 16.2.2 — smooth, internally
consistent, and wrong through three consecutive annual imports. Time-series and distribution
tests are ``apps/anomalist``'s job and are deliberately not duplicated here.
"""

from __future__ import annotations

import datetime as dt
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_ai import Agent, BinaryContent

from apps.chart_critic.bundle import Bundle

# Tested defaults. The flash tier is enough: on the SDG 16.2.2 case both flash and flash-lite
# identified the swap from the render alone, and flash-lite did it for ~$0.0002 a call.
DEFAULT_MODEL = "google:gemini-3.7-flash"
CHEAP_MODEL = "google:gemini-3.1-flash-lite"

# Fallback prices in $ per million tokens (input, output), from Google's published rates.
# `genai_prices` (used by apps.utils.llms.costs) does not carry these models yet; drop an entry
# here once it does, and note that gemini-3.7-flash doubles to 1.50/7.50 on 2027-01-01.
FALLBACK_PRICES = {
    "google:gemini-3.7-flash": (0.75, 3.75),
    "google:gemini-3.1-flash-lite": (0.10, 0.40),
}


class Issue(BaseModel):
    """One thing wrong with a chart."""

    severity: Literal["high", "medium", "low"] = Field(
        description=(
            "How much this misleads a reader who is actually looking at the chart. "
            "'high': it changes what someone concludes about a country or region people care "
            "about, in a period people look at — a wrong unit, an impossible value, a series "
            "that contradicts the chart's own text. "
            "'medium': real and visible, but narrower — one minor series, an old period nobody "
            "revisits, or text that disagrees with the data without changing the headline reading. "
            "'low': technically true but nobody acts on it — a typo that misleads no one, a "
            "naming inconsistency, or a defect confined to a micro-territory or a range that "
            "ends decades ago. Judge the reader's loss, not how odd the defect looks: a "
            "correct observation about Tuvalu in 1965 is 'low', not 'high'."
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
            "a suspicion, and never inflate it to make a finding look stronger."
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
            "Which part of what you were shown carried this finding. 'rendered_chart' for something "
            "only the picture shows; 'chart_text' for the title, subtitle or note; "
            "'indicator_metadata' for a unit, description or column name; 'data_summary' for the "
            "numbers; 'chart_config' for the configuration; "
            "'contradiction_between_fields' when two of them disagree with each other."
        ),
    )
    chart_params: str = Field(
        default="",
        description=(
            "Grapher query parameters that make this problem visible, so a reviewer lands on it "
            "instead of the default view. Use ISO3 codes joined with ~ for entities, e.g. "
            "'country=~CAF&time=2000..latest' or 'country=~GBR&time=1945..1960&tab=line'. "
            "Leave empty if the default view already shows it."
        ),
    )


class Review(BaseModel):
    issues: list[Issue] = Field(default_factory=list)


INSTRUCTIONS = """You are reviewing a published Our World in Data chart before readers see it.

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
trend. Say so when a discontinuity looks like an artefact of measurement rather than an event.

For each issue, set chart_params to the grapher query parameters that put the problem on
screen — the entity and time range you are talking about — so a reviewer sees it immediately
rather than the chart's default view.

Your knowledge of definitions and thresholds may be out of date, and the chart's own metadata
is more current than you are. Do not flag a definition, a poverty line, a classification or a
unit convention merely because it differs from what you remember — flag it only when the chart
contradicts itself or contradicts its own data. (A real example of getting this wrong: objecting
that the International Poverty Line is not $3 per day, when it was revised to exactly that.)

Do not invent problems. An ordinary chart has no issues, and returning an empty list is the
expected outcome for most charts."""


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


def build_agent(model: str = DEFAULT_MODEL) -> Agent[None, Review]:
    """An agent with no system instructions — see :func:`prompt_parts` for why.

    Temperature is left at the model default on purpose. Pinning it to 0 was measured as
    *worse* for recall (one known finding went from 1/3 passes to 0/3) and it does not buy
    reproducibility anyway: this model raises a genuine finding on some passes and not others
    whatever the temperature. Repeat passes, not temperature, are the dial — see ``--repeat``.
    """
    return Agent[None, Review](model=model, output_type=Review, retries=2)


def prompt_parts(bundle: Bundle, extra_views: list[tuple[str, bytes]] | None = None) -> list[str | BinaryContent]:
    """The user turn: the render, then the metadata and numbers, then the instruction.

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
        parts.append("Image 1 — the chart's default view, as a reader first sees it:")
        parts.append(BinaryContent(data=bundle.png, media_type="image/png"))
    # Extra views matter because the default view is usually not where a problem is visible.
    # In #we-need-to-correct-it almost every reported chart link carries query parameters.
    for i, (label, png) in enumerate(extra_views or [], start=2):
        parts.append(f"Image {i} — {label}:")
        parts.append(BinaryContent(data=png, media_type="image/png"))
    parts.append(bundle.summary)
    parts.append(INSTRUCTIONS.format(today=dt.date.today().isoformat()))
    return parts
