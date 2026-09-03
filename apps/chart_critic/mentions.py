"""Who to tell about a finding, resolved to a Slack mention.

A finding addressed to nobody gets read and forgotten. Who to address is two different
questions, so it has two different answers:

- **A chart-level finding** — a subtitle that contradicts the indicator, a unit that is not the
  unit on display — belongs to whoever last edited the chart, but *only* in the default sweep,
  where the chart is under review precisely because they edited it in the last day. Outside that
  window the last editor may have changed a colour two years ago, and naming them is noise. The
  caller decides, via ``tag_last_editor``.
- **A data-level finding** belongs to the dataset's accountable owner: the first entry of
  ``owners`` in the step's ``.meta.yml``, which most live datasets declare. That is a stated
  responsibility rather than a guess about it, which is why nothing here falls back to git blame
  or to the chart's most frequent editor — neither of those means ownership, and both would put
  a real person's name on a claim they have nothing to do with.

Addresses come from grapher's ``users`` table and member ids from Slack's ``users.lookupByEmail``
(the owidbot token carries ``users:read.email``). Every step fails soft to the plain name: an
unresolved person reads as "last edited by Pablo Rosado", which still says who to ask, rather
than as a broken mention.
"""

from __future__ import annotations

import functools
from typing import Any

from slack_sdk.errors import SlackApiError
from structlog import get_logger
from unidecode import unidecode

log = get_logger()

# Service accounts that turn up as a chart's last editor — an ETL grapher step upserting a config
# is the common one. There is no person behind them and no Slack account either, so naming them
# would just be a line of noise in every message.
SERVICE_ACCOUNTS = {"etl@ourworldindata.org"}


def _key(name: str | None) -> str:
    """Match names across the accent that grapher's account happens not to carry.

    ``owners`` says "Mojmír Vinkler" (the canonical spelling in ``etl.owners``) while the
    grapher account says "Mojmir Vinkler", and the two must still be the same person.
    """
    return unidecode(name or "").casefold().strip()


@functools.cache
def _emails_by_name() -> dict[str, str]:
    """``{normalised full name: email}`` for people who still work here.

    Inactive accounts are left out on purpose: a former colleague named as a dataset owner should
    not be looked up, and the plain-name fallback is the right outcome for them.
    """
    from etl.db import read_sql

    try:
        df = read_sql("SELECT fullName, email FROM users WHERE isActive = 1 AND email IS NOT NULL")
    except Exception as e:  # noqa: BLE001 — a mention is a courtesy, never the finding
        log.warning("chart_critic.user_emails_failed", error=str(e))
        return {}
    return {_key(row.fullName): str(row.email) for row in df.itertuples() if _key(row.fullName)}


@functools.cache
def _member_id(email: str) -> str | None:
    """Slack member id for an email address, or ``None`` if Slack does not know it."""
    from etl import config
    from etl.slack_helpers import slack_client

    if not config.SLACK_API_TOKEN:
        return None
    try:
        return str(slack_client.users_lookupByEmail(email=email)["user"]["id"])
    except SlackApiError as e:
        log.warning("chart_critic.slack_lookup_failed", email=email, error=str(e))
        return None


def mention(name: str | None, email: str | None = None) -> str | None:
    """``<@U123>`` when Slack knows the person, their plain name when it does not, ``None`` when
    there is nobody to name."""
    email = email or _emails_by_name().get(_key(name))
    if email in SERVICE_ACCOUNTS:
        return None
    member_id = _member_id(email) if email else None
    return f"<@{member_id}>" if member_id else (name or None)


def attach(facts: dict[str, dict[str, Any]], tag_last_editor: bool) -> None:
    """Resolve every chart's mentions in place, before any message is built.

    Called once from the CLI so the database and Slack lookups happen in one visible place and
    the message formatting stays a pure function of its inputs.
    """
    for chart in facts.values():
        editor = chart.get("editor") or {}
        chart["editor_mention"] = mention(editor.get("name"), editor.get("email")) if tag_last_editor else None
        chart["owner_mentions"] = [m for m in (mention(owner) for owner in chart.get("owners") or []) if m]
