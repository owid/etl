"""WIP: Currently integrating Draft charts into pipeline

NEXT STEPS:
    - QA #lucas-playground with Ed. Daily frequency is fine?
    - We currently allow for re-suggestions if suggested more than 1 year ago. Check with Ed.
    - We should detect when a chart is being "re-suggested", and include the thread that last-suggested it. For this we need more permissions for our bot
        1. Go to https://api.slack.com/apps
        2. Select your app
        3. Go to OAuth & Permissions → Scopes → Bot Token Scopes
        4. Add channels:history (and groups:history for private channels)
        5. Reinstall the app to your workspace (required after adding scopes)
"""

import json
from collections import defaultdict
from datetime import datetime

import pandas as pd
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from apps.housekeeper.utils import (
    MODEL_DEFAULT_PRETTY,
    TODAY,
    get_chart_summary,
    owidb_get_reviews_id,
    owidb_submit_review_id,
)
from etl.analytics.metabase import get_question_data, read_metabase
from etl.config import OWID_ENV, SLACK_API_TOKEN
from etl.slack_helpers import send_slack_message

log = get_logger()

# Default reviewers for daily chart reviews
DAILY_CHART_REVIEWER_DEFAULT = "Fiona"
DAILY_DRAFT_CHART_REVIEWER_DEFAULT = "Fiona"


####################################
# Main entry point
####################################
def send_slack_chart_reviews(
    channel_name: str,
    include_published: bool = True,
    include_draft: bool = True,
):
    """Send daily chart reviews to Slack (both published and draft).

    This is the main entry point that fetches data once and runs both pipelines.

    Args:
        channel_name: Name of the Slack channel to send the message to.
        include_published: Whether to send a published chart review.
        include_draft: Whether to send a draft chart review.
    """
    log.info("Getting all charts to review")
    df_published, df_draft = get_all_charts_to_review()

    # Get user data (slack usernames)
    slack_users = get_usernames()
    # Uncomment below if you want to test the workflow without tagging people
    # slack_users = {k: f"_{v}" for k, v in slack_users.items()}

    if include_published and not df_published.empty:
        _send_published_chart_review(
            chart=df_published.iloc[0],
            channel_name=channel_name,
            slack_username="Daily chart",
            icon_emoji="sus-blue",
            slack_users=slack_users,
        )
    elif include_published:
        log.info("No published charts to review")

    if include_draft and not df_draft.empty:
        _send_draft_chart_review(
            chart=df_draft.iloc[0],
            channel_name=channel_name,
            slack_username="Daily draft chart",
            icon_emoji="sus-white",
            slack_users=slack_users,
        )
    elif include_draft:
        log.info("No draft charts to review")


####################################
# Data fetching
####################################
def get_all_charts_to_review():
    """Get both published and draft charts that need review.

    Fetches from Metabase once, filters out charts reviewed in the last year,
    and returns two filtered dataframes.

    Returns:
        tuple: (df_published, df_draft) - DataFrames of charts to review
    """
    # Fetch all charts from Metabase (question 812)
    df = get_question_data(812, prod=True)

    # Dtypes / Parsing
    df = df.astype(
        {
            "last_edited_at": "datetime64[ns]",
            "created_at": "datetime64[ns]",
        }
    )
    df["post_details"] = df["post_details"].apply(lambda x: json.loads(x) if pd.notna(x) else [])
    df["revisions"] = df["revisions"].apply(lambda x: json.loads(x) if pd.notna(x) else [])

    # Skip charts reviewed in the last year (both published and drafts use same object_type)
    reviews_id = owidb_get_reviews_id(object_type="chart")
    df = df.loc[~df["chart_id"].isin(reviews_id)]

    # Split into published and draft
    df_published = df.loc[df["is_published"]].sort_values(["views_365d", "views_14d", "views_7d"], ascending=True)
    df_draft = df.loc[~df["is_published"]].sort_values("last_edited_at", ascending=True)

    return df_published, df_draft


####################################
# Published chart review pipeline
####################################
def _send_published_chart_review(
    chart,
    channel_name: str,
    slack_username: str,
    icon_emoji: str,
    slack_users: dict[str, str],
):
    """Send a published chart review to Slack.

    Args:
        chart: Chart row (pandas Series) to review.
        channel_name: Name of the Slack channel.
        slack_username: Username to use when sending the message.
        icon_emoji: Emoji to use as icon.
    """
    log.info(f"Selected published chart: {chart['chart_id']}, {chart['slug']}")

    # Get references
    refs = get_references(chart["chart_id"])

    # Prepare message
    message = build_published_message(chart, refs)

    # Send message
    if SLACK_API_TOKEN:
        log.info("Sending published chart message...")
        image_url = OWID_ENV.thumb_url(chart["slug"])
        response = send_slack_message(
            message=message,
            channel=channel_name,
            image_url=image_url,
            icon_emoji=icon_emoji,
            username=slack_username,
        )

        # Send thread messages
        kwargs = {
            "channel": channel_name,
            "icon_emoji": icon_emoji,
            "username": slack_username,
            "thread_ts": response["ts"],
        }
        _send_published_extra_messages(chart, refs, **kwargs)

        # Add chart to reviewed
        owidb_submit_review_id(object_type="chart", object_id=chart["chart_id"])


def build_published_message(chart, refs):
    """Build message for published chart review."""
    message_usage = _get_published_message_usage(chart, refs)
    date_str = TODAY.strftime("%d %b, %Y")
    message = (
        f"[{date_str}] *Decide whether to keep <{OWID_ENV.chart_admin_site(chart['chart_id'])}|this chart> online* <@{DAILY_CHART_REVIEWER_DEFAULT}>\n"
        f"_{message_usage}_\n"
    )
    return message


def _get_published_message_usage(chart, refs):
    """Get brief message about chart usage (views and references)."""
    msg_chart_views = f"{chart['views_365d']:.0f} views last year"

    num_posts = len(refs.get("postsGdocs", [])) + len(refs.get("postsWordpress", []))
    num_explorers = len(refs.get("explorers", []))
    if num_posts == 0 and num_explorers == 0:
        msg_references = "no references"
    else:
        references = [
            "1 post" if num_posts == 1 else ("" if num_posts == 0 else f"{num_posts} posts"),
            "1 explorer" if num_explorers == 1 else ("" if num_explorers == 0 else f"{num_explorers} explorers"),
        ]
        references = " and ".join(filter(None, references))
        msg_references = f"referenced in {references}" if references else "no references"
    return f"{msg_chart_views}; {msg_references}"


def _send_published_extra_messages(chart, refs, **kwargs):
    """Send extra context in thread for published chart."""
    # 0/ Get chart description + Suggestion
    log.info("Getting AI summary...")
    ai_summary = get_chart_summary(chart=chart)

    # 1/ Chart summary
    if ai_summary and ai_summary["description"]:
        log.info("Sending chart summary...")
        msg_description = f"🧾 *Chart description* ({MODEL_DEFAULT_PRETTY})\n{ai_summary['description']}"
        send_slack_message(message=msg_description, **kwargs)

    # 2/ Suggestion
    if ai_summary and ai_summary["suggestion"]:
        log.info("Sending suggestion...")
        msg_suggestion = _build_suggestion_message(ai_summary["suggestion"])
        send_slack_message(message=msg_suggestion, **kwargs)

    # 3/ References details
    msg_refs = _build_refs_message(refs)
    if msg_refs:
        log.info("Sending reference message...")
        send_slack_message(message=msg_refs, **kwargs)

    # 4/ Related charts
    # msg_related = f"🔍 *<{OWID_ENV.wizard_url}related_charts?slug={chart['slug']}|Explore related charts>*"
    # log.info("Sending 'similar charts' link...")
    # send_slack_message(message=msg_related, **kwargs)

    # 5 Revision history
    msg_revisions = _build_revisions_message(chart.revisions)
    send_slack_message(message=msg_revisions, **kwargs)

    # 6/ Tag explanation
    tag_message = "❓ *Why have I been tagged?* You are tagged as the data steward for published charts."
    log.info("Sending tag explanation...")
    send_slack_message(message=tag_message, **kwargs)


####################################
# Draft chart review pipeline
####################################
def _send_draft_chart_review(
    chart,
    channel_name: str,
    slack_username: str,
    icon_emoji: str,
    slack_users: dict[str, str],
):
    """Send a draft chart review to Slack.

    Args:
        chart: Chart row (pandas Series) to review.
        channel_name: Name of the Slack channel.
        slack_username: Username to use when sending the message.
        icon_emoji: Emoji to use as icon.
    """
    log.info(f"Selected draft chart: {chart['chart_id']}, {chart['slug']}")

    # Find responsible user (creator, editor, or default)
    slack_tag, reason, _ = _find_responsible_user(chart, slack_users)

    # Build message with responsible user tag
    message = build_draft_message(chart, slack_tag=slack_tag)

    # Send message
    if SLACK_API_TOKEN:
        log.info("Sending draft review message...")
        response = send_slack_message(
            message=message,
            channel=channel_name,
            icon_emoji=icon_emoji,
            username=slack_username,
        )

        # Send tag explanation in thread
        tag_message = f"❓ *Why have I been tagged?*\n{reason}"
        log.info("Sending tag explanation...")
        send_slack_message(
            message=tag_message,
            channel=channel_name,
            icon_emoji=icon_emoji,
            username=slack_username,
            thread_ts=response["ts"],
        )

        # Add to reviewed
        owidb_submit_review_id(object_type="chart", object_id=chart["chart_id"])


def build_draft_message(chart, slack_tag: str | None = None):
    """Build simple message for draft chart review.

    Args:
        chart: Chart row (pandas Series) to review.
        slack_tag: Slack mention format for the creator (e.g., "<@username>") or None.
    """
    last_edited = chart["last_edited_at"]
    today_str = TODAY.strftime("%d %b, %Y")

    # Get chart title
    api = AdminAPI(OWID_ENV)
    config = api.get_chart_config(chart["chart_id"])

    # Calculate time ago
    days_ago = (TODAY - last_edited.date()).days
    if days_ago > 365:
        years = days_ago // 365
        time_ago = f"{years} year{'s' if years > 1 else ''} ago"
    elif days_ago > 30:
        months = days_ago // 30
        time_ago = f"{months} month{'s' if months > 1 else ''} ago"
    else:
        time_ago = f"{days_ago} day{'s' if days_ago != 1 else ''} ago"

    date_str = last_edited.strftime("%d %b %Y")

    # Format the tag part of the message
    tag_part = f" <@{slack_tag}>" if slack_tag else ""

    message = (
        f"[{today_str}] *Delete <{OWID_ENV.chart_admin_site(chart['chart_id'])}|this draft> if it's no longer needed*{tag_part}\n"
        f"title: {config.get('title', '')}\n"
        f"_last edited: {date_str} ({time_ago}), by {chart['last_edited_by']}_"
    )
    return message


####################################
# Helpers
####################################
def get_references(chart_id: int):
    """Get references to a chart (explorers, posts)."""
    api = AdminAPI(OWID_ENV)
    refs = api.get_chart_references(chart_id)
    return refs["references"]


def get_usernames():
    df = read_metabase(
        "select * from users",
        database_id=5,
    )

    if "slackUsername" not in df.columns:
        SLACK_NAMES = {
            "Angela Wenham": "Angela",
            "Antoinette Finnegan": "Antoinette",
            "Bastian Herre": "Bastian",
            "Bertha Rohenkohl": "Bertha",
            "Bobbie Macdonald": "bobbie",
            "Charlie Giattino": "charlie",
            "Daniel Bachler": "daniel",
            "Edouard Mathieu": "Ed",
            "Esteban Ortiz-Ospina": "Este",
            "Fiona Spooner": "Fiona",
            "Hannah Ritchie": "hannah",
            "Ike Saunders": "ike",
            "Joe Hasell": "joe",
            "Lucas Rodés-Guirao": "lucas",
            "Marcel Gerber": "marcel",
            "Martin Račák": "Martin",
            "Marwa Boukarim": "Marwa",
            "Matthieu Bergel": "matthieu",
            "Max Roser": "max",
            "Mojmir Vinkler": "Mojmir",
            "Natalie Reynolds-Garcia": "Nat",
            "Pablo Arriagada": "Pablo A",
            "Pablo Rosado": "Pablo R",
            "Sophia Mersmann": "sophia",
            "Tuna Acisu": "Tuna",
            "Valerie Muigai": "Valerie",
            "Veronika Samborska": "Veronika",
        }
        df["slackUsername"] = df["fullName"].map(SLACK_NAMES)

    df = df[["fullName", "slackUsername"]].dropna()
    dix = df.set_index("fullName")["slackUsername"].to_dict()
    return dix


def _parse_revisions(revisions) -> list[dict]:
    """Parse revisions field - handles both JSON string and list of dicts.

    Can be removed if Metabase returns list directly.
    """
    import json

    if revisions is None:
        return []
    if isinstance(revisions, str):
        try:
            return json.loads(revisions)
        except json.JSONDecodeError:
            return []
    return revisions if isinstance(revisions, list) else []


def _format_date(date_value) -> str:
    """Format a date value to string."""
    if date_value is None:
        return ""
    if hasattr(date_value, "strftime"):
        return date_value.strftime("%d %b %Y")
    return str(date_value)


def _find_responsible_user(chart, users) -> tuple[str, str, str | None]:
    """Find the responsible user for a draft chart.

    Logic:
    1. Check created_by - if in SLACK_NAMES, use them
    2. Check revisions chronologically - find first editor in SLACK_NAMES
    3. Fall back to DAILY_DRAFT_CHART_REVIEWER_DEFAULT

    Args:
        chart: Chart row (pandas Series) with created_by and revisions fields.
        users: Dictionary with available users on Slack

    Returns:
        Tuple of (slack_tag, reason, date_str) where:
        - slack_tag: Username to tag on Slack
        - reason: Why this person was tagged (for thread message)
        - date_str: Relevant date (creation or edit date), or None for default
    """
    # 1. Check creator
    creator = chart.get("created_by")
    if creator and creator in users:
        slack_tag = users[creator]
        date_str = _format_date(chart.get("created_at"))
        return slack_tag, f"You created this chart on {date_str}.", date_str

    # 2. Check revisions chronologically
    revisions = _parse_revisions(chart.get("revisions"))
    # Sort by edited_at (oldest first)
    sorted_revisions = sorted(revisions, key=lambda r: r.get("edited_at", ""))
    for rev in sorted_revisions:
        editor = rev.get("edited_by")
        if editor and editor in users:
            slack_tag = users[editor]
            date_str = _format_date(rev.get("edited_at"))
            return slack_tag, f"You edited this chart on {date_str}.", date_str

    # 3. Fall back to default
    slack_tag = DAILY_DRAFT_CHART_REVIEWER_DEFAULT
    return slack_tag, "You are the default reviewer for draft charts.", None


def _build_refs_message(refs):
    """Build message with chart references as bullet lists."""
    sections = []

    explorers = refs.get("explorers", [])
    posts_gdoc = refs.get("postsGdocs", [])
    posts_wp = refs.get("postsWordpress", [])

    # Explorers
    if explorers:
        num_explorers = len(explorers)
        explorer_items = [f"  • <{OWID_ENV.explorer_site(e)}|{e}>" for e in explorers]
        header = f"• *Explorers ({num_explorers}):*"
        sections.append(header + "\n" + "\n".join(explorer_items))

    # Posts
    if posts_gdoc or posts_wp:
        num_posts = len(posts_gdoc) + len(posts_wp)
        post_items = []
        for p in posts_gdoc:
            post_items.append(f"  • <{p['url']}|{p['slug']}>")
        for p in posts_wp:
            post_items.append(f"  • <{p['url']}|{p['slug']}> (wordpress)")
        header = f"• *Posts ({num_posts}):*"
        sections.append(header + "\n" + "\n".join(post_items))

    if not sections:
        return None

    return "💬 *References*:\n" + "\n".join(sections)


def _build_revisions_message(edits, include_time_range=False):
    """
    Slack-friendly summary:
      YYYY-MM-DD  Username (X revisions)
    If include_time_range=True, appends: [HH:MM–HH:MM] (or [HH:MM] if single).
    """
    rows = []
    for e in edits:
        dt = datetime.strptime(e["edited_at"], "%Y-%m-%d %H:%M:%S")
        rows.append((dt.date().isoformat(), e["edited_by"], dt))

    # group counts (and times)
    grouped = defaultdict(list)  # (date, user) -> [dt...]
    for d, user, dt in rows:
        grouped[(d, user)].append(dt)

    # sort keys by date then user
    keys = sorted(grouped.keys(), key=lambda k: (k[0], k[1]))

    lines = []
    for d, user in keys:
        dts = sorted(grouped[(d, user)])
        n = len(dts)

        line = f"{d}  {user} ({n} revision{'s' if n != 1 else ''})"

        if include_time_range:
            start = dts[0].strftime("%H:%M")
            end = dts[-1].strftime("%H:%M")
            line += f" [{start}]" if start == end else f" [{start}–{end}]"

        lines.append(line)

    revisions_block = "\n".join(lines)
    text = f"🕒 *Chart revisions*\n```\n{revisions_block}\n```"

    return text


def _build_suggestion_message(suggestion):
    """Build Slack-friendly suggestion message from AI summary."""
    msg_suggestion = f"💡 *Suggestion* ({MODEL_DEFAULT_PRETTY}): {suggestion.action}\n"
    for reason in suggestion.reasons:
        msg_suggestion += f"• {reason}\n"
    return msg_suggestion
