"""WIP: Currently integrating Draft charts into pipeline

NEXT STEPS:
    - Draft charts sent daily, with minimal text.
    - Charts suggested more than a year ago might be re-suggested. When this happens, we should link to the previous slack revision. For this use `etl.slack_helpers.get_messages` and suggestedAt field in HousekeeperReview.
"""

from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from apps.housekeeper.utils import (
    TODAY,
    get_chart_summary,
    owidb_get_reviews_id,
    owidb_submit_review_id,
)
from etl.analytics.metabase import get_question_data
from etl.config import OWID_ENV, SLACK_API_TOKEN
from etl.slack_helpers import send_slack_message

log = get_logger()


####################################
# Main entry point
####################################
def send_slack_chart_reviews(
    channel_name: str,
    slack_username: str,
    icon_emoji: str,
    include_published: bool = True,
    include_draft: bool = True,
):
    """Send daily chart reviews to Slack (both published and draft).

    This is the main entry point that fetches data once and runs both pipelines.

    Args:
        channel_name: Name of the Slack channel to send the message to.
        slack_username: Username to use when sending the message.
        icon_emoji: Emoji to use as icon when sending the message.
        include_published: Whether to send a published chart review.
        include_draft: Whether to send a draft chart review.
    """
    log.info("Getting all charts to review")
    df_published, df_draft = get_all_charts_to_review()

    slack_kwargs = {
        "channel_name": channel_name,
        "slack_username": slack_username,
        "icon_emoji": icon_emoji,
    }

    if include_published and not df_published.empty:
        _send_published_chart_review(df_published.iloc[0], **slack_kwargs)
    elif include_published:
        log.info("No published charts to review")

    if include_draft and not df_draft.empty:
        _send_draft_chart_review(df_draft.iloc[0], **slack_kwargs)
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
def _send_published_chart_review(chart, channel_name: str, slack_username: str, icon_emoji: str):
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
        f"{date_str} <{OWID_ENV.chart_site(chart['slug'])}|daily chart>\n"
        f"{message_usage}\n"
        f"Go to <{OWID_ENV.chart_admin_site(chart['chart_id'])}|edit :writing_hand:>\n"
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
    return f"({msg_chart_views}; {msg_references})"


def _send_published_extra_messages(chart, refs, **kwargs):
    """Send extra context in thread for published chart."""
    # 1/ Similar charts
    similar_message = f"🕵️ <{OWID_ENV.wizard_url}related_charts?slug={chart['slug']}| → Explore related charts>"
    log.info("Sending 'similar charts' link...")
    send_slack_message(message=similar_message, **kwargs)

    # 2/ References details
    refs_message = _build_refs_message(refs)
    if refs_message:
        log.info("Sending reference message...")
        send_slack_message(message=refs_message, **kwargs)

    # 3/ AI Summary
    log.info("Getting AI summary...")
    ai_summary = get_chart_summary(chart=chart)
    if ai_summary:
        log.info("Sending AI summary...")
        send_slack_message(message=ai_summary, **kwargs)


####################################
# Draft chart review pipeline
####################################
def _send_draft_chart_review(chart, channel_name: str, slack_username: str, icon_emoji: str):
    """Send a draft chart review to Slack.

    Args:
        chart: Chart row (pandas Series) to review.
        channel_name: Name of the Slack channel.
        slack_username: Username to use when sending the message.
        icon_emoji: Emoji to use as icon.
    """
    log.info(f"Selected draft chart: {chart['chart_id']}, {chart['slug']}")

    # Build simple message
    message = build_draft_message(chart)

    # Send message
    if SLACK_API_TOKEN:
        log.info("Sending draft review message...")
        send_slack_message(
            message=message,
            channel=channel_name,
            icon_emoji=icon_emoji,
            username=slack_username,
        )

        # Add to reviewed
        owidb_submit_review_id(object_type="chart", object_id=chart["chart_id"])


def build_draft_message(chart):
    """Build simple message for draft chart review."""
    last_edited = chart["last_edited_at"]

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

    message = (
        f"📝 *Daily draft review*\n"
        f"Last edited: *{date_str}* ({time_ago})\n"
        f"<{OWID_ENV.chart_admin_site(chart['chart_id'])}|→ View in admin>"
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


def _build_refs_message(refs):
    """Build message with chart references."""
    message = []

    explorers = refs.get("explorers", [])
    posts_gdoc = refs.get("postsGdocs", [])
    posts_wp = refs.get("postsWordpress", [])

    # Explorers
    if explorers:
        num_explorers = len(explorers)
        explorer_links = [f"<{OWID_ENV.explorer_site(e)}|{e}>" for e in explorers]
        if num_explorers == 1:
            message.append(f"*→ 1 explorer:* {', '.join(explorer_links)}\n")
        else:
            message.append(f"*→ {num_explorers} explorers:* {', '.join(explorer_links)}\n")

    # Posts
    if posts_gdoc or posts_wp:
        num_posts = len(posts_gdoc) + len(posts_wp)
        message_posts = []
        if posts_gdoc:
            _msg = ", ".join([f"<{p['url']}|{p['slug']}>" for p in posts_gdoc])
            message_posts.append(_msg)
        if posts_wp:
            _msg = ", ".join([f"<{p['url']}|{p['slug']}> (wordpress)" for p in posts_wp])
            message_posts.append(_msg)

        if num_posts == 1:
            message.append("*→ 1 Post:* " + ", ".join(message_posts))
        else:
            message.append(f"*→ {num_posts} Posts:* " + ", ".join(message_posts))

    if not message:
        return None

    return "💬 *References*:\n" + "\n".join(message)
