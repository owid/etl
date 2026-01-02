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


def send_slack_chart_review(channel_name: str, slack_username: str, icon_emoji: str):
    """Get daily chart to be reviewed and send to Slack channel.

    Args:
        channel_name (str): Name of the Slack channel to send the message to.
        slack_username (str): Username to use when sending the message.
        icon_emoji (str): Emoji to use as icon when sending the message.
    """
    # Get charts
    log.info("Getting charts to review")
    df = get_charts_to_review()

    # Select chart
    log.info("Select chart...")
    chart = df.iloc[0]

    log.info(f"Selected chart: {chart['chart_id']}, {chart['slug']}")

    # Get references
    refs = get_references(chart["chart_id"])

    # Prepare message
    log.info("Preparing main message...")
    message = build_main_message(chart, refs)

    # Send message
    if SLACK_API_TOKEN:
        # 1/ Main message
        log.info("Sending main message, with image...")
        image_url = OWID_ENV.thumb_url(chart["slug"])
        response = send_slack_message(
            message=message,
            channel=channel_name,
            image_url=image_url,
            icon_emoji=icon_emoji,
            username=slack_username,
        )

        # 2/ More context in the thread
        kwargs = {
            "channel": channel_name,
            "icon_emoji": icon_emoji,
            "username": slack_username,
            "thread_ts": response["ts"],
        }
        send_extra_messages(chart, refs, **kwargs)

        # 3/ Add chart to reviewed charts
        owidb_submit_review_id(object_type="chart", object_id=chart["chart_id"])


def get_charts_to_review():
    """Get charts that need to be reviewed by Housekeeper.

    Metabase question 812 contains the data. We get it, keep only those unreviewed + published.
    """
    # Get charts to review from Metabase, question 812
    df = get_question_data(
        812,
        prod=True,
    )
    # Skip those that have been already reviewed
    reviews_id = owidb_get_reviews_id(object_type="chart")
    df = df.loc[~df["chart_id"].isin(reviews_id)]

    # Only return published charts
    df = df.loc[df["is_published"]]

    return df


def get_references(chart_id: int):
    """Get references to a chart (explorers, posts).

    For debugging purposes:
    2582 (wordpress link), 1609 (no references), 5689 (explorer, no post), 4288 (explorer, wp post), 2093 (no explorer, post), 3475 (explorer, post), (explorer, post + wp)
    """
    api = AdminAPI(OWID_ENV)
    refs = api.get_chart_references(chart_id)
    refs = refs["references"]

    return refs


def build_main_message(chart, refs):
    """Prepare message to be sent on Slack."""
    message_usage = _get_main_message_usage(chart, refs)
    DATE = TODAY.strftime("%d %b, %Y")
    message = (
        f"{DATE} <{OWID_ENV.chart_site(chart['slug'])}|daily chart>\n"
        f"{message_usage}\n"
        f"Go to <{OWID_ENV.chart_admin_site(chart['chart_id'])}|edit :writing_hand:>\n"
    )

    return message


def _get_main_message_usage(chart, refs):
    """Get brief message about chart usage.

    This includes chart views, and references to chart (from explorers and posts).
    """
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
    message_usage = f"({msg_chart_views}; {msg_references})"

    return message_usage


def send_extra_messages(chart, refs, **kwargs):
    """Provide more context in the thread"""
    ## 1/ Similar charts
    similar_messages = f"🕵️ <{OWID_ENV.wizard_url}related_charts?slug={chart['slug']}| → Explore related charts>"

    ## 2/ AI: Chart description, chart edit timeline, suggestion
    log.info("Getting AI summary...")
    ai_summary = get_chart_summary(chart=chart)

    ## 3/ Send extra info
    ### Similar charts
    log.info("Sending 'similar charts' link...")
    send_slack_message(
        message=similar_messages,
        **kwargs,
    )
    ### References details
    log.info("Sending reference message...")
    refs_message = make_refs_message(refs)
    if refs_message:
        send_slack_message(
            message=refs_message,
            **kwargs,
        )
    ### AI Summary
    log.info("Sending AI summary...")
    if ai_summary:
        send_slack_message(
            message=ai_summary,
            **kwargs,
        )


def make_refs_message(refs):
    """Prepare message with references."""
    message = []

    explorers = refs.get("explorers", [])
    posts_gdoc = refs.get("postsGdocs", [])
    posts_wp = refs.get("postsWordpress", [])

    # Explorers
    if explorers != []:
        num_explorers = len(explorers)
        explorer_links = [f"<{OWID_ENV.explorer_site(e)}|{e}>" for e in explorers]
        if num_explorers == 1:
            message.append(f"*→ 1 explorer:* {', '.join(explorer_links)}\n")
        else:
            message.append(f"*→ {num_explorers} explorers:* {', '.join(explorer_links)}\n")

    # Posts
    if (posts_gdoc != []) or (posts_wp != []):
        num_posts = len(posts_gdoc) + len(posts_wp)
        message_posts = []
        if posts_gdoc != []:
            _msg = ", ".join([f"<{p['url']}|{p['slug']}>" for p in posts_gdoc])
            message_posts.append(_msg)

        if posts_wp != []:
            _msg = ", ".join([f"<{p['url']}|{p['slug']}> (wordpress)" for p in posts_wp])
            message_posts.append(_msg)

        if num_posts == 1:
            message.append("*→ 1 Post:* " + ", ".join(message_posts))
        else:
            message.append(f"*→ {num_posts} Posts:* " + ", ".join(message_posts))

    # Build complete message
    if message == []:
        return None

    refs_message = "💬 *References*:\n" + "\n".join(message)

    return refs_message
