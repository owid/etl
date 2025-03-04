import pandas as pd
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from apps.housekeeper.utils import (
    TODAY,
    YEAR_AGO,
    add_reviews,
    get_chart_summary,
    get_charts_with_slug_rename_last_year,
    get_reviews_id,
)
from apps.wizard.app_pages.similar_charts.data import get_raw_charts
from etl.config import OWID_ENV, SLACK_API_TOKEN
from etl.slack_helpers import send_slack_message

log = get_logger()


def send_slack_chart_review(channel_name: str, slack_username: str, icon_emoji: str):
    # Get charts
    log.info("Getting charts to review")
    df = get_charts_to_review()

    # Sort charts
    log.info("Sorting charts...")
    df = sort_charts(df)

    # Select chart
    log.info("Select chart...")
    chart = select_chart(df)
    # DEBUGGING:
    # 2582 (wordpress link), 1609 (no references), 5689 (explorer, no post), 4288 (explorer, wp post), 2093 (no explorer, post), 3475 (explorer, post), (explorer, post + wp)
    # chart = df[df.chart_id == 3475].iloc[0]
    # chart = df.iloc[0]
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
        add_reviews(object_type="chart", object_id=chart["chart_id"])


def get_charts_to_review():
    def _get_chart_references():
        """Get references to charts (complete).

        This includes references in explorers and/or articles or any other post.
        """
        query_posts = """SELECT
                a.target chart_slug,
                a.componentType link_method,
                b.slug post_slug,
                b.TYPE post_type,
                CASE
                    WHEN type = 'data-insight' THEN CONCAT('https://ourworldindata.org/data-insights/', b.slug)
                    WHEN type = 'team' THEN CONCAT('https://ourworldindata.org/team/', b.slug)
                    ELSE CONCAT('https://ourworldindata.org/', b.slug)
                END post_url,
                b.published post_published
            FROM posts_gdocs_links a
            JOIN posts_gdocs b ON a.sourceId = b.id
            WHERE linkType = "grapher";
            """
        df_exp, df_links = OWID_ENV.read_sqls(
            [
                "SELECT * FROM explorer_charts",
                query_posts,
            ]
        )

        return df_exp, df_links

    def _add_explorer_references(df, df_exp) -> pd.DataFrame:
        """Add explorer reference details to main dataframe."""
        # Group by chart, get number of explorers and explorer slugs
        df_exp = (
            df_exp.groupby("chartId", as_index=False)["explorerSlug"]
            .agg({"num_explorers": "size", "explorer_slugs": "unique"})
            .rename(columns={"chartId": "chart_id"})
        )

        # Merge with main dataframe
        df = df.merge(df_exp, how="left", on="chart_id")

        # Set to NaNs to zero (this is a count indicator)
        df["num_explorers"] = df["num_explorers"].fillna(0)

        return df

    def _add_post_references(df, df_posts) -> pd.DataFrame:
        """Add post reference details to main dataframe."""
        # Prepare post details for group by operation
        df_posts["post_details"] = df_posts.set_index("chart_slug")[
            ["post_type", "post_url", "post_published"]
        ].to_dict(orient="records")

        # Group by chart_slug, get number of posts and post details
        df_posts = (
            df_posts.groupby("chart_slug", as_index=False)["post_details"]
            .agg({"num_posts": "size", "post_details": list})
            .rename(columns={"chart_slug": "slug"})
        )

        # Merge with main dataframe
        df = df.merge(df_posts, on="slug", how="left")

        # Set to NaNs to zero (this is a count indicator)
        df["num_posts"] = df["num_posts"].fillna(0)
        return df

    # Get all charts
    df = get_raw_charts()

    # Keep only older-than-a-year charts
    df = df.loc[df["created_at"] < YEAR_AGO]

    # The following code gets all references from explorers and posts for all charts. This is currently commented because we use AdminAPI instead to *just* get this information for the selected daily chart.
    # If this details were needed to sort the charts, please use this instead.
    # Add references (explorers and articles)
    df_exp, df_links = _get_chart_references()
    df = _add_explorer_references(df, df_exp)
    df = _add_post_references(df, df_links)

    # Ignore some charts (reviewed, recently slug-renamed)
    ## Discard charts already presented in the chat
    reviews_id = get_reviews_id(object_type="chart")
    ## Keep only charts whose slug hasn't been changed in the last year
    rename_id = get_charts_with_slug_rename_last_year()
    ## Combine & ignore
    df = df.loc[~df["chart_id"].isin(reviews_id + rename_id)]

    return df


def sort_charts(df: pd.DataFrame):
    # Sort by views
    df = df.sort_values(["views_365d", "views_14d", "views_7d"])

    return df


def select_chart(df: pd.DataFrame):
    # Select oldest chart
    chart = df.iloc[0]

    return chart


def get_references(chart_id: int):
    api = AdminAPI(OWID_ENV)
    refs = api.get_chart_references(chart_id)
    refs = refs["references"]

    return refs


def build_main_message(chart, refs):
    message_usage = _get_main_message_usage(chart, refs)
    DATE = TODAY.date().strftime("%d %b, %Y")
    message = (
        f"{DATE}: *Daily chart:* "
        f"<{OWID_ENV.chart_site(chart['slug'])}|{chart['title']}>\n"
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
    similar_messages = (
        f"🕵️ <{OWID_ENV.wizard_url}similar_charts?chart_search_text={chart['slug']}| → Explore similar charts>"
    )

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
