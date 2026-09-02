"""Script to generate an analytics report for a data producer."""

import re
from datetime import datetime

import click
import pandas as pd
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.analytics.config import GRAPHERS_BASE_URL
from etl.analytics.data import (
    get_chart_views_by_chart_id,
    get_mdim_explorer_views_by_producer,
    get_post_views_by_chart_id,
    get_post_views_of_producer_collections,
    get_post_views_of_redirected_charts_by_producer,
    get_visualizations_using_data_by_producer,
)
from etl.config import (
    DATA_PRODUCER_REPORT_FOLDER_ID,
    DATA_PRODUCER_REPORT_STATUS_SHEET_ID,
    DATA_PRODUCER_REPORT_TEMPLATE_DOC_ID,
    OWID_ENV,
)
from etl.data_helpers.misc import humanize_number
from etl.db import get_engine
from etl.google import GoogleDoc, GoogleDrive, GoogleSheet
from etl.http import session as http_session
from etl.notion import get_data_producer_contacts, get_impact_highlights

# Initialize logger.
log = get_logger()

# Initialize database engine.
engine = get_engine()

# Common definitions of periods.
PERIODS = {
    "Q1": {"name": "first quarter", "min_date": "01-01", "max_date": "03-31"},
    "Q2": {"name": "second quarter", "min_date": "04-01", "max_date": "06-30"},
    "Q3": {"name": "third quarter", "min_date": "07-01", "max_date": "09-30"},
    "Q4": {"name": "fourth quarter", "min_date": "10-01", "max_date": "12-31"},
    "H1": {"name": "first half", "min_date": "01-01", "max_date": "06-30"},
    "H2": {"name": "second half", "min_date": "07-01", "max_date": "12-31"},
    "Y": {"name": "year", "min_date": "01-01", "max_date": "12-31"},
}


def get_chart_title_from_url(chart_url: str) -> str:
    response = http_session.get(f"{chart_url}.metadata.json")
    title = response.json()["chart"]["title"]
    return title


def run_sanity_checks(df_charts: pd.DataFrame, df_posts: pd.DataFrame, df_additional_charts: pd.DataFrame) -> None:
    error = "Expected no duplicates in df_producer. If there are, drop duplicates (and check if that's expected)."
    assert df_charts[df_charts.duplicated(subset=["chart_id"])].empty, error

    error = "Unexpected post type."
    assert set(df_posts["post_type"]) <= set(["article", "topic-page", "linear-topic-page", "data-insight"]), error

    error = "Expected no duplicates in df_posts. If there are, drop duplicates (and check if that's expected)."
    assert df_posts[df_posts.duplicated(subset=["url"])].empty, error

    # df_additional_charts is one row per mdim VIEW (unique by view_config_id) and one row per explorer
    # (unique by url). Check each kind on its own identity.
    error = "Expected no duplicate mdim views in df_additional_charts (unique by view_config_id)."
    df_mdim = df_additional_charts[df_additional_charts["type"] == "multidim"]
    assert df_mdim[df_mdim.duplicated(subset=["view_config_id"])].empty, error

    error = "Expected no duplicate explorers in df_additional_charts (unique by url)."
    df_explorer = df_additional_charts[df_additional_charts["type"] == "explorer"]
    assert df_explorer[df_explorer.duplicated(subset=["url"])].empty, error

    # View counts must be well-formed numbers: a NaN means a merge silently dropped a match, a negative
    # means a subtraction/merge bug. Check every dataframe that carries a `views` column.
    for name, df in [("charts", df_charts), ("posts", df_posts), ("additional_charts", df_additional_charts)]:
        assert not df["views"].isna().any(), f"NaN views in df_{name}."
        assert (df["views"] >= 0).all(), f"Negative views in df_{name}."

    # Daily-average columns (only charts and additional_charts carry them) must also be clean non-negatives.
    for name, df in [("charts", df_charts), ("additional_charts", df_additional_charts)]:
        assert not df["views_daily"].isna().any(), f"NaN views_daily in df_{name}."
        assert (df["views_daily"] >= 0).all(), f"Negative views_daily in df_{name}."

    # Additional-charts arithmetic: the reported total is exactly own + redirected, and the
    # includes_redirect_views flag matches whether any redirected views were folded in.
    ac = df_additional_charts
    assert (ac["own_views"] >= 0).all() and (ac["redirected_views"] >= 0).all(), (
        "Negative component views in df_additional_charts."
    )
    assert (ac["views"] == ac["own_views"] + ac["redirected_views"]).all(), (
        "df_additional_charts: views != own_views + redirected_views."
    )
    assert (ac["includes_redirect_views"] == (ac["redirected_views"] > 0)).all(), (
        "df_additional_charts: includes_redirect_views does not match redirected_views > 0."
    )

    # View distributions are heavily skewed, not flat: the top 10 charts should draw MORE than their
    # uniform share (10/N of total views). If they don't, the counts look suspiciously uniform (e.g. a
    # broken join assigning every chart the same number). Only meaningful with clearly more than 10 charts.
    n_charts = len(df_charts)
    if n_charts > 10:
        total_views = df_charts["views"].sum()
        top10_views = df_charts["views"].nlargest(10).sum()
        uniform_share = total_views * (10 / n_charts)
        assert top10_views > uniform_share, (
            f"Top 10 charts hold {top10_views:,.0f} views, not more than the uniform-share expectation of "
            f"{uniform_share:,.0f} (10/{n_charts} of {total_views:,.0f}). Chart view counts look suspiciously flat."
        )

    # All checks passed (a failure above raises AssertionError). Log a positive confirmation so a run
    # shows the checks executed and on how much data.
    log.info(
        "run_sanity_checks.passed",
        charts=len(df_charts),
        posts=len(df_posts),
        additional_charts=len(df_additional_charts),
    )


def _format_collection_title(title: str, type_: str) -> str:
    """Display title for a collection row. Mdim rows already carry the actual grapher chart title of the
    view (set in get_mdim_explorer_views_by_producer), so they're shown as-is; explorers get a
    'Data Explorer' suffix (their title alone, e.g. 'Energy', doesn't reveal it links to an explorer
    rather than a regular chart)."""
    return f"{title} Data Explorer" if type_ == "explorer" else title


def gather_producer_analytics(producers: list[str], min_date: str, max_date: str) -> dict[str, pd.DataFrame]:
    # Get charts using data from the data producer(s).
    df_producer_charts = get_visualizations_using_data_by_producer(producers=producers)

    assert not df_producer_charts.empty, f"No charts found for producer(s): {', '.join(producers)}"

    # Remove duplicate rows.
    # NOTE: This happens, for example, when a chart uses multiple snapshots of the same producer (so they are different origins for the same producer), e.g. chart 488 has two origins with producer "Global Carbon Project".
    df_producer_charts = df_producer_charts.drop_duplicates(subset=["chart_id"]).reset_index(drop=True)

    # List IDs of charts using data from the current data producer.
    producer_chart_ids = sorted(set(df_producer_charts["chart_id"]))

    # Get views for those charts.
    df_charts = get_chart_views_by_chart_id(chart_ids=producer_chart_ids, date_min=min_date, date_max=max_date)

    # Include chart titles.
    df_charts = df_charts.merge(df_producer_charts[["chart_id", "chart_title"]], how="left", on="chart_id").rename(
        columns={"chart_title": "title"}
    )

    # Include a column to signal if a chart was featured in the homepage.
    df_charts["featured_on_homepage"] = False

    # Get posts showing charts using data from the current data producer.
    # NOTE: Include DIs as part of posts (for the total view count).
    df_posts = get_post_views_by_chart_id(chart_ids=producer_chart_ids, date_min=min_date, date_max=max_date)

    # This dataframe may contain the homepage among the list of posts.
    homepage_mask = df_posts["post_type"] == "homepage"
    # Remove the homepage from the list of posts, but add a column in the charts dataframe, to signal that the chart was featured in the homepage.
    if homepage_mask.any():
        df_charts.loc[
            df_charts["chart_id"].isin(sorted(set(df_posts[homepage_mask]["chart_id"]))), "featured_on_homepage"
        ] = True
        df_posts = df_posts.drop(homepage_mask[homepage_mask].index).reset_index(drop=True)  # ty: ignore
    # Keep only the information about posts.
    df_posts = (
        df_posts.drop_duplicates(subset=["post_url"])
        .rename(columns={"post_title": "title", "post_url": "url"})
        .drop(columns=["chart_url", "chart_id"])
        .reset_index(drop=True)
    )

    # Recover posts that cite a chart/explorer slug now redirected into one of the producer's mdim views.
    # These link to a dead slug, so the live-chart lookup above misses them; they use the same
    # component-type rule. Concatenate and drop duplicate posts (a post already matched via a live chart).
    df_posts_redirected = get_post_views_of_redirected_charts_by_producer(
        producers=producers, date_min=min_date, date_max=max_date
    )
    # Posts that embed one of the producer's mdims or explorers (linked by the live slug): also missed
    # by the chart-id lookup (collections have no chart id), e.g. after a chart→mdim migration
    # re-points the post.
    df_posts_collections = get_post_views_of_producer_collections(
        producers=producers, date_min=min_date, date_max=max_date
    )
    recovered = [df for df in [df_posts_redirected, df_posts_collections] if not df.empty]
    if recovered:
        df_posts = (
            pd.concat([df_posts, *recovered], ignore_index=True).drop_duplicates(subset=["url"]).reset_index(drop=True)
        )

    # Get views of the producer's mdim VIEWS (one row per view) and explorers. Each mdim view's total
    # already folds in views absorbed from charts/explorers redirected into that specific view, and only
    # views that use the producer's data are included (no whole-surface over-crediting).
    df_additional_charts = get_mdim_explorer_views_by_producer(
        producers=producers, date_min=min_date, date_max=max_date
    )

    # Build the display title (mdim view -> the view's grapher chart title, as-is; explorer -> "... Data
    # Explorer").
    df_additional_charts["title"] = [
        _format_collection_title(title=title, type_=type_)
        for title, type_ in zip(df_additional_charts["title"], df_additional_charts["type"])
    ]
    df_additional_charts["featured_on_homepage"] = False

    # Sanity checks (need `type`/`view_config_id`, so run before splitting).
    run_sanity_checks(df_charts=df_charts, df_posts=df_posts, df_additional_charts=df_additional_charts)

    # Split into the main interactive-chart list (folded into the totals) vs the separate "additional
    # charts" section:
    # - Mdim VIEWS are ALWAYS in the main list. Per-view attribution credits each view to the right
    #   producer, so even a multi-producer mdim's views count in full here without over-crediting.
    # - Explorers are whole-surface (no per-view breakdown). A "pure" explorer (only this producer's data)
    #   goes in the main list too; a "mixed" explorer (also other producers' data) is reported separately,
    #   since its whole-explorer views can't be cleanly attributed to this producer.
    is_mixed_explorer = (df_additional_charts["type"] == "explorer") & df_additional_charts["uses_other_producers_data"]
    df_additional_charts_exclusive = df_additional_charts[~is_mixed_explorer].reset_index(drop=True)
    df_additional_charts_mixed = df_additional_charts[is_mixed_explorer].reset_index(drop=True)

    # Create a dictionary with all analytics.
    analytics = {
        "charts": df_charts,
        "posts": df_posts,
        "additional_charts_exclusive": df_additional_charts_exclusive,
        "additional_charts_mixed": df_additional_charts_mixed,
    }

    return analytics


def insert_list_with_links_in_gdoc(google_doc: GoogleDoc, df: pd.DataFrame, placeholder: str) -> None:
    # For chart lists, get the index of the position where it should be introduced.
    insert_index = google_doc.find_marker_index(marker=placeholder)

    edits = []
    end_index = insert_index
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        title = row["title"]
        url = row["url"]
        views = humanize_number(number=row["views"], sig_figs=3)

        numbered_title = f"{i}. {title}"
        line = f"{numbered_title} – {views} views\n"

        # Add text for charts that have been featured on our homepage.
        if row["featured_on_homepage"]:
            line += "    This chart was also featured on our homepage.\n"

        # Insert line of text.
        edits.append({"insertText": {"location": {"index": end_index}, "text": line}})
        # Apply link to just the title (excluding "1. ").
        title_start = end_index + len(f"{i}. ")
        title_end = title_start + len(title)
        edits.append(
            {
                "updateTextStyle": {
                    "range": {"startIndex": title_start, "endIndex": title_end},
                    "textStyle": {"link": {"url": url}},
                    "fields": "link",
                }
            }
        )
        end_index += len(line)

    # Apply edits to insert list in the right place.
    google_doc.edit(requests=edits)

    # Remove the original placeholder text.
    google_doc.replace_text(mapping={placeholder: ""})


def insert_media_mentions_in_gdoc(google_doc: GoogleDoc, df: pd.DataFrame, placeholder: str) -> None:
    """Insert one bullet per media mention at the placeholder's position.

    Each bullet reads "<date>: <highlight text>. Link: <source link>", with the source link (if any) turned
    into a clickable hyperlink.
    """
    insert_index = google_doc.find_marker_index(marker=placeholder)

    edits = []
    end_index = insert_index
    for _, row in df.iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d")
        url = row.get("Source link")
        has_link = pd.notna(url) and bool(url)

        line = f"• {date_str}: {row['Highlight']}."
        line += f" Link: {url}\n" if has_link else "\n"
        edits.append({"insertText": {"location": {"index": end_index}, "text": line}})

        if has_link:
            link_start = end_index + len(line) - len(url) - 1  # -1 to skip the trailing "\n"
            link_end = link_start + len(url)
            edits.append(
                {
                    "updateTextStyle": {
                        "range": {"startIndex": link_start, "endIndex": link_end},
                        "textStyle": {"link": {"url": url}},
                        "fields": "link",
                    }
                }
            )
        end_index += len(line)

    # Apply edits to insert list in the right place.
    google_doc.edit(requests=edits)

    # Remove the original placeholder text.
    google_doc.replace_text(mapping={placeholder: ""})


class Report:
    """An analytics report for a data producer."""

    def __init__(self, producer: str, period: str, year: int, aliases: list[str] | None = None):
        # The analytics semantic layer is built from production, so grapher-DB reads (redirects,
        # origins_variables, chart configs) must target production too; against a staging snapshot, any
        # redirect or re-versioned dataset newer than the snapshot is silently dropped from the report.
        assert OWID_ENV.env_remote == "production", (
            f"Reports must be generated against the production grapher DB "
            f"(current: {OWID_ENV.conf.DB_HOST}). Run with ENV_FILE=.env.prod."
        )
        self.producer = producer  # Canonical name for display
        self.aliases = aliases or []
        self.all_producer_names = [producer] + self.aliases  # All names for data gathering
        self.period = period
        self.year = year
        # For annual reports (period "Y"), use just the year. For other periods, include the period code.
        period_prefix = f"{year}" if period == "Y" else f"{year}-{period}"
        self.title = f"{period_prefix} Our World in Data analytics report for {producer}"
        self.min_date = f"{year}-{PERIODS[period]['min_date']}"
        self.max_date = f"{year}-{PERIODS[period]['max_date']}"

        # Determine the period subfolder name, e.g. "Y-2025 Data provider reports"
        self.subfolder_name = f"{period}-{year} Data provider reports"

        # Check if this report already exists in Google Drive
        google_drive = GoogleDrive()
        self.subfolder_id = google_drive.get_or_create_subfolder(
            parent_folder_id=DATA_PRODUCER_REPORT_FOLDER_ID,
            folder_name=self.subfolder_name,
        )
        files = google_drive.list_files_in_folder(folder_id=self.subfolder_id)

        self.doc_id: str | None = None
        self.pdf_id: str | None = None

        # Data provider emails, that will be granted reading permissions to access the pdf reports.
        # NOTE: They will be fetched by gather_emails()
        self.emails: list[str] | None = None

        for file in files:
            if file["name"] == self.title:
                if file["mimeType"] == "application/vnd.google-apps.document":
                    self.doc_id = file["id"]
            elif file["name"] == f"{self.title}.pdf":
                if file["mimeType"] in [
                    "application/pdf",
                    "application/x-pdf",
                    "application/acrobat",
                    "application/vnd.pdf",
                ]:
                    self.pdf_id = file["id"]

        # Log what was found during initialization
        if self.doc_id and self.pdf_id:
            log.info(f"Found existing Google Doc and PDF for {self.title}")
        elif self.doc_id:
            log.info(f"Found existing Google Doc (no PDF) for {self.title}")
        elif self.pdf_id:
            log.info(f"Found existing PDF (no Google Doc) for {self.title}")
        else:
            log.info(f"No existing files found for {self.title}")

        # Initialize other attributes (that will be populated later on).
        self.analytics: dict[str, pd.DataFrame] | None = None
        self.highlights: pd.DataFrame | None = None
        self.google_doc: GoogleDoc | None = None
        if self.doc_id:
            self.google_doc = GoogleDoc(doc_id=self.doc_id)

    @property
    def doc_link(self) -> str | None:
        """Get the Google Doc link if doc_id exists."""
        if self.doc_id:
            return f"https://docs.google.com/document/d/{self.doc_id}/edit"
        return None

    @property
    def pdf_link(self) -> str | None:
        """Get the PDF link if pdf_id exists."""
        if self.pdf_id:
            return f"https://drive.google.com/file/d/{self.pdf_id}/view"
        return None

    @property
    def folder_link(self) -> str:
        """Get the folder link where reports are stored."""
        return f"https://drive.google.com/drive/folders/{self.subfolder_id}"

    @property
    def exists(self) -> bool:
        """Check if this report already exists (has a Google Doc)."""
        return self.doc_id is not None

    @property
    def has_pdf(self) -> bool:
        """Check if this report has a PDF."""
        return self.pdf_id is not None

    @property
    def status(self) -> str:
        """Get a human-readable status of the report."""
        if not self.exists:
            return "Not created"
        elif not self.has_pdf:
            return "Google Doc exists, no PDF"
        else:
            return "Both Google Doc and PDF exist"

    def gather_analytics(self, notion_df: pd.DataFrame | None = None) -> None:
        """Gather analytics data for this report.

        Parameters
        ----------
        notion_df : pd.DataFrame, optional
            Pre-fetched Notion impact highlights table, already filtered to (at least) this report's period -
            see get_notion_table_period. Passing it in avoids re-fetching the table from Notion once per
            producer when generating many reports in a batch. If None, it's fetched here, filtered to
            [self.min_date, self.max_date] at that point (the only place highlights are date-filtered).
        """
        if self.aliases:
            log.info(
                f"Gathering analytics for {self.producer} (with aliases: {', '.join(self.aliases)}) {self.period} {self.year}"
            )
        else:
            log.info(f"Gathering analytics for {self.producer} {self.period} {self.year}")

        # Gather analytics for all producer names at once (primary + aliases)
        self.analytics = gather_producer_analytics(
            producers=self.all_producer_names, min_date=self.min_date, max_date=self.max_date
        )

        # Gather impact highlights (e.g. media mentions) for this producer. Only filters by producer here -
        # notion_df is expected to already be date-filtered to this report's period (see docstring above).
        self.highlights = get_impact_highlights(
            producers=self.all_producer_names, min_date=self.min_date, max_date=self.max_date, df=notion_df
        )

    def create_google_doc(self) -> None:
        """Create the Google Doc from template."""
        if not self.analytics:
            raise ValueError("Analytics must be gathered before creating the document")

        # Initialize Google Drive and copy template into the period subfolder.
        google_drive = GoogleDrive()
        self.doc_id = google_drive.copy(
            file_id=DATA_PRODUCER_REPORT_TEMPLATE_DOC_ID,
            body={"name": self.title, "parents": [self.subfolder_id]},
        )
        self.google_doc = GoogleDoc(doc_id=self.doc_id)

        # Populate the document.
        self._populate_document()

    def _populate_document(self) -> None:
        """Internal method to populate the Google Doc with data."""
        if not self.analytics or not self.google_doc:
            raise ValueError("Analytics and Google Doc must be initialized")

        # The main chart list and totals below combine grapher charts, all producer mdim VIEWS, and "pure"
        # explorers (only this producer's data). "Mixed" explorers (also other producers' data) are
        # reported separately (see "Additional charts using your data" section) and excluded from these
        # totals, since their whole-explorer views can't be cleanly attributed to this producer.
        cols = ["url", "views", "title", "featured_on_homepage", "n_days"]
        df_charts_and_additional_exclusive = pd.concat(
            [self.analytics["charts"][cols], self.analytics["additional_charts_exclusive"][cols]], ignore_index=True
        )
        df_additional_charts_mixed = self.analytics["additional_charts_mixed"]

        # Create dataframes for top content.
        list_cols = ["url", "views", "title", "featured_on_homepage"]
        df_top_charts = (
            df_charts_and_additional_exclusive.sort_values("views", ascending=False)[list_cols]
            .reset_index(drop=True)
            .iloc[0:10]
        )
        df_top_posts = (
            self.analytics["posts"]
            .sort_values(["views"], ascending=False)
            .reset_index(drop=True)
            .iloc[0:10]
            .assign(**{"featured_on_homepage": False})
        )
        df_top_additional_charts = (
            df_additional_charts_mixed.sort_values("views", ascending=False)[list_cols].reset_index(drop=True).iloc[0:5]
        )

        # Calculate metrics.
        n_charts = len(df_charts_and_additional_exclusive)
        n_publications = len(self.analytics["posts"])
        n_chart_views = df_charts_and_additional_exclusive["views"].sum()
        n_post_views = self.analytics["posts"]["views"].sum()
        n_daily_chart_views = n_chart_views / df_charts_and_additional_exclusive["n_days"].max()
        # A producer can have no qualifying posts; avoid NaN (n_days.max() over an empty frame).
        n_daily_post_views = n_post_views / self.analytics["posts"]["n_days"].max() if n_publications > 0 else 0
        n_additional = len(df_additional_charts_mixed)
        n_additional_views = df_additional_charts_mixed["views"].sum()

        # Humanize numbers.
        n_charts_humanized = humanize_number(n_charts, sig_figs=3)
        n_posts_humanized = humanize_number(n_publications, sig_figs=3)
        n_chart_views_humanized = humanize_number(n_chart_views, sig_figs=3)
        n_daily_chart_views_humanized = humanize_number(n_daily_chart_views, sig_figs=3)
        n_post_views_humanized = humanize_number(n_post_views, sig_figs=3)
        n_daily_post_views_humanized = humanize_number(n_daily_post_views, sig_figs=3)
        n_additional_humanized = humanize_number(n_additional, sig_figs=3)
        n_additional_views_humanized = humanize_number(n_additional_views, sig_figs=3)
        max_date_humanized = datetime.strptime(f"{self.year}-{PERIODS[self.period]['max_date']}", "%Y-%m-%d").strftime(
            "%B %d, %Y"
        )
        period_humanized = (
            str(self.year) if self.period == "Y" else f"the {PERIODS[self.period]['name']} of {self.year}"
        )

        # Prepare executive summary.
        executive_summary_intro = f"""As of {max_date_humanized}, Our World in Data features your data in"""
        if n_charts == 0:
            raise AssertionError("Expected at least one chart to report.")
        elif n_charts == 1:
            executive_summary_intro += f""" {n_charts_humanized} interactive chart"""
        else:
            executive_summary_intro += f""" {n_charts_humanized} interactive charts"""
        if n_publications == 0:
            raise AssertionError("Expected at least one publication to report.")

        plural_publications = "s" if n_publications > 1 else ""
        executive_summary_intro += f""" and {n_posts_humanized} publication{plural_publications}."""

        # Replace placeholders.
        replacements = {
            r"{{producer}}": self.producer,
            r"{{period_humanized}}": period_humanized,
            r"{{executive_summary_intro}}": executive_summary_intro,
            r"{{n_charts_humanized}}": n_charts_humanized,
            r"{{n_posts_humanized}}": n_posts_humanized,
            r"{{n_post_views_humanized}}": n_post_views_humanized,
            r"{{n_chart_views_humanized}}": n_chart_views_humanized,
            r"{{n_daily_chart_views_humanized}}": n_daily_chart_views_humanized,
            r"{{n_daily_post_views_humanized}}": n_daily_post_views_humanized,
            r"{{n_additional_humanized}}": n_additional_humanized,
            r"{{n_additional_views_humanized}}": n_additional_views_humanized,
        }
        self.google_doc.replace_text(mapping=replacements)

        # Add content.
        # The image shows the most viewed chart overall, so it matches the top entry of the list below.
        # Grapher charts and mdim views both export a ".png" (for an mdim view, inserted before its query
        # string); explorers don't, so they're skipped. The eligible list may be empty (a producer whose
        # only traffic came from explorers): drop the image placeholder in that case.
        df_with_image = df_charts_and_additional_exclusive[
            df_charts_and_additional_exclusive["url"].str.startswith(GRAPHERS_BASE_URL)
        ]
        if df_with_image.empty:
            log.warning("No charts with a static export among top content; skipping the top-chart image.")
            self.google_doc.replace_text(mapping={r"{{top_chart_image}}": ""})
        else:
            top_url = df_with_image.sort_values("views", ascending=False).iloc[0]["url"]
            base, _, query = top_url.partition("?")
            top_chart_url = f"{base}.png?{query}" if query else f"{base}.png"
            self.google_doc.insert_image(image_url=top_chart_url, placeholder=r"{{top_chart_image}}", width=320)
        insert_list_with_links_in_gdoc(self.google_doc, df=df_top_charts, placeholder=r"{{top_charts_list}}")
        insert_list_with_links_in_gdoc(self.google_doc, df=df_top_posts, placeholder=r"{{top_posts_list}}")

        # Additional charts using this producer's data: explorers that also use other producers' data, so
        # their views aren't folded into the totals above. This section requires the Google Doc template to wrap
        # it between "{{additional_charts_section_start}}" and "{{additional_charts_section_end}}" marker lines,
        # and to have a "{{top_additional_charts_list}}" placeholder inside it (plus {{n_additional_humanized}}
        # and {{n_additional_views_humanized}} in its intro text). If there's nothing to show, the whole section
        # (both markers included) is deleted rather than left empty; if the template hasn't been updated with
        # these markers yet, the relevant step is skipped with a warning rather than failing the report.
        if df_additional_charts_mixed.empty:
            try:
                self.google_doc.delete_section(
                    start_marker=r"{{additional_charts_section_start}}",
                    end_marker=r"{{additional_charts_section_end}}",
                )
            except ValueError:
                log.warning(
                    "Markers '{{additional_charts_section_start}}'/'{{additional_charts_section_end}}' not found "
                    "in the Google Doc template. Add them around the 'Additional charts using your data' section "
                    "so it can be removed automatically when a producer has none to show."
                )
        else:
            try:
                insert_list_with_links_in_gdoc(
                    self.google_doc, df=df_top_additional_charts, placeholder=r"{{top_additional_charts_list}}"
                )
            except ValueError:
                log.warning(
                    "Placeholder '{{top_additional_charts_list}}' not found in the Google Doc template. Add an "
                    "'Additional charts using your data' section with this placeholder to include mdims/explorers "
                    "that also use other producers' data."
                )
            # The section is being kept, so (unlike the empty branch above, which deletes the markers along with
            # the whole section) just strip the marker lines themselves, leaving the section content in place.
            self.google_doc.replace_text(
                mapping={
                    r"{{additional_charts_section_start}}": "",
                    r"{{additional_charts_section_end}}": "",
                }
            )

        # Media mentions: external press/publication coverage of the producer's data, from the Notion impact
        # highlights table, narrowed to this report's period (see gather_analytics). NOTE: unlike
        # print_impact_highlights/the scratch doc, these are not manually curated - every highlight logged for
        # this producer in the period is included, whether or not it's been reviewed for inclusion. This section
        # requires the Google Doc template to wrap it between "{{media_mentions_section_start}}" and
        # "{{media_mentions_section_end}}" marker lines, with a "{{media_mentions_list}}" placeholder inside
        # (plus {{period_humanized}} in its intro text). If there's nothing to show, the whole section (both
        # markers included) is deleted rather than left empty; if the template hasn't been updated with these
        # markers yet, the relevant step is skipped with a warning rather than failing the report.
        if self.highlights is None or self.highlights.empty:
            try:
                self.google_doc.delete_section(
                    start_marker=r"{{media_mentions_section_start}}",
                    end_marker=r"{{media_mentions_section_end}}",
                )
            except ValueError:
                log.warning(
                    "Markers '{{media_mentions_section_start}}'/'{{media_mentions_section_end}}' not found in "
                    "the Google Doc template. Add them around the 'Media mentions' section so it can be removed "
                    "automatically when a producer has none to show."
                )
        else:
            try:
                insert_media_mentions_in_gdoc(
                    self.google_doc,
                    df=self.highlights.sort_values("Date"),
                    placeholder=r"{{media_mentions_list}}",
                )
            except ValueError:
                log.warning(
                    "Placeholder '{{media_mentions_list}}' not found in the Google Doc template. Add a 'Media "
                    "mentions' section with this placeholder to include external coverage highlights."
                )
            # The section is being kept, so (unlike the empty branch above, which deletes the markers along with
            # the whole section) just strip the marker lines themselves, leaving the section content in place.
            self.google_doc.replace_text(
                mapping={
                    r"{{media_mentions_section_start}}": "",
                    r"{{media_mentions_section_end}}": "",
                }
            )

    def create_pdf(self, overwrite: bool = True) -> str:
        """Create PDF from the Google Doc."""
        if not self.google_doc:
            raise ValueError("Google Doc must be created before generating PDF")

        self.pdf_id = self.google_doc.save_as_pdf(overwrite=overwrite)
        return self.pdf_id

    def update_pdf_from_existing(self, doc_id: str, overwrite: bool = True) -> str:
        """Update PDF from an existing Google Doc."""
        self.doc_id = doc_id
        self.google_doc = GoogleDoc(doc_id=doc_id)
        self.pdf_id = self.google_doc.save_as_pdf(overwrite=overwrite)
        return self.pdf_id

    def generate_links(self) -> None:
        """Log report links."""
        if self.doc_link:
            log.info(f"Google Doc: {self.doc_link}")
        if self.pdf_link:
            log.info(f"PDF: {self.pdf_link}")
        if self.doc_link or self.pdf_link:
            log.info(f"Files are saved in folder: {self.folder_link}")

    def get_links(self) -> dict[str, str]:
        """Get all available links for this report."""
        links = {}
        if self.doc_link:
            links["google_doc"] = self.doc_link
        if self.pdf_link:
            links["pdf"] = self.pdf_link
        links["folder"] = self.folder_link
        return links

    def gather_emails(self) -> None:
        # Try to fetch data provider contacts from Notion table using all producer names
        df = get_data_producer_contacts(producers=self.all_producer_names)

        if len(df) >= 1:
            # If multiple rows found (unlikely but possible), try each one
            all_emails = []
            for _, row in df.iterrows():
                emails_raw = row["Emails for analytics reports"]
                # Handle empty cells (None or NaN)
                if pd.notna(emails_raw) and emails_raw:
                    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
                    emails = re.findall(email_pattern, emails_raw)
                    all_emails.extend(emails)

            # Remove duplicates while preserving order
            emails = list(dict.fromkeys(all_emails))
        else:
            emails = []

        if emails:
            self.emails = emails
        else:
            log.warning("Could not find contact emails for this data provider in the Notion contacts page.")
            self.emails = None

    def change_file_permissions(self) -> None:
        # Add data providers emails with commenter permissions.
        if self.emails is not None:
            GoogleDrive().set_file_permissions(
                file_id=self.pdf_id,  # ty: ignore
                role="commenter",
                emails=self.emails,
                send_notification_email=False,
            )
            log.info(f"Commenter access has been granted to emails: {self.emails}")
        else:
            log.warning("Emails are not defined. Consider manually changing sharing permissions directly from the PDF.")

    def create_full_report(
        self, overwrite_pdf: bool = True, grant_permissions: bool = False, notion_df: pd.DataFrame | None = None
    ) -> None:
        """Create a complete report from scratch."""
        self.gather_analytics(notion_df=notion_df)

        # Create the report
        self.create_google_doc()
        self.create_pdf(overwrite=overwrite_pdf)
        self.generate_links()

        # Gather contact emails (with whom reports will be shared).
        self.gather_emails()

        # Change file permissions, to include data providers emails.
        if grant_permissions:
            self.change_file_permissions()


def print_impact_highlights(highlights: pd.DataFrame) -> None:
    # TODO:
    # * Consider creating another column in the highlights table, that contains the description to be shared with the data provider.
    # * Then, here, filter for only those selected highlights where that column is not empty.
    # * Adapt GDoc template to include those highlights, if any.
    # * It might be useful to create a function that writes to GDoc with embedded hyperlinks.
    if not highlights.empty:
        log.info(
            f"{len(highlights)} highlights found for this data producer. Manually check them and consider adding them to the producer GDoc."
        )
        for _, highlight in highlights.iterrows():
            log.info(f"* {highlight['Highlight']}")
            log.info(f"Source link: {highlight['Source link']}")
            # log.info(f"Notion highlight: {highlight['notion_url']}")


@click.command(name="create_data_producer_report", cls=RichCommand, help=__doc__)
@click.option(
    "--producer",
    type=str,
    help="Producer name (canonical name used in report title).",
)
@click.option(
    "--alias",
    "aliases",
    multiple=True,
    help="Alternative producer names (can be specified multiple times, e.g., --alias 'UCDP' --alias 'Uppsala University').",
)
@click.option(
    "--period",
    type=click.Choice(["Q1", "Q2", "Q3", "Q4", "H1", "H2", "Y"]),
    help="Period (Q1, Q2, Q3, Q4, H1, H2, or Y).",
)
@click.option(
    "--year",
    type=int,
    default=datetime.today().year,
    help="Year.",
)
@click.option(
    "--overwrite-pdf/--no-overwrite-pdf",
    default=False,
    help="Overwrite existing PDF if report already exists. Use this to generate a new PDF after manual changes have been made to the GDoc. To clarify, this flag does not regenerate the PDF from scratch.",
)
@click.option(
    "--grant-permissions/--no-grant-permissions",
    default=False,
    help="Grant permissions to data providers to access PDF file.",
)
def run(producer, aliases, period, year, overwrite_pdf, grant_permissions):
    # First check if all required definitions of Google Drive, Doc and Sheet IDs are in place.
    for drive_id in [
        DATA_PRODUCER_REPORT_FOLDER_ID,
        DATA_PRODUCER_REPORT_TEMPLATE_DOC_ID,
        DATA_PRODUCER_REPORT_STATUS_SHEET_ID,
    ]:
        error = "Your .env file should contain all definitions of DATA_PRODUCER_REPORT_*_ID (see .env.example)."
        assert drive_id != "", error

    # Convert aliases tuple to list
    aliases_list = list(aliases) if aliases else []

    # Create report instance (it will automatically check for existing reports).
    report = Report(producer, period, year, aliases=aliases_list)

    if report.exists:
        log.warning(f"Google Doc report already exists for {producer} {period} {year}")
        assert report.doc_id is not None

        if report.has_pdf:
            if overwrite_pdf:
                log.info("Overwriting existing PDF...")
                report.update_pdf_from_existing(report.doc_id, overwrite=True)
                report.generate_links()
            else:
                log.warning("PDF already exists and overwrite_pdf=False. No action taken.")

            if grant_permissions:
                # Gather data provider emails and grant read access to already existing PDF.
                report.gather_emails()
                report.change_file_permissions()

        return

    # Report doesn't exist, create it from scratch
    log.info(f"Creating new report for {producer} {period} {year}")
    report.create_full_report(overwrite_pdf=overwrite_pdf, grant_permissions=grant_permissions)

    # Get impact highlights for all producer names
    highlights = get_impact_highlights(
        producers=report.all_producer_names, min_date=report.min_date, max_date=report.max_date
    )
    print_impact_highlights(highlights=highlights)

    # Add new entry in the status sheet.
    df = pd.DataFrame(
        {
            "producer": [producer],
            "year": [int(year)],
            "period": [period],
            "report": [report.pdf_link],
            "gdoc": [report.doc_link],
            "reviewed": [0],
            "shared with producer on": [None],
        }
    )
    sheet = GoogleSheet(sheet_id=DATA_PRODUCER_REPORT_STATUS_SHEET_ID)
    sheet.append_dataframe(df=df, sheet_name="status")


if __name__ == "__main__":
    run()
