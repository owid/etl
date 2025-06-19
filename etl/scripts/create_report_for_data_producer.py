"""Script to generate a quarterly analytics report for a data producer."""

from datetime import datetime
from typing import Dict, List

import click
import pandas as pd
import requests
from rich_click.rich_command import RichCommand
from structlog import get_logger

from apps.utils.google import GoogleDoc, GoogleDrive, GoogleSheet
from apps.utils.notion import get_impact_highlights
from etl.analytics import (
    get_chart_views_by_chart_id,
    get_post_views_by_chart_id,
    get_visualizations_using_data_by_producer,
)
from etl.data_helpers.misc import humanize_number
from etl.db import get_engine

# Initialize logger.
log = get_logger()

# Initialize database engine.
engine = get_engine()

# Folder ID for reports.
FOLDER_ID = "1SySOSNXgNLEJe2L1k7985p-zSeUoU4kN"

# Document ID of template.
TEMPLATE_ID = "149cLrJK9VI-BNjnM-LxnWgbQoB7mwjSpgjO497rzxeU"

# Document ID of reports status sheet.
STATUS_SHEET_ID = "1CLM4EKiu0DZaNz5BFdDvMvCxbveKUHCsgNd-a4Fumnk"

# Common definitions of quarters.
QUARTERS = {
    1: {"name": "first", "min_date": "01-01", "max_date": "03-31"},
    2: {"name": "second", "min_date": "04-01", "max_date": "06-30"},
    3: {"name": "third", "min_date": "07-01", "max_date": "09-30"},
    4: {"name": "fourth", "min_date": "10-01", "max_date": "12-31"},
}


def get_chart_title_from_url(chart_url: str) -> str:
    response = requests.get(f"{chart_url}.metadata.json")
    title = response.json()["chart"]["title"]
    return title


def run_sanity_checks(df_charts: pd.DataFrame, df_posts: pd.DataFrame) -> None:
    error = "Expected no duplicates in df_producer. If there are, drop duplicates (and check if that's expected)."
    assert df_charts[df_charts.duplicated(subset=["chart_id"])].empty, error

    error = "Unexpected post type."
    assert set(df_posts["post_type"]) <= set(["article", "topic-page", "linear-topic-page", "data-insight"]), error

    error = "Expected no duplicates in df_posts. If there are, drop duplicates (and check if that's expected)."
    assert df_posts[df_posts.duplicated(subset=["url"])].empty, error


def gather_producer_analytics(producer: str, min_date: str, max_date: str) -> Dict[str, pd.DataFrame]:
    # Get charts using data from the current data producer.
    df_producer_charts = get_visualizations_using_data_by_producer(producers=[producer])

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
        df_posts = df_posts.drop(homepage_mask[homepage_mask].index).reset_index(drop=True)  # type: ignore
    # Keep only the information about posts.
    df_posts = (
        df_posts.drop_duplicates(subset=["post_url"])
        .rename(columns={"post_title": "title", "post_url": "url"})
        .drop(columns=["chart_url", "chart_id"])
        .reset_index(drop=True)
    )

    # Sanity checks.
    run_sanity_checks(df_charts=df_charts, df_posts=df_posts)

    # Create a dictionary with all analytics.
    analytics = {"charts": df_charts, "posts": df_posts}

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


class Report:
    """A quarterly analytics report for a data producer."""

    def __init__(self, producer: str, quarter: int, year: int):
        self.producer = producer
        self.quarter = quarter
        self.year = year
        self.title = f"{year}-Q{quarter} Our World in Data analytics report for {producer}"
        self.min_date = f"{year}-{QUARTERS[quarter]['min_date']}"
        self.max_date = f"{year}-{QUARTERS[quarter]['max_date']}"

        # Check if this report already exists in Google Drive
        google_drive = GoogleDrive()
        files = google_drive.list_files_in_folder(folder_id=FOLDER_ID)

        self.doc_id: str | None = None
        self.pdf_id: str | None = None

        # Data provider emails, that will be granted reading permissions to access the pdf reports.
        # NOTE: They will be fetched by gather_emails()
        self.emails: List[str] | None = None

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
        self.analytics: Dict[str, pd.DataFrame] | None = None
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
        return f"https://drive.google.com/drive/folders/{FOLDER_ID}"

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

    def status_with_links(self) -> str:
        """Get a detailed status including links."""
        if not self.exists:
            return "Not created"
        elif not self.has_pdf:
            return f"Google Doc exists (no PDF)\n  📄 Doc: {self.doc_link}"
        else:
            return f"Both Google Doc and PDF exist\n  📄 Doc: {self.doc_link}\n  📋 PDF: {self.pdf_link}"

    def gather_analytics(self) -> None:
        """Gather analytics data for this report."""
        log.info(f"Gathering analytics for {self.producer} Q{self.quarter} {self.year}")
        self.analytics = gather_producer_analytics(
            producer=self.producer, min_date=self.min_date, max_date=self.max_date
        )

    def create_google_doc(self) -> None:
        """Create the Google Doc from template."""
        if not self.analytics:
            raise ValueError("Analytics must be gathered before creating the document")

        # Initialize Google Drive and copy template.
        google_drive = GoogleDrive()
        self.doc_id = google_drive.copy(file_id=TEMPLATE_ID, body={"name": self.title})
        self.google_doc = GoogleDoc(doc_id=self.doc_id)

        # Populate the document.
        self._populate_document()

    def _populate_document(self) -> None:
        """Internal method to populate the Google Doc with data."""
        if not self.analytics or not self.google_doc:
            raise ValueError("Analytics and Google Doc must be initialized")

        # Create dataframes for top content.
        df_top_charts = (
            self.analytics["charts"]
            .sort_values("views", ascending=False)[["url", "views", "title", "featured_on_homepage"]]
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

        # Calculate metrics.
        n_charts = len(self.analytics["charts"])
        n_publications = len(self.analytics["posts"])
        n_chart_views = self.analytics["charts"]["views"].sum()
        n_post_views = self.analytics["posts"]["views"].sum()
        n_daily_chart_views = n_chart_views / self.analytics["charts"]["n_days"].max()
        n_daily_post_views = n_post_views / self.analytics["posts"]["n_days"].max()

        # Humanize numbers.
        n_charts_humanized = humanize_number(n_charts)
        n_posts_humanized = humanize_number(n_publications)
        n_chart_views_humanized = humanize_number(n_chart_views)
        n_daily_chart_views_humanized = humanize_number(n_daily_chart_views)
        n_post_views_humanized = humanize_number(n_post_views)
        n_daily_post_views_humanized = humanize_number(n_daily_post_views)
        max_date_humanized = datetime.strptime(
            f"{self.year}-{QUARTERS[self.quarter]['max_date']}", "%Y-%m-%d"
        ).strftime("%B %d, %Y")
        quarter_date_humanized = f"the {QUARTERS[self.quarter]['name']} quarter of {self.year}"

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
            r"{{year}}": str(self.year),
            r"{{quarter}}": str(self.quarter),
            r"{{executive_summary_intro}}": executive_summary_intro,
            r"{{n_charts_humanized}}": n_charts_humanized,
            r"{{n_posts_humanized}}": n_posts_humanized,
            r"{{n_post_views_humanized}}": n_post_views_humanized,
            r"{{quarter_date_humanized}}": quarter_date_humanized,
            r"{{n_chart_views_humanized}}": n_chart_views_humanized,
            r"{{n_daily_chart_views_humanized}}": n_daily_chart_views_humanized,
            r"{{n_daily_post_views_humanized}}": n_daily_post_views_humanized,
        }
        self.google_doc.replace_text(mapping=replacements)

        # Add content
        top_chart_url = df_top_charts.iloc[0]["url"] + ".png"
        self.google_doc.insert_image(image_url=top_chart_url, placeholder=r"{{top_chart_image}}", width=320)
        insert_list_with_links_in_gdoc(self.google_doc, df=df_top_charts, placeholder=r"{{top_charts_list}}")
        insert_list_with_links_in_gdoc(self.google_doc, df=df_top_posts, placeholder=r"{{top_posts_list}}")

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

    def get_links(self) -> Dict[str, str]:
        """Get all available links for this report."""
        links = {}
        if self.doc_link:
            links["google_doc"] = self.doc_link
        if self.pdf_link:
            links["pdf"] = self.pdf_link
        links["folder"] = self.folder_link
        return links

    def gather_emails(self) -> None:
        # TODO: Read contact emails from Notion.
        log.warning("Could not find contact emails for this data provider in the Notion contacts page.")
        self.emails = None

    def change_file_permissions(self) -> None:
        # Add data providers emails with reading premissions.
        if self.emails is not None:
            GoogleDrive().set_file_permissions(file_id=self.pdf_id, role="reader", emails=self.emails)  # type: ignore
        else:
            log.warning("Emails are not defined. Consider manually changing sharing permissions directly from the PDF.")

    def create_full_report(self, overwrite_pdf: bool = True) -> None:
        """Create a complete report from scratch."""
        self.gather_analytics()

        # Get impact highlights
        highlights = get_impact_highlights(producers=[self.producer], min_date=self.min_date, max_date=self.max_date)
        print_impact_highlights(highlights=highlights)

        # Create the report
        self.create_google_doc()
        self.create_pdf(overwrite=overwrite_pdf)
        self.generate_links()

        # Gather contact emails (with whome reports will be shared).
        self.gather_emails()

        # Change file permissions, to include data providers emails.
        self.change_file_permissions()

    def print_summary(self) -> None:
        """Print a comprehensive summary of the report status and links."""
        print(f"\n📊 Report: {self.title}")
        print(f"Status: {self.status}")
        print(f"Period: {self.min_date} to {self.max_date}")

        if self.doc_link:
            print(f"📄 Google Doc: {self.doc_link}")
        if self.pdf_link:
            print(f"📋 PDF: {self.pdf_link}")
        print(f"📁 Folder: {self.folder_link}")

        if self.analytics:
            n_charts = len(self.analytics["charts"])
            n_posts = len(self.analytics["posts"])
            print(f"📈 Analytics: {n_charts} charts, {n_posts} posts")
        print()


def print_impact_highlights(highlights: pd.DataFrame) -> None:
    # TODO:
    # * Consider creating another column in the highlights table, that contains the description to be shared with the data provider.
    # * Then, here, filter for only those selected highlights where that column is not empty.
    # * Adapt GDoc template to include those highlights, if any.
    # * It might be useful to create a function that writes to GDoc with embedded hyperlinks.
    if not highlights.empty:
        print(
            f"{len(highlights)} highlights found for this data producer. Manually check them and consider adding them to the producer GDoc."
        )
        for _, highlight in highlights.iterrows():
            print(f"* {highlight['Highlight']}")
            print(f"Source link: {highlight['Source link']}")
            print(f"Notion highlight: {highlight['notion_url']}")


@click.command(name="create_data_producer_report", cls=RichCommand, help=__doc__)
@click.option(
    "--producer",
    type=str,
    help="Producer name(s).",
)
@click.option(
    "--quarter",
    type=int,
    help="Quarter (1, 2, 3, or 4).",
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
    help="Overwrite existing PDF if report already exists.",
)
def run(producer, quarter, year, overwrite_pdf):
    # Create report instance (it will automatically check for existing reports)
    report = Report(producer, quarter, year)

    print("\n📊 Report Status:")
    print(report.status_with_links())
    print(f"📁 Folder: {report.folder_link}")
    print()

    if report.exists:
        log.warning(f"Google Doc report already exists for {producer} Q{quarter} {year}")
        # Since report.exists is True, we know doc_id is not None
        assert report.doc_id is not None

        if report.has_pdf:
            if overwrite_pdf:
                log.info("Overwriting existing PDF...")
                report.update_pdf_from_existing(report.doc_id, overwrite=True)
                report.generate_links()
            else:
                log.warning("PDF already exists and overwrite_pdf=False. No action taken.")
        else:
            if overwrite_pdf:
                log.info("Creating PDF from existing Google Doc...")
                report.update_pdf_from_existing(report.doc_id, overwrite=True)
                report.generate_links()
            else:
                log.warning("Google Doc exists but no PDF found, and overwrite_pdf=False. No action taken.")
        return

    # Report doesn't exist, create it from scratch
    log.info(f"Creating new report for {producer} Q{quarter} {year}")
    report.create_full_report(overwrite_pdf=overwrite_pdf)

    # Add new entry in the status sheet.
    df = pd.DataFrame(
        {
            "producer": [producer],
            "year": [int(year)],
            "quarter": [int(quarter)],
            "report": [report.pdf_link],
            "gdoc": [report.doc_link],
            "reviewed": [0],
            "shared with producer on": [None],
        }
    )
    sheet = GoogleSheet(sheet_id=STATUS_SHEET_ID)
    sheet.append_dataframe(df=df, sheet_name="status")


if __name__ == "__main__":
    run()
