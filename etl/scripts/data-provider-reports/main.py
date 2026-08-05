"""Run create_report_for_data_producer for all providers listed in providers.yml."""

from pathlib import Path

import click
import pandas as pd
import yaml
from create_report_for_data_producer import PERIODS, Report, get_impact_highlights, print_impact_highlights
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.analytics.data import get_mdim_explorer_views_by_producer
from etl.config import (
    DATA_PRODUCER_REPORT_FOLDER_ID,
    DATA_PRODUCER_REPORT_STATUS_SHEET_ID,
    DATA_PRODUCER_REPORT_TEMPLATE_DOC_ID,
)
from etl.google import GoogleDoc, GoogleDrive, GoogleSheet
from etl.notion import get_notion_table_period

log = get_logger()

PROVIDERS_FILE = Path(__file__).parent / "providers.yml"


def append_to_scratch_file(text: str) -> str:
    """Append text to scratch file."""
    scratch_file = Path(__file__).parent / "scratch.txt"
    with scratch_file.open("a") as f:
        f.write(text + "\n")
    return text


def _format_highlight_bullet(row: pd.Series) -> str:
    """Format one get_impact_highlights row as a bullet: the highlight text, its source link, and any comments."""
    lines = [f"• {row['Highlight']}"]
    if pd.notna(row.get("Source link")) and row["Source link"]:
        lines.append(f"    Link: {row['Source link']}")
    comments = [
        str(row[col]).strip()
        for col in ["Additional info", "Inclusion discussion"]
        if pd.notna(row.get(col)) and str(row[col]).strip()
    ]
    if comments:
        lines.append(f"    Comments: {' | '.join(comments)}")
    return "\n".join(lines) + "\n"


def _format_mdim_explorer_bullet(row: pd.Series) -> str:
    """Format one get_mdim_explorer_views_by_producer row as a bullet."""
    attribution_emoji = "‼️" if row["uses_other_producers_data"] else "✅"
    title = row["title"] or row["slug"]
    return (
        f"• {row['slug']} — {title} — {row['url']} — "
        f"{row['views']:,.0f} views ({row['views_daily']:,.1f}/day) {attribution_emoji}\n"
    )


def append_producer_to_scratch_doc(
    doc: GoogleDoc,
    producer: str,
    highlights: pd.DataFrame,
    highlights_min_date: str,
    highlights_max_date: str,
    mdims_and_explorers: pd.DataFrame,
) -> None:
    """Append a producer section to the shared scratch Google Doc.

    The section looks like:

        <producer name>         ← Heading 2
        Highlights              ← Heading 3
        • <highlight text>
            Link: <Source link>
            Comments: <Additional info / Inclusion discussion, if any>
        • ...
        Explorers               ← Heading 3
        • <slug> — <title> — <url> — <views> views (<views_daily>/day) <✅ or ‼️>
        • ...

    Parameters
    ----------
    doc : GoogleDoc
        The scratch doc to append to.
    producer : str
        Producer name, used as the section heading.
    highlights : pd.DataFrame
        DataFrame as returned by get_impact_highlights (columns include Highlight, Date, Source link,
        Additional info, Inclusion discussion, ...), carrying the producer's FULL highlights history. It's
        narrowed down to [highlights_min_date, highlights_max_date] here, only for what gets written to the
        scratch doc - callers that need the unfiltered history (e.g. print_impact_highlights) should keep using
        the full dataframe. NOTE: also not filtered by Include? - all highlights in the period are written, for
        manual review.
    highlights_min_date, highlights_max_date : str
        Date range (inclusive) to filter highlights to, based on their Date column - normally the report's
        period.
    mdims_and_explorers : pd.DataFrame
        DataFrame with columns slug, type, title, url, views, n_days, views_daily, uses_other_producers_data
        (see get_mdim_explorer_views_by_producer). uses_other_producers_data drives the ✅/‼️ marker: ‼️ means
        the mdim/explorer also uses at least one indicator from some other producer, so its views may not be
        fully attributable to this producer.
    """
    # Fetch the current document to find the end index.
    raw_doc = doc.drive.docs_service.documents().get(documentId=doc.doc_id).execute()
    content = raw_doc.get("body", {}).get("content", [])
    end_index = content[-1]["endIndex"] - 1  # -1 to stay inside the body

    # Narrow highlights down to the report's period, only for what gets written here.
    period_highlights = (
        highlights.loc[
            highlights["Date"].between(pd.to_datetime(highlights_min_date), pd.to_datetime(highlights_max_date))
        ]
        if not highlights.empty
        else highlights
    )

    # Build the full text block to insert.
    highlights_text = (
        "".join(_format_highlight_bullet(row) for _, row in period_highlights.iterrows())
        if not period_highlights.empty
        else "• (none)\n"
    )

    explorers_text = (
        "".join(_format_mdim_explorer_bullet(row) for _, row in mdims_and_explorers.iterrows())
        if not mdims_and_explorers.empty
        else "• (none)\n"
    )

    producer_heading = f"{producer}\n"
    highlights_heading = "Highlights\n"
    explorers_heading = "Explorers\n"

    full_text = producer_heading + highlights_heading + highlights_text + explorers_heading + explorers_text

    # Compute character offsets for each heading so we can style them.
    producer_start = end_index
    producer_end = producer_start + len(producer_heading)
    highlights_heading_start = producer_end
    highlights_heading_end = highlights_heading_start + len(highlights_heading)
    explorers_heading_start = highlights_heading_end + len(highlights_text)
    explorers_heading_end = explorers_heading_start + len(explorers_heading)

    doc.edit(
        requests=[
            {"insertText": {"location": {"index": end_index}, "text": full_text}},
            {
                "updateParagraphStyle": {
                    "range": {"startIndex": producer_start, "endIndex": producer_end},
                    "paragraphStyle": {"namedStyleType": "HEADING_2"},
                    "fields": "namedStyleType",
                }
            },
            {
                "updateParagraphStyle": {
                    "range": {"startIndex": highlights_heading_start, "endIndex": highlights_heading_end},
                    "paragraphStyle": {"namedStyleType": "HEADING_3"},
                    "fields": "namedStyleType",
                }
            },
            {
                "updateParagraphStyle": {
                    "range": {"startIndex": explorers_heading_start, "endIndex": explorers_heading_end},
                    "paragraphStyle": {"namedStyleType": "HEADING_3"},
                    "fields": "namedStyleType",
                }
            },
        ]
    )


@click.command(name="data_provider_reports", cls=RichCommand, help=__doc__)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Run all producers even if a report already exists for them, instead of skipping. "
    "NOTE: create_full_report always copies a fresh doc from the template, so this creates an additional "
    "report alongside the existing one rather than overwriting it.",
)
def main(force: bool) -> None:
    config = yaml.safe_load(PROVIDERS_FILE.read_text())
    year: int = int(config["YEAR"])
    period: str = config["PERIOD"]
    providers: list[dict] = config["PROVIDERS"]

    for drive_id in [
        DATA_PRODUCER_REPORT_FOLDER_ID,
        DATA_PRODUCER_REPORT_TEMPLATE_DOC_ID,
        DATA_PRODUCER_REPORT_STATUS_SHEET_ID,
    ]:
        error = "Your .env file should contain all definitions of DATA_PRODUCER_REPORT_*_ID (see .env.example)."
        assert drive_id != "", error

    log.info(f"Running reports for {len(providers)} providers — {period} {year}")

    doc_id = GoogleDrive().create_doc({"title": "LIS – scratch notes 2025-H1"})
    GoogleDrive().move(file_id=doc_id, folder_id=DATA_PRODUCER_REPORT_FOLDER_ID)

    doc = GoogleDoc(doc_id=doc_id)

    notion_table_period = get_notion_table_period()

    min_date = f"{year}-{PERIODS[period]['min_date']}"
    max_date = f"{year}-{PERIODS[period]['max_date']}"

    for entry in providers:
        producer: str = entry["name"]
        raw_alias = entry.get("alias")
        if isinstance(raw_alias, list):
            aliases = raw_alias
        elif raw_alias:
            aliases = [raw_alias]
        else:
            aliases = []

        try:
            report = Report(producer, period, year, aliases=aliases)

            if report.exists:
                if not force:
                    log.warning(f"Report already exists for {producer} — skipping")
                    continue
                log.warning(f"Report already exists for {producer} — creating an additional one anyway (--force)")

            log.info(f"Creating report for {producer}")
            report.create_full_report(overwrite_pdf=False, grant_permissions=False)

            # NOTE: notion_table_period carries the full highlights history (unfiltered by date) - it's only
            # narrowed down to the report's period when writing to the scratch doc below, so that anyone
            # reviewing print_impact_highlights output still sees every highlight for this producer.
            highlights_df = get_impact_highlights(
                producers=report.all_producer_names,
                df=notion_table_period,
            )
            print_impact_highlights(highlights=highlights_df)

            df = pd.DataFrame(
                {
                    "producer": [producer],
                    "year": [year],
                    "period": [period],
                    "report": [report.pdf_link],
                    "gdoc": [report.doc_link],
                    "reviewed": [0],
                    "shared with producer on": [None],
                }
            )
            sheet = GoogleSheet(sheet_id=DATA_PRODUCER_REPORT_STATUS_SHEET_ID)
            sheet.append_dataframe(df=df, sheet_name="status")

            mdims_and_explorers_df = get_mdim_explorer_views_by_producer(
                producers=report.all_producer_names, date_min=min_date, date_max=max_date
            )

            append_producer_to_scratch_doc(
                doc=doc,
                producer=producer,
                highlights=highlights_df,
                highlights_min_date=min_date,
                highlights_max_date=max_date,
                mdims_and_explorers=mdims_and_explorers_df,
            )

        except Exception as e:
            log.error(f"Failed to create report for {producer}: {e}")
            continue

    log.info(
        f"Done. Files can be found in the Google Drive folder: https://drive.google.com/drive/folders/{DATA_PRODUCER_REPORT_FOLDER_ID}"
    )


if __name__ == "__main__":
    main()
