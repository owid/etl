"""Run create_report_for_data_producer for all providers listed in providers.yml."""

from pathlib import Path

import pandas as pd
import yaml
from create_report_for_data_producer import PERIODS, Report, get_impact_highlights, print_impact_highlights
from structlog import get_logger

from etl.analytics.data import get_explorer_views_by_url
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


def append_producer_to_scratch_doc(
    doc: GoogleDoc, producer: str, highlights: pd.DataFrame, explorers: pd.DataFrame
) -> None:
    """Append a producer section to the shared scratch Google Doc.

    The section looks like:

        <producer name>         ← Heading 2
        Highlights              ← Heading 3
        • <highlight>
        • ...
        Explorers               ← Heading 3
        • <explorer line>
        • ...

    Parameters
    ----------
    doc : GoogleDoc
        The scratch doc to append to.
    producer : str
        Producer name, used as the section heading.
    highlights : pd.DataFrame
        DataFrame containing highlight bullets.
    explorers : pd.DataFrame
        DataFrame containing explorer bullets (e.g. "Title – 10,709 views").
    """
    # Fetch the current document to find the end index.
    raw_doc = doc.drive.docs_service.documents().get(documentId=doc.doc_id).execute()
    content = raw_doc.get("body", {}).get("content", [])
    end_index = content[-1]["endIndex"] - 1  # -1 to stay inside the body

    # Build the full text block to insert.
    highlights_text = "".join(f"• {h}\n" for h in highlights) if highlights else "• (none)\n"

    explorers_text = "".join(f"• {e}\n" for e in explorers) if explorers else "• (none)\n"

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


def main() -> None:
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
                log.warning(f"Report already exists for {producer} — skipping")
                continue

            log.info(f"Creating report for {producer}")
            report.create_full_report(overwrite_pdf=False, grant_permissions=False)

            highlights_df = get_impact_highlights(
                producers=report.all_producer_names,
                min_date=report.min_date,
                max_date=report.max_date,
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

            explorers_df = get_explorer_views_by_url(
                urls=entry.get("explorer_links", []), date_min=min_date, date_max=max_date
            )

            append_producer_to_scratch_doc(
                doc=doc,
                producer=producer,
                highlights=highlights_df,
                explorers=explorers_df,
            )

        except Exception as e:
            log.error(f"Failed to create report for {producer}: {e}")
            continue

    log.info(
        f"Done. Files can be found in the Google Drive folder: https://drive.google.com/drive/folders/{DATA_PRODUCER_REPORT_FOLDER_ID}"
    )


if __name__ == "__main__":
    main()
