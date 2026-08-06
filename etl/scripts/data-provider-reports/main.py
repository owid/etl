"""Run create_report_for_data_producer for all providers listed in providers.yml."""

from pathlib import Path

import click
import pandas as pd
import yaml
from create_report_for_data_producer import PERIODS, Report, get_impact_highlights, print_impact_highlights
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.config import (
    DATA_PRODUCER_REPORT_FOLDER_ID,
    DATA_PRODUCER_REPORT_STATUS_SHEET_ID,
    DATA_PRODUCER_REPORT_TEMPLATE_DOC_ID,
)
from etl.google import GoogleSheet
from etl.notion import get_notion_table_period

log = get_logger()

PROVIDERS_FILE = Path(__file__).parent / "providers.yml"


def _get_aliases(entry: dict) -> list[str]:
    """Normalize a providers.yml entry's "alias" field (missing, a string, or a list) to a list."""
    raw_alias = entry.get("alias")
    if isinstance(raw_alias, list):
        return raw_alias
    elif raw_alias:
        return [raw_alias]
    return []


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
@click.option(
    "--update-pdfs",
    is_flag=True,
    default=False,
    help="Don't create any new reports - for every provider that already has a Google Doc, just re-export its "
    "current content to PDF, overwriting the existing PDF. Producers with no existing Google Doc are skipped. "
    "Ignores --force.",
)
def main(force: bool, update_pdfs: bool) -> None:
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

    if update_pdfs:
        log.info(f"Updating PDFs for {len(providers)} providers — {period} {year}")
        for entry in providers:
            producer: str = entry["name"]
            aliases = _get_aliases(entry)

            try:
                report = Report(producer, period, year, aliases=aliases)
                if not report.exists:
                    log.warning(f"No existing Google Doc found for {producer} — skipping")
                    continue
                assert report.doc_id is not None

                log.info(f"Updating PDF for {producer}")
                report.update_pdf_from_existing(report.doc_id, overwrite=True)
                report.generate_links()
            except Exception as e:
                log.error(f"Failed to update PDF for {producer}: {e}")
                continue

        log.info(
            f"Done. Files can be found in the Google Drive folder: https://drive.google.com/drive/folders/{DATA_PRODUCER_REPORT_FOLDER_ID}"
        )
        return

    log.info(f"Running reports for {len(providers)} providers — {period} {year}")

    min_date = f"{year}-{PERIODS[period]['min_date']}"
    max_date = f"{year}-{PERIODS[period]['max_date']}"

    # Fetch impact highlights from Notion already filtered to this run's period.
    notion_table_period = get_notion_table_period(min_date=min_date, max_date=max_date)

    for entry in providers:
        producer: str = entry["name"]
        aliases = _get_aliases(entry)

        try:
            report = Report(producer, period, year, aliases=aliases)

            if report.exists:
                if not force:
                    log.warning(f"Report already exists for {producer} — skipping")
                    continue
                log.warning(f"Report already exists for {producer} — creating an additional one anyway (--force)")

            log.info(f"Creating report for {producer}")
            report.create_full_report(overwrite_pdf=False, grant_permissions=False, notion_df=notion_table_period)

            # notion_table_period is already filtered to this run's period, so this only filters by producer.
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

        except Exception as e:
            log.error(f"Failed to create report for {producer}: {e}")
            continue

    log.info(
        f"Done. Files can be found in the Google Drive folder: https://drive.google.com/drive/folders/{DATA_PRODUCER_REPORT_FOLDER_ID}"
    )


if __name__ == "__main__":
    main()
