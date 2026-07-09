"""Run create_report_for_data_producer for all providers listed in providers.yml."""

from pathlib import Path

import pandas as pd
import yaml
from create_report_for_data_producer import Report, get_impact_highlights, print_impact_highlights
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

    notion_table_period = get_notion_table_period()

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

        except Exception as e:
            log.error(f"Failed to create report for {producer}: {e}")
            continue

    log.info("Done.")


if __name__ == "__main__":
    main()
