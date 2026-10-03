#!/usr/bin/env python
"""Extract UN custodian agencies from SDG indicator pages.

This script scrapes custodian agency information from https://sdg-indikatoren.de/
and is useful for generating or checking the un_sdg.sources_additional.json file.
"""

import json
from pathlib import Path

import click
import requests
from bs4 import BeautifulSoup


def extract_custodian_agency(indicator_id: str) -> str | None:
    """Extract UN custodian agency from SDG indicator page.

    Args:
        indicator_id: SDG indicator ID (e.g., "1.4.2")

    Returns:
        UN custodian agency text or None if not found
    """
    # Convert dots to dashes for URL (e.g., "1.4.2" -> "1-4-2")
    url_id = indicator_id.replace(".", "-")
    url = f"https://sdg-indikatoren.de/en/{url_id}/"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching {indicator_id}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    row = soup.find("th", string="UN custodian agency ")

    if row:
        custodian_agency = row.find_next("td").get_text(separator="\n", strip=True)

        # Replace all variations of "United Nations Children's Fund" with "UNICEF"
        # IMPORTANT: Do this BEFORE stripping abbreviations in parentheses
        custodian_agency = custodian_agency.replace(
            "United Nations International Children's Emergency Fund (UNICEF)", "UNICEF"
        )
        custodian_agency = custodian_agency.replace("United Nations Children's Fund (UNICEF)", "UNICEF")
        custodian_agency = custodian_agency.replace("United Nations Childrens Fund (UNICEF)", "UNICEF")

        # Replace UN Women before stripping abbreviations
        custodian_agency = custodian_agency.replace(
            "The United Nations Entity for Gender Equality and the Empowerment of Women (UN Women)", "UN Women"
        )
        custodian_agency = custodian_agency.replace(
            "United Nations Entity for Gender Equality and the Empowerment of Women (UN Women)", "UN Women"
        )

        # Strip remaining abbreviations in parentheses
        import re

        custodian_agency = re.sub(r"\s*\([^)]+\)", "", custodian_agency)

        # Replace newlines with " and "
        custodian_agency = custodian_agency.replace("\n", " and ")

        return custodian_agency

    return None


@click.command()
@click.option(
    "--input-file",
    "-i",
    default="etl/steps/data/grapher/un/2025-10-29/un_sdg.sources_additional.json",
    help="Input JSON file with indicator IDs",
)
@click.option(
    "--output-file",
    "-o",
    default="etl/steps/data/grapher/un/2025-10-29/un_sdg.custodian_agencies.json",
    help="Output JSON file for custodian agencies",
)
def main(input_file: str, output_file: str):
    """Extract UN custodian agencies for all SDG indicators."""

    # Load input file
    input_path = Path(input_file)
    with open(input_path) as f:
        data = json.load(f)

    # Extract indicator IDs (skip _COMMENT key)
    indicator_ids = [k for k in data.keys() if not k.startswith("_")]

    print(f"Extracting custodian agencies for {len(indicator_ids)} indicators...")

    # Extract custodian agencies
    custodian_agencies = {}
    for i, indicator_id in enumerate(indicator_ids, 1):
        print(f"[{i}/{len(indicator_ids)}] Processing {indicator_id}...")
        custodian_agency = extract_custodian_agency(indicator_id)

        if custodian_agency:
            custodian_agencies[indicator_id] = custodian_agency
            print(f"  ✓ {custodian_agency}")
        else:
            print("  ✗ Not found")

    # Save output
    output_path = Path(output_file)
    with open(output_path, "w") as f:
        json.dump(custodian_agencies, f, indent=4, ensure_ascii=False)

    print(f"\n✓ Saved {len(custodian_agencies)} custodian agencies to {output_path}")


if __name__ == "__main__":
    main()
