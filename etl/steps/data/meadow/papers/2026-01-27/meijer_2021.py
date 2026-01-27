"""Load a snapshot and extract Table S8 (Country Statistics) from Meijer et al. (2021)."""

import pdfplumber
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run() -> None:
    """Extract Table S8 from the PDF and create a meadow dataset."""
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("meijer_2021.pdf")

    #
    # Process data.
    #
    log.info("Extracting Table S8 from PDF")
    tb = extract_table_s8(snap.path)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country"], short_name=paths.short_name)
    for col in tb.columns:
        tb[col].metadata.origins = [snap.metadata.origin]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def extract_table_s8(pdf_path: str) -> Table:
    """
    Extract Table S8 (Country Statistics) from the Meijer et al. (2021) supplementary PDF.

    The table spans pages 25-29 and contains country-level data on plastic emissions.
    """
    # Table S8 spans pages 25-29 (0-indexed: pages 24-28)
    table_pages = range(24, 29)

    all_rows = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num in table_pages:
            page = pdf.pages[page_num]
            # Use custom table extraction settings to handle wrapped text better
            table_settings = {
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "intersection_tolerance": 3,
            }
            tables = page.extract_tables(table_settings=table_settings)

            if not tables:
                log.warning(f"No tables found on page {page_num + 1}")
                continue

            # Usually there's one table per page
            for table in tables:
                current_country = None
                for row in table:
                    # The PDF has sparse structure with data at specific indices
                    # Country is at index 1, data columns at indices 4, 7, 10, 13, 16, 19, 22, 25, 28
                    if len(row) < 29:
                        continue

                    # Get country name from index 1
                    country = row[1]
                    if not country or not country.strip():
                        continue

                    # Skip header rows
                    country_str = str(country).strip()
                    if any(keyword in country_str for keyword in ["Country", "Area", "[km", "administrative"]):
                        continue

                    if country_str.startswith("Table"):
                        continue

                    # Check if this row has numeric data in column 4 (area)
                    area_value = str(row[4]).strip() if len(row) > 4 and row[4] else ""
                    # A data row will have either a number or be empty, but not just alphabetic text
                    has_numeric_data = bool(area_value) and (
                        area_value.replace(",", "").replace(".", "").isdigit()
                        or area_value in ["", "NoData", "n/a", "-"]
                    )

                    if has_numeric_data or not area_value:
                        # This is a complete data row
                        if current_country:
                            # Use the accumulated country name
                            row = list(row)
                            row[1] = current_country + " " + country_str
                            current_country = None
                        all_rows.append(row)
                    else:
                        # This is a wrapped country name, accumulate it
                        if current_country:
                            current_country += " " + country_str
                        else:
                            current_country = country_str

    log.info(f"Extracted {len(all_rows)} rows from Table S8")

    # Create DataFrame with appropriate column names
    columns = [
        "country",
        "area_km2",
        "coast_length_km",
        "rainfall_mm_per_year",
        "factor_l_a",
        "factor_l_a_p",
        "p_e_percent",
        "mpw_tons_per_year",
        "me_tons_per_year",
        "ratio_me_mpw",
    ]

    # Extract data from the correct column indices
    # PDF structure: country at index 1, data at indices 4, 7, 10, 13, 16, 19, 22, 25, 28
    data_indices = [1, 4, 7, 10, 13, 16, 19, 22, 25, 28]
    valid_rows = []
    for row in all_rows:
        extracted_row = [row[i] if i < len(row) else None for i in data_indices]
        valid_rows.append(extracted_row)

    # Fix known split country names
    # Some country names are split across rows in the PDF
    country_name_fixes = {
        "Congo (Democratic": "Congo (Democratic Republic of the)",
        "Republic of the)": None,  # Remove - merged with previous
        "Federated States of": "Federated States of Micronesia",
        "Micronesia": None,  # Will be removed if it follows "Federated States of"
        "Saint Vincent and the": "Saint Vincent and the Grenadines",
        "Grenadines": None,  # Remove - merged with previous
    }

    # Apply fixes
    i = 0
    while i < len(valid_rows):
        country = str(valid_rows[i][0]).strip()
        if country in country_name_fixes:
            replacement = country_name_fixes[country]
            if replacement:
                valid_rows[i][0] = replacement
                # Remove the next row if it's the continuation
                if i + 1 < len(valid_rows):
                    next_country = str(valid_rows[i + 1][0]).strip()
                    if next_country in country_name_fixes and country_name_fixes[next_country] is None:
                        valid_rows.pop(i + 1)
                i += 1
            else:
                # This row should be removed (it's a continuation)
                valid_rows.pop(i)
        else:
            i += 1

    tb = Table(valid_rows, columns=columns)

    # Clean and convert data types
    for col in tb.columns:
        if col != "country":
            # Remove commas and percentage signs
            tb[col] = tb[col].astype(str).str.replace(",", "").str.replace("%", "")
            # Handle scientific notation and special values
            tb[col] = tb[col].replace({"NoData": None, "n/a": None, "-": None, "": None, "None": None})

    # Convert numeric columns using nullable float type
    numeric_cols = [col for col in columns if col != "country"]
    for col in numeric_cols:
        tb[col] = tb[col].astype("Float64")

    # Clean country names
    tb["country"] = tb["country"].astype(str).str.strip()

    return tb
