"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("unwto.xlsx")

    #
    # Process data.
    #
    # The new data format is a ZIP file containing multiple Excel files organized by category
    # Each file has a "Data" sheet with columns: indicator_code, indicator_label, reporter_area_label, year, value, etc.

    # Map files within the ZIP to logical categories
    files_to_process = [
        ("02_Inbound/01_Total_arrivals/UN_Tourism_inbound_arrivals_12_2025.xlsx", "Inbound Tourism-Arrivals"),
        (
            "02_Inbound/03_Total_arrivals_by_region/UN_Tourism_inbound_arrivals_by_region_12_2025.xlsx",
            "Inbound Tourism-Regions",
        ),
        (
            "02_Inbound/04_Total_arrivals_by_main_purpose/UN_Tourism_inbound_arrivals_by_purpose_12_2025.xlsx",
            "Inbound Tourism-Purpose",
        ),
        (
            "02_Inbound/05_Total_arrivals_by_mode_of_transport/UN_Tourism_inbound_arrivals_by_transport_12_2025.xlsx",
            "Inbound Tourism-Transport",
        ),
        (
            "02_Inbound/06_Accommodation_guests_and_overnights/UN_Tourism_inbound_accommodation_12_2025.xlsx",
            "Inbound Tourism-Accommodation",
        ),
        ("02_Inbound/02_Expenditure/UN_Tourism_inbound_expenditure_12_2025.xlsx", "Inbound Tourism-Expenditure"),
        ("01_Domestic/01_Total_trips/UN_Tourism_domestic_trips_12_2025.xlsx", "Domestic Tourism-Trips"),
        (
            "01_Domestic/02_Accommodation/UN_Tourism_domestic_accommodation_12_2025.xlsx",
            "Domestic Tourism-Accommodation",
        ),
        ("03_Outbound/01_Total_departures/UN_Tourism_outbound_departures_12_2025.xlsx", "Outbound Tourism-Departures"),
        ("03_Outbound/02_Expenditure/UN_Tourism_outbound_expenditure_12_2025.xlsx", "Outbound Tourism-Expenditure"),
        (
            "04_Accommodation/01_Accommodation_in_hotels_and_similar_establishments/UN_Tourism_accommodation_hotels_12_2025.xlsx",
            "Tourism Industries",
        ),
        ("06_Employment/UN_Tourism_8_9_2_employed_persons_04_2025.xlsx", "Employment"),
        ("07_SDGs/UN_Tourism_12_b_1_TSA_SEEA_04_2025.xlsx", "Tourism Industries-Environment"),
        ("05_Macroeconomic/UN_Tourism_8_9_1_TDGDP_04_2025.xlsx", "Tourism Industries-GDP"),
    ]

    # Read the ZIP file using snapshot's extracted() context manager
    tbs = []
    with snap.extracted() as archive:
        for file_path, category in files_to_process:
            try:
                # Read the Excel file from the extracted archive
                full_path = archive.path / file_path

                # SDG files use a different format (Employment, Environment, GDP)
                if "Employment" in file_path:
                    tb = pr.read_excel(full_path, sheet_name="SDG 8.9.2")
                    # Rename columns for Employment file
                    tb = tb.rename(
                        columns={
                            "GeoAreaName": "country",
                            "SeriesDescription": "indicator",
                            "TimePeriod": "year",
                            "Value": "value",
                        }
                    )
                    # Select only the columns we need
                    tb = tb[["country", "year", "indicator", "value"]].copy()
                elif "12_b_1_TSA_SEEA" in file_path:
                    # Environment monitoring file
                    tb = pr.read_excel(full_path, sheet_name="SDG 12.b.1")
                    tb = tb.rename(
                        columns={
                            "GeoAreaName": "country",
                            "SeriesDescription": "indicator",
                            "TimePeriod": "year",
                            "Value": "value",
                        }
                    )
                    tb = tb[["country", "year", "indicator", "value"]].copy()
                elif "8_9_1_TDGDP" in file_path:
                    # GDP file
                    tb = pr.read_excel(full_path, sheet_name="SDG 8.9.1")
                    tb = tb.rename(
                        columns={
                            "GeoAreaName": "country",
                            "SeriesDescription": "indicator",
                            "TimePeriod": "year",
                            "Value": "value",
                        }
                    )
                    tb = tb[["country", "year", "indicator", "value"]].copy()
                elif "by_region" in file_path:
                    # Regions file has partner_area_label that needs to be part of indicator
                    tb = pr.read_excel(full_path, sheet_name="Data")
                    tb = tb.rename(
                        columns={
                            "reporter_area_label": "country",
                            "indicator_label": "indicator",
                        }
                    )
                    # Combine indicator and partner area for regions data
                    tb["indicator"] = (
                        tb["indicator"].astype(str) + " - " + tb["partner_area_label"].fillna("").astype(str)
                    )
                    tb = tb[["country", "year", "indicator", "value"]].copy()
                else:
                    tb = pr.read_excel(full_path, sheet_name="Data")
                    # Rename columns for other files
                    tb = tb.rename(
                        columns={
                            "reporter_area_label": "country",
                            "indicator_label": "indicator",
                        }
                    )
                    # Select only the columns we need
                    tb = tb[["country", "year", "indicator", "value"]].copy()

                # Add category prefix to indicator
                tb["indicator"] = category + "-" + tb["indicator"].astype(str)

                # Drop rows with missing values
                tb = tb.dropna(subset=["value"])

                tbs.append(tb)

            except (KeyError, FileNotFoundError, ValueError) as e:
                # File not found in archive or sheet not found
                print(f"Warning: Could not process {file_path}: {e}")
                continue

    # Concatenate all the processed DataFrames
    tb = pr.concat(tbs, axis=0, ignore_index=True)

    # Convert 'value' to float
    tb["value"] = tb["value"].astype(float)

    # Pivot the Table to have 'indicator' as columns and 'value' as cell values
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    for col in tb.columns:
        tb[col].metadata.origins = snap.metadata.origin
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
