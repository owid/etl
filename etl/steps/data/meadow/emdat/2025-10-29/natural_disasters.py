"""Load snapshot of EM-DAT natural disasters data and prepare a table with basic metadata."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to extract from raw data, and how to rename them.
COLUMNS = {
    "Country": "country",
    "Disaster Group": "group",
    "Disaster Subgroup": "subgroup",
    "Disaster Type": "type",
    "Disaster Subtype": "subtype",
    "Event Name": "event",
    "Region": "region",
    "Total Deaths": "total_dead",
    "No. Injured": "injured",
    "No. Affected": "affected",
    "No. Homeless": "homeless",
    "Total Affected": "total_affected",
    "Reconstruction Costs ('000 US$)": "reconstruction_costs",
    "Insured Damage ('000 US$)": "insured_damages",
    "Total Damage ('000 US$)": "total_damages",
    "Start Year": "start_year",
    "Start Month": "start_month",
    "Start Day": "start_day",
    "End Year": "end_year",
    "End Month": "end_month",
    "End Day": "end_day",
    # Column Consumer Price Index (CPI) is kept for the analysis on the share of small, medium and large events.
    "CPI": "cpi",
    # Column Entry Date is kept for the analysis on the share of small, medium and large events.
    "Entry Date": "entry_date",
}


def run(dest_dir: str) -> None:
    #
    # Load and process inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot("natural_disasters.xlsx")
    tb = snap.read(safe_types=False, sheet_name="EM-DAT Data")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Sanity check.
    error = "Expected only 'Natural' in 'group' column."
    assert set(tb["group"]) == set(["Natural"]), error

    # Set an appropriate index and sort conveniently.
    # NOTE: There are multiple rows for certain country-years. This will be handled in the garden step.
    tb = tb.format(keys=["country", "start_year"], verify_integrity=False, sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
