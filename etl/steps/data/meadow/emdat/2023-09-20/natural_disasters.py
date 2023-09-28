"""Load snapshot of EM-DAT natural disasters data and prepare a table with basic metadata.

"""
import warnings

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to extract from raw data, and how to rename them.
COLUMNS = {
    "Country": "country",
    "Year": "year",
    "Disaster Group": "group",
    "Disaster Subgroup": "subgroup",
    "Disaster Type": "type",
    "Disaster Subtype": "subtype",
    "Disaster Subsubtype": "subsubtype",
    "Event Name": "event",
    "Region": "region",
    "Continent": "continent",
    "Total Deaths": "total_dead",
    "No Injured": "injured",
    "No Affected": "affected",
    "No Homeless": "homeless",
    "Total Affected": "total_affected",
    "Reconstruction Costs ('000 US$)": "reconstruction_costs",
    "Insured Damages ('000 US$)": "insured_damages",
    "Total Damages ('000 US$)": "total_damages",
    "Start Year": "start_year",
    "Start Month": "start_month",
    "Start Day": "start_day",
    "End Year": "end_year",
    "End Month": "end_month",
    "End Day": "end_day",
}


def run(dest_dir: str) -> None:
    #
    # Load and process inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot("natural_disasters.xlsx")
    with warnings.catch_warnings(record=True):
        tb = snap.read(sheet_name="emdat data", skiprows=6)

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS)

    # Sanity check.
    error = "Expected only 'Natural' in 'group' column."
    assert set(tb["group"]) == set(["Natural"]), error

    # Set an appropriate index and sort conveniently.
    # NOTE: There are multiple rows for certain country-years. This will be handled in the garden step.
    tb = tb.set_index(["country", "year"], verify_integrity=False).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
