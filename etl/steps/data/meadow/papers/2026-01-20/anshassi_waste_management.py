"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Load snapshot and create meadow dataset."""
    # Load snapshot
    snap = paths.load_snapshot("anshassi_waste_management.xlsx")

    # Read the specific sheet with multi-level headers
    tb = snap.read(sheet_name="Country MSW Data 2", header=[0, 1])

    # Flatten the multi-level column names
    tb.columns = ["_".join(col).strip() if col[1] not in ["", col[0]] else col[1] for col in tb.columns]

    # Rename columns for clarity
    column_mapping = {
        "Unnamed: 0_level_0_Country": "country",
        "Unnamed: 1_level_0_2020 Income Level": "income_level",
        "Unnamed: 2_level_0_2016 & 2020 Region": "region",
        "Unnamed: 3_level_0_Final Classification": "final_classification",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Collection": "collected",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Open Dump Landfill": "collected_open_dump_landfill",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Controlled Landfill": "collected_controlled_landfill",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Sanitary Landfill": "collected_sanitary_landfill",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Open Air Burning": "collected_open_air_burning",
        "Final Waste Management Method (% of Total Mg Waste Generated)_MSWI Incineration": "collected_mswi_incineration",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Composting": "collected_composting",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Recycling": "collected_recycling",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Uncollected": "uncollected",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Uncollected- Open Dump": "uncollected_open_dump",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Uncollected- Controlled Landfill": "uncollected_controlled_landfill",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Uncollected- Sanitary Landfill": "uncollected_sanitary_landfill",
        "Final Waste Management Method (% of Total Mg Waste Generated)_Uncollected- Open Air Burning": "uncollected_open_air_burning",
    }

    tb = tb.rename(columns=column_mapping)

    # Select only the columns we need
    columns_to_keep = [
        "country",
        "income_level",
        "region",
        "final_classification",
        "collected",
        "uncollected",
        "collected_open_dump_landfill",
        "collected_controlled_landfill",
        "collected_sanitary_landfill",
        "collected_open_air_burning",
        "collected_mswi_incineration",
        "collected_composting",
        "collected_recycling",
        "uncollected_open_dump",
        "uncollected_controlled_landfill",
        "uncollected_sanitary_landfill",
        "uncollected_open_air_burning",
    ]

    tb = tb[columns_to_keep]

    # Format table
    tb = tb.format(["country"])
    # Create a new meadow dataset
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
