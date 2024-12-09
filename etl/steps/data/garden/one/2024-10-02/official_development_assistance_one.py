# NOTE: After December 2024 update, check the steps in `remove_data_for_most_recent_year`
"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define index columns
INDEX_SECTORS = ["donor_name", "recipient_name", "year", "sector_name"]
INDEX_CHANNELS = ["donor_name", "recipient_name", "year", "channel_name"]

# Define most recent year in the data
MOST_RECENT_YEAR = 2023

# Define mapping for sectors, including new names, sub-sectors, and sectors.
SECTORS_MAPPING = {
    "I.1.a. Education, Level Unspecified": {
        "new_name": "I.1.a. Education, level unspecified",
        "sub_sector": "I.1. Education",
        "sector": "I. Social infrastructure and services",
    },
    "I.1.b. Basic Education": {
        "new_name": "I.1.b. Basic education",
        "sub_sector": "I.1. Education",
        "sector": "I. Social infrastructure and services",
    },
    "I.1.c. Secondary Education": {
        "new_name": "I.1.c. Secondary education",
        "sub_sector": "I.1. Education",
        "sector": "I. Social infrastructure and services",
    },
    "I.1.d. Post-Secondary Education": {
        "new_name": "I.1.d. Post-secondary education",
        "sub_sector": "I.1. Education",
        "sector": "I. Social infrastructure and services",
    },
    "I.2.a. Health, General": {
        "new_name": "I.2.a. Health, general",
        "sub_sector": "I.2. Health",
        "sector": "I. Social infrastructure and services",
    },
    "I.2.b. Basic Health": {
        "new_name": "I.2.b. Basic health",
        "sub_sector": "I.2. Health",
        "sector": "I. Social infrastructure and services",
    },
    "I.2.c. Non-communicable diseases (NCDs)": {
        "new_name": "I.2.c. Non-communicable diseases (NCDs)",
        "sub_sector": "I.2. Health",
        "sector": "I. Social infrastructure and services",
    },
    "I.3. Population Policies/Programmes & Reproductive Health": {
        "new_name": "I.3. Population policies/programmes and reproductive health",
        "sub_sector": "I.3. Population policies/programmes and reproductive health",
        "sector": "I. Social infrastructure and services",
    },
    "I.4. Water Supply & Sanitation": {
        "new_name": "I.4. Water supply and sanitation",
        "sub_sector": "I.4. Water supply and sanitation",
        "sector": "I. Social infrastructure and services",
    },
    "I.5.a. Government & Civil Society-general": {
        "new_name": "I.5.a. Government and civil society (subcategory)",
        "sub_sector": "I.5. Government and civil society",
        "sector": "I. Social infrastructure and services",
    },
    "I.5.b. Conflict, Peace & Security": {
        "new_name": "I.5.b. Conflict, peace and security",
        "sub_sector": "I.5. Government and civil society",
        "sector": "I. Social infrastructure and services",
    },
    "I.6. Other Social Infrastructure & Services": {
        "new_name": "I.6. Other social infrastructure and services",
        "sub_sector": "I.6. Other social infrastructure and services",
        "sector": "I. Social infrastructure and services",
    },
    "II.1. Transport & Storage": {
        "new_name": "II.1. Transport and storage",
        "sub_sector": "II.1. Transport and storage",
        "sector": "II. Economic infrastructure and services",
    },
    "II.2. Communications": {
        "new_name": "II.2. Communications",
        "sub_sector": "II.2. Communications",
        "sector": "II. Economic infrastructure and services",
    },
    "II.3.a. Energy Policy": {
        "new_name": "II.3.a. Energy policy",
        "sub_sector": "II.3. Energy",
        "sector": "II. Economic infrastructure and services",
    },
    "II.3.b. Energy generation, renewable sources": {
        "new_name": "II.3.b. Energy generation, renewable sources",
        "sub_sector": "II.3. Energy",
        "sector": "II. Economic infrastructure and services",
    },
    "II.3.c. Energy generation, non-renewable sources": {
        "new_name": "II.3.c. Energy generation, non-renewable sources",
        "sub_sector": "II.3. Energy",
        "sector": "II. Economic infrastructure and services",
    },
    "II.3.d. Hybrid energy plants": {
        "new_name": "II.3.d. Hybrid energy plants",
        "sub_sector": "II.3. Energy",
        "sector": "II. Economic infrastructure and services",
    },
    "II.3.e. Nuclear energy plants": {
        "new_name": "II.3.e. Nuclear energy plants",
        "sub_sector": "II.3. Energy",
        "sector": "II. Economic infrastructure and services",
    },
    "II.3.f. Energy distribution": {
        "new_name": "II.3.f. Energy distribution",
        "sub_sector": "II.3. Energy",
        "sector": "II. Economic infrastructure and services",
    },
    "II.4. Banking & Financial Services": {
        "new_name": "II.4. Banking and financial services",
        "sub_sector": "II.4. Banking and financial services",
        "sector": "II. Economic infrastructure and services",
    },
    "II.5. Business & Other Services": {
        "new_name": "II.5. Business and other services",
        "sub_sector": "II.5. Business and other services",
        "sector": "II. Economic infrastructure and services",
    },
    "III.1.a. Agriculture": {
        "new_name": "III.1.a. Agriculture",
        "sub_sector": "III.1. Agriculture, forestry, fishing",
        "sector": "III. Production sectors",
    },
    "III.1.b. Forestry": {
        "new_name": "III.1.b. Forestry",
        "sub_sector": "III.1. Agriculture, forestry, fishing",
        "sector": "III. Production sectors",
    },
    "III.1.c. Fishing": {
        "new_name": "III.1.c. Fishing",
        "sub_sector": "III.1. Agriculture, forestry, fishing",
        "sector": "III. Production sectors",
    },
    "III.2.a. Industry": {
        "new_name": "III.2.a. Industry",
        "sub_sector": "III.2. Industry, mining, construction",
        "sector": "III. Production sectors",
    },
    "III.2.b. Mineral Resources & Mining": {
        "new_name": "III.2.b. Mineral resources and mining",
        "sub_sector": "III.2. Industry, mining, construction",
        "sector": "III. Production sectors",
    },
    "III.2.c. Construction": {
        "new_name": "III.2.c. Construction",
        "sub_sector": "III.2. Industry, mining, construction",
        "sector": "III. Production sectors",
    },
    "III.3.a. Trade Policies & Regulations": {
        "new_name": "III.3.a. Trade policies and regulations",
        "sub_sector": "III.3. Trade and tourism",
        "sector": "III. Production sectors",
    },
    "III.3.b. Tourism": {
        "new_name": "III.3.b. Tourism",
        "sub_sector": "III.3. Trade and tourism",
        "sector": "III. Production sectors",
    },
    "IV.1. General Environment Protection": {
        "new_name": "IV.1. General environment protection",
        "sub_sector": "IV.1. General environment protection",
        "sector": "IV. Multisector/cross-cutting",
    },
    "IV.2. Other Multisector": {
        "new_name": "IV.2. Other multisector",
        "sub_sector": "IV.2. Other multisector",
        "sector": "IV. Multisector/cross-cutting",
    },
    "VI.1. General Budget Support": {
        "new_name": "VI.1. General budget support",
        "sub_sector": "VI.1. General budget support",
        "sector": "VI. Commodity aid / General programme assistance",
    },
    "VI.2. Development Food Assistance": {
        "new_name": "VI.2. Development food assistance",
        "sub_sector": "VI.2. Development food assistance",
        "sector": "VI. Commodity aid / General programme assistance",
    },
    "VI.3. Other Commodity Assistance": {
        "new_name": "VI.3. Other commodity assistance",
        "sub_sector": "VI.3. Other commodity assistance",
        "sector": "VI. Commodity aid / General programme assistance",
    },
    "VII. Action Relating to Debt": {
        "new_name": "VII. Action relating to debt",
        "sub_sector": "VII. Action relating to debt",
        "sector": "VII. Action relating to debt",
    },
    "VIII.1. Emergency Response": {
        "new_name": "VIII.1. Emergency response",
        "sub_sector": "VIII.1. Emergency response",
        "sector": "VIII. Humanitarian aid",
    },
    "VIII.2. Reconstruction Relief & Rehabilitation": {
        "new_name": "VIII.2. Reconstruction relief and rehabilitation",
        "sub_sector": "VIII.2. Reconstruction relief and rehabilitation",
        "sector": "VIII. Humanitarian aid",
    },
    "VIII.3. Disaster Prevention & Preparedness": {
        "new_name": "VIII.3. Disaster prevention and preparedness",
        "sub_sector": "VIII.3. Disaster prevention and preparedness",
        "sector": "VIII. Humanitarian aid",
    },
    "IX. Unallocated / Unspecified": {
        "new_name": "IX. Unallocated / unspecified",
        "sub_sector": "IX. Unallocated / unspecified",
        "sector": "IX. Unallocated / unspecified",
    },
    "Sectors not specified": {
        "new_name": "IX. Unallocated / unspecified",
        "sub_sector": "IX. Unallocated / unspecified",
        "sector": "IX. Unallocated / unspecified",
    },
    "Administrative Costs of Donors": {
        "new_name": "Administrative costs of donors",
        "sub_sector": "Administrative costs of donors",
        "sector": "Administrative costs of donors",
    },
    "Refugees in Donor Countries": {
        "new_name": "Refugees in donor countries",
        "sub_sector": "Refugees in donor countries",
        "sector": "Refugees in donor countries",
    },
}

# Define main categories for sectors
SECTORS_MAIN_CATEGORIES = [
    "I. Social infrastructure and services",
    "II. Economic infrastructure and services",
    "III. Production sectors",
    "IV. Multisector/cross-cutting",
    "VI. Commodity aid / General programme assistance",
    "VII. Action relating to debt",
    "VIII. Humanitarian aid",
    "IX. Unallocated / unspecified",
    "Administrative costs of donors",
    "Refugees in donor countries",
]

# Define channel categories coming from ONE
CHANNEL_CATEGORIES = {
    "1": "Public sector",
    "2": "Non-governmental organization (NGO) and civil society",
    "3": "Public-private partnerships (PPP) and networks",
    "4": "Multilateral organizations",
    "5": "University, college or other teaching institution, research institute or think-tank",
    "6": "Private sector institutions",
    "9": "Unspecified",
}

# Define multiplier for values
VALUE_MULTIPLIER = 1_000_000


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("official_development_assistance_one")

    # Read tables from meadow dataset.
    tb_sectors = ds_meadow["sectors"].reset_index()
    tb_channels = ds_meadow["channels"].reset_index()

    #
    # Process data.
    #
    # Remove data for the most recent year.
    tb_sectors = remove_data_for_most_recent_year(tb=tb_sectors, year=MOST_RECENT_YEAR)
    tb_channels = remove_data_for_most_recent_year(tb=tb_channels, year=MOST_RECENT_YEAR)

    tb_sectors = geo.harmonize_countries(
        df=tb_sectors,
        country_col="donor_name",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )
    tb_sectors = geo.harmonize_countries(
        df=tb_sectors,
        country_col="recipient_name",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    tb_channels = geo.harmonize_countries(
        df=tb_channels,
        country_col="donor_name",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )
    tb_channels = geo.harmonize_countries(
        df=tb_channels,
        country_col="recipient_name",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    # Multiply value by VALUE_MULTIPLIER
    tb_sectors["value"] *= VALUE_MULTIPLIER
    tb_channels["value"] *= VALUE_MULTIPLIER

    # Rename sectors and add parent categories.
    tb_sectors = rename_sectors_and_add_parent_categories(tb=tb_sectors)

    # Add non-humanitarian aid and total sectors.
    tb_sectors = add_non_humanitarian_aid_and_total(tb=tb_sectors)

    # Rename channels in main categories and aggregate them.
    tb_channels = rename_and_aggregate_channels(tb=tb_channels)

    # Create donor-only tables (using recipient_name = "All recipients (OECD)") and recipient-only tables (using donor_name = "Official donors (OECD)").
    tb_sectors_donor = tb_sectors[tb_sectors["recipient_name"] == "All recipients (OECD)"].reset_index(drop=True)
    tb_sectors_recipient = tb_sectors[tb_sectors["donor_name"] == "Official donors (OECD)"].reset_index(drop=True)

    tb_channels_donor = tb_channels[tb_channels["recipient_name"] == "All recipients (OECD)"].reset_index(drop=True)
    tb_channels_recipient = tb_channels[tb_channels["donor_name"] == "Official donors (OECD)"].reset_index(drop=True)

    # Format tables.
    tb_sectors = tb_sectors.format(INDEX_SECTORS, short_name="sectors")
    tb_sectors_donor = tb_sectors_donor.format(INDEX_SECTORS, short_name="sectors_donor")
    tb_sectors_recipient = tb_sectors_recipient.format(INDEX_SECTORS, short_name="sectors_recipient")

    tb_channels = tb_channels.format(INDEX_CHANNELS, short_name="channels")
    tb_channels_donor = tb_channels_donor.format(INDEX_CHANNELS, short_name="channels_donor")
    tb_channels_recipient = tb_channels_recipient.format(INDEX_CHANNELS, short_name="channels_recipient")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[
            tb_sectors,
            tb_sectors_donor,
            tb_sectors_recipient,
            tb_channels,
            tb_channels_donor,
            tb_channels_recipient,
        ],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def rename_sectors_and_add_parent_categories(tb: Table) -> Table:
    """
    Rename sectors and add aggregations of the main and first level sub-sectors.
    """

    # Make value float
    tb["value"] = tb["value"].astype("float")

    # Make sector_name string
    tb["sector_name"] = tb["sector_name"].astype("string")

    # When sector_name is empty, set it to "Sectors not specified".
    tb["sector_name"] = tb["sector_name"].fillna("Sectors not specified")

    # Assert that all sector names are in the mapping
    assert set(tb["sector_name"].unique()).issubset(
        SECTORS_MAPPING.keys()
    ), f"Not all sector names are in the mapping: {set(tb['sector_name'].unique()) - set(SECTORS_MAPPING.keys())}"

    # Define three different tb_sectors tables: one with the original sector names (to rename), one with the subsectors and one with the sectors.
    tb_sectors = tb.copy()
    tb_subsectors = tb.copy()
    tb_sectors_main = tb.copy()

    # For tb_sectors, rename each category with new_name.
    for old_name, config in SECTORS_MAPPING.items():
        tb_sectors["sector_name"] = tb_sectors["sector_name"].replace(old_name, config["new_name"])

    # For tb_subsectors, rename each category with sub_sector.
    for old_name, config in SECTORS_MAPPING.items():
        tb_subsectors["sector_name"] = tb_subsectors["sector_name"].replace(old_name, config["sub_sector"])

    # Aggregate tb_subsectors by donor_name, recipient_name, year and sector_name.
    tb_subsectors = tb_subsectors.groupby(INDEX_SECTORS, observed=True, dropna=False)["value"].sum().reset_index()

    # For tb_sectors_main, rename each category with sector.
    for old_name, config in SECTORS_MAPPING.items():
        tb_sectors_main["sector_name"] = tb_sectors_main["sector_name"].replace(old_name, config["sector"])

    # Aggregate tb_sectors_main by donor_name, recipient_name, year and sector_name.
    tb_sectors_main = tb_sectors_main.groupby(INDEX_SECTORS, observed=True, dropna=False)["value"].sum().reset_index()

    # Concatenate the three tables.
    tb = pr.concat([tb_sectors, tb_subsectors, tb_sectors_main], ignore_index=True)

    # Remove duplicates.
    tb = tb.drop_duplicates(subset=INDEX_SECTORS)

    return tb


def add_non_humanitarian_aid_and_total(tb: Table) -> Table:
    """
    Add non-humanitarian aid and total sectors to the table
    """

    # Create a copy of the table
    tb_total = tb.copy()
    tb_humanitarian = tb.copy()

    # Filter tb_total by SECTORS_MAIN_CATEGORIES
    tb_total = tb_total[tb_total["sector_name"].isin(SECTORS_MAIN_CATEGORIES)].reset_index(drop=True)

    # Define INDEX_SECTORS without sector_name
    INDEX_WITHOUT_SECTOR_NAME = [col for col in INDEX_SECTORS if col != "sector_name"]

    # Aggregate all the data by INDEX_SECTORS except by sector_name
    tb_total = tb_total.groupby(INDEX_WITHOUT_SECTOR_NAME, observed=True, dropna=False)["value"].sum().reset_index()

    # Add "Total" as sector_name
    tb_total["sector_name"] = "Total"

    # For tb_humanitarian, keep only the rows with sector_name = "VIII. Humanitarian aid"
    tb_humanitarian = tb_humanitarian[tb_humanitarian["sector_name"] == "VIII. Humanitarian aid"].reset_index(drop=True)

    # Merge tb_total and tb_humanitarian
    tb_non_humanitarian = pr.merge(
        tb_total, tb_humanitarian, on=INDEX_WITHOUT_SECTOR_NAME, how="left", suffixes=("_total", "_humanitarian")
    )

    # Calculate value as the difference between value_total and value_humanitarian
    tb_non_humanitarian["value"] = tb_non_humanitarian["value_total"] - tb_non_humanitarian["value_humanitarian"]

    # Define sector_name as "Non-humanitarian aid"
    tb_non_humanitarian["sector_name"] = "Non-humanitarian aid"

    # Keep only the columns in INDEX_SECTORS and value
    tb_non_humanitarian = tb_non_humanitarian[INDEX_SECTORS + ["value"]]

    # Concatenate tb, tb_total, and tb_non_humanitarian
    tb = pr.concat([tb, tb_total, tb_non_humanitarian], ignore_index=True)

    return tb


def rename_and_aggregate_channels(tb: Table) -> Table:
    """
    Rename channels in main categories and aggregate them
    """

    # Make value float
    tb["value"] = tb["value"].astype("float")

    # Make channel_code string
    tb["channel_code"] = tb["channel_code"].astype("string")

    # When channel_code is empty, set it to "9".
    tb["channel_code"] = tb["channel_code"].fillna("9")

    # Create channel_first_digit column
    tb["channel_first_digit"] = tb["channel_code"].str[0]

    # Assert that all channel codes are in the mapping
    assert set(
        tb["channel_first_digit"].unique()
    ).issubset(
        CHANNEL_CATEGORIES.keys()
    ), f"Not all channel codes are in the mapping: {set(tb['channel_first_digit'].unique()) - set(CHANNEL_CATEGORIES.keys())}"

    # Create channel_name column as the mapping of channel_code
    tb["channel_name"] = tb["channel_first_digit"].replace(CHANNEL_CATEGORIES)

    # Aggregate tb by donor_name, recipient_name, year and channel_name.
    tb = tb.groupby(INDEX_CHANNELS, observed=True, dropna=False)["value"].sum().reset_index()

    return tb


def remove_data_for_most_recent_year(tb: Table, year: int) -> Table:
    """
    Remove data for the most recent year.
    """

    # Filter the table to remove the most recent year
    tb = tb[tb["year"] != year].reset_index(drop=True)

    return tb
