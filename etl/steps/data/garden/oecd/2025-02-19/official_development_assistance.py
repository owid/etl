# NOTE: After December 2024 update, check the steps in `remove_jumps_in_the_data_and_unneeded_cols`
"""Load a meadow dataset and create a garden dataset."""

from typing import List

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define conversions for columns.
TO_MILLION = 1e6

# categories to keep
CATEGORIES = {
    "dac1": {
        "aid_type": {
            "ODA, bilateral total": {"new_name": "oda_bilateral"},
            "ODA, multilateral total": {"new_name": "oda_multilateral"},
            "I.A. Bilateral Official Development Assistance by types of aid (1+2+3+4+5+6+7+8+9+10)": {
                "new_name": "oda_bilateral_2",
            },
            "I.B. Multilateral Official Development Assistance (capital subscriptions are included with grants)": {
                "new_name": "oda_multilateral_2",
            },
            "Official Development Assistance, grant equivalent measure": {
                "new_name": "oda",
            },
            "TOTAL FLOWS % GNI": {"new_name": "flows_share_gni"},
            "TOTAL OFFICIAL AND PRIVATE FLOWS (I+II+III+IV+V)": {
                "new_name": "flows",
            },
            "I. Official Development Assistance (ODA) (I.A + I.B)": {
                "new_name": "i_oda",
            },
            "II. Other Official Flows (OOF)": {"new_name": "ii_oof"},
            "III. Officially Supported Export Credits": {
                "new_name": "iii_officially_supported_export_credits",
            },
            "IV. Private Flows at Market Terms": {
                "new_name": "iv_private_flows_market_terms",
            },
            "V. Net Private Grants (V.1 minus V.2)": {"new_name": "v_net_private_grants"},
            "I.A.1. Budget support": {"new_name": "i_a_1_budget_support"},
            "I.A.2. Bilateral core contributions & pooled programmes & funds": {
                "new_name": "i_a_2_bilateral_core_contributions_pooled_programmes_funds",
            },
            "I.A.3. Project-type interventions": {"new_name": "i_a_3_project_type_interventions"},
            "I.A.4. Experts and other technical assistance": {
                "new_name": "i_a_4_experts_other_technical_assistance",
            },
            "I.A.5. Scholarships and student costs in donor countries": {
                "new_name": "i_a_5_scholarships_student_costs_donor_countries",
            },
            "I.A.6. Debt relief": {"new_name": "i_a_6_debt_relief"},
            "I.A.7. Administrative costs not included elsewhere": {
                "new_name": "i_a_7_administrative_costs_not_included_elsewhere",
            },
            "I.A.8. Other in-donor expenditures": {"new_name": "i_a_8_other_in_donor_expenditures"},
            "I.A.8.1. Development awareness": {"new_name": "i_a_8_1_development_awareness"},
            "I.A.8.2. Refugees in donor countries": {"new_name": "i_a_8_2_refugees_in_donor_countries"},
            "I.A.8.2. Refugees in donor countries, of which: Support to refugees and asylum seekers in other provider countries": {
                "new_name": "i_a_8_2_refugees_other_provider_countries",
            },
            "I.A.9. Recoveries on bilateral ODA grants / negative commitments": {
                "new_name": "i_a_9_recoveries_bilateral_oda_grants_negative_commitments",
            },
            "I.A.10. Other loans repayments": {"new_name": "i_a_10_other_loans_repayments"},
            "I.A.11. Private sector instruments": {"new_name": "i_a_11_private_sector_instruments"},
            "I.A.12. Other ODA not assigned to the above categories (historical series)": {
                "new_name": "i_a_12_other_oda_not_assigned_to_the_above_categories_historical_series",
            },
            "Population": {"new_name": "population"},
            "GNI": {"new_name": "gni"},
        },
        "fund_flows": {
            "Net Disbursements": {"new_name": "net_disbursements"},
            "Grant equivalents": {"new_name": "grant_equivalents"},
        },
    },
    "dac2a": {
        "aid_type": {
            "ODA: Total Net": {"new_name": "oda"},
            "ODA as % GNI (Recipient)": {"new_name": "oda_share_gni"},
            "Grants, Total": {"new_name": "grants"},
            "ODA Loans: Total Net": {"new_name": "loans"},
            "Capital Subscriptions - Deposits": {"new_name": "capital_subscriptions_deposits"},
            "Recoveries": {"new_name": "recoveries"},
            "Development Food Aid": {"new_name": "development_food_aid"},
            "Humanitarian Aid": {"new_name": "humanitarian_aid"},
            "Technical Cooperation": {"new_name": "technical_cooperation"},
        },
    },
    "dac5": {
        "aid_type": {
            "Total ODA": {"new_name": "oda_by_sector"},
        },
    },
}

# Define indices for pivot tables.
INDICES = {
    "dac1": ["country", "year"],
    "dac2a": ["country", "year", "donor"],
    "dac5": ["country", "year", "sector"],
}

# Define type of donors to include from the recipient dataset.
DONORS_TOTALS = {
    "DAC countries (OECD)": "DAC countries",
    "Non-DAC countries (OECD)": "Non-DAC countries",
    "Multilateral organizations (OECD)": "Multilateral organizations",
    "Private donors (OECD)": "Private donors",
}

# Define official donors aggregation
OFFICIAL_DONORS = {"Official donors (OECD)": "Official donors"}

# Define sectors to include from the DAC5 dataset.
SECTORS_DAC5 = {
    "I. Social Infrastructure & Services": "Social infrastructure and services",
    "II. Economic Infrastructure & Services": "Economic infrastructure and services",
    "III. Production Sectors": "Production sectors",
    "IV. Multi-Sector / Cross-Cutting": "Multi-sector / Cross-cutting",
    "V. Total Sector Allocable (I+II+III+IV)": "Total sector allocable",
    "VI. Commodity Aid / General Programme Assistance": "Commodity aid / General programme assistance",
    "VII. Action Relating to Debt": "Action relating to debt",
    "VIII. Humanitarian Aid": "Humanitarian aid",
    "IX. Unallocated / Unspecified": "Unallocated / Unspecified",
    "Total (V+VI+VII+VIII+IX)": "Total",
    "I.1. Education": "Education",
    "I.2. Health": "Health",
    "I.3. Population Policies/Programmes & Reproductive Health": "Population policies/programmes and reproductive health",
    "I.4. Water Supply & Sanitation": "Water supply and sanitation",
    "I.5. Government & Civil Society": "Government and civil society",
    "I.5.a. Government & Civil Society-general": "Government and civil society (subcategory)",
    "I.5.b. Conflict, Peace & Security": "Conflict, peace and security",
    "I.6. Other Social Infrastructure & Services": "Other social infrastructure and services",
    "II.1. Transport & Storage": "Transport and storage",
    "II.2. Communications": "Communications",
    "II.3. Energy": "Energy",
    "II.4. Banking & Financial Services": "Banking and financial services",
    "II.5. Business & Other Services": "Business and other services",
    "III.1. Agriculture, Forestry, Fishing": "Agriculture, forestry, fishing",
    "III.2. Industry, Mining, Construction": "Industry, mining, construction",
    "III.3.a. Trade Policies & Regulations": "Trade policies and regulations",
    "III.3.b. Tourism": "Tourism",
    "IV.1. General Environment Protection": "General environment protection",
    "IV.2. Other Multisector": "Other multisector",
    "VI.1. General Budget Support": "General budget support",
    "VI.2. Development Food Assistance": "Development food assistance",
    "VI.3. Other Commodity Assistance": "Other commodity assistance",
    "VIII.1. Emergency Response": "Emergency response",
    "VIII.2. Reconstruction Relief & Rehabilitation": "Reconstruction relief and rehabilitation",
    "VIII.3. Disaster Prevention & Preparedness": "Disaster prevention and preparedness",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("official_development_assistance")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb_dac1 = ds_meadow["dac1"].reset_index()
    tb_dac2a = ds_meadow["dac2a"].reset_index()
    tb_dac5 = ds_meadow["dac5"].reset_index()

    #
    # Process data.
    #
    tb_dac1 = geo.harmonize_countries(
        df=tb_dac1,
        country_col="donor",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )
    tb_dac2a = geo.harmonize_countries(
        df=tb_dac2a,
        country_col="recipient",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )
    tb_dac5 = geo.harmonize_countries(
        df=tb_dac5,
        country_col="donor",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    # Harmonize names of donors in tb_dac2a
    tb_dac2a = geo.harmonize_countries(
        df=tb_dac2a,
        country_col="donor",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    tb_dac1 = reformat_table_and_make_it_wide(
        tb=tb_dac1,
        short_name="dac1",
        columns_in_current_prices=["oda_share_gni", "flows_share_gni", "population"],
        recipient_or_donor="donor",
    )
    tb_dac2a = reformat_table_and_make_it_wide(
        tb=tb_dac2a,
        short_name="dac2a",
        columns_in_current_prices=["oda_share_gni"],
        recipient_or_donor="recipient",
    )
    tb_dac5 = reformat_table_and_make_it_wide(
        tb=tb_dac5,
        short_name="dac5",
        columns_in_current_prices=[],
        recipient_or_donor="donor",
    )

    tb_dac1 = create_indicators_per_capita(
        tb=tb_dac1, indicator_list=["i_oda_net_disbursements", "oda_grant_equivalents"]
    )

    tb_dac1 = create_indicators_as_share_of_gni(
        tb=tb_dac1, indicator_list=["i_oda_net_disbursements", "oda_grant_equivalents"]
    )

    tb_dac1 = remove_jumps_in_the_data_and_unneeded_cols(tb=tb_dac1)

    tb_dac1 = limit_grant_equivalents_from_2018_only(tb=tb_dac1)

    tb_dac1 = combine_net_and_grant_equivalents(tb=tb_dac1)

    tb_dac1 = add_oda_components_as_share_of_oda(
        tb=tb_dac1,
        subcomponent_list=[
            "i_a_5_scholarships_student_costs_donor_countries",
            "i_a_7_administrative_costs_not_included_elsewhere",
            "i_a_8_1_development_awareness",
            "i_a_8_2_refugees_in_donor_countries",
        ],
    )

    tb = add_donor_data_from_recipient_dataset(tb_donor=tb_dac1, tb_recipient=tb_dac2a)

    tb = add_recipient_dataset(tb=tb, tb_recipient=tb_dac2a)

    tb = add_aid_by_sector_donor_dataset(tb=tb, tb_sector=tb_dac5)

    tb = create_indicators_per_capita_owid_population(
        tb=tb,
        indicator_list=[
            "oda_recipient",
            "grants_recipient",
            "loans_recipient",
            "technical_cooperation_recipient",
            "development_food_aid_recipient",
            "humanitarian_aid_recipient",
            "oda_by_sector",
        ],
        ds_population=ds_population,
    )

    tb = tb.format(["country", "year", "donor", "sector"], short_name=paths.short_name)
    tb_dac2a = tb_dac2a.format(["country", "year", "donor"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_dac2a], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def reformat_table_and_make_it_wide(
    tb: Table, short_name: str, columns_in_current_prices: List[str], recipient_or_donor: str
) -> Table:
    """
    Filter the categories we want, reformat the category names, change units and make table wide.
    """
    tb = tb.copy()

    # If part is a column, select only part 1
    if "part" in tb.columns:
        tb = tb[tb["part"] == 1].reset_index(drop=True)

    for column in CATEGORIES[short_name].keys():
        # Filter categories
        tb = tb[tb[column].isin(CATEGORIES[short_name][column].keys())].reset_index(drop=True)

        # Make columns string
        tb[column] = tb[column].astype("string")

        for category, config in CATEGORIES[short_name][column].items():
            # Rename categories
            tb.loc[tb[column] == category, column] = config["new_name"]

    # Drop missing values.
    tb = tb.dropna(subset=["value"]).reset_index(drop=True)

    # Keep only amounttype = "D". Also keep "A" only for columns_in_current_units.
    # D means "constant prices" and A means "current prices".
    tb = tb[
        (tb["amounttype"] == "D") | ((tb["amounttype"] == "A") & tb["aid_type"].isin(columns_in_current_prices))
    ].reset_index(drop=True)

    # Change units when amounttype is "D" (constant prices).
    tb.loc[tb["amounttype"] == "D", "value"] = tb["value"] * TO_MILLION

    # Also change units if aid_type is population
    tb.loc[tb["aid_type"] == "population", "value"] = tb["value"] * TO_MILLION

    # Rename donor or recipient to country
    tb = tb.rename(columns={recipient_or_donor: "country"})

    # Make table wide
    tb = tb.pivot(
        index=INDICES[short_name],
        columns=CATEGORIES[short_name].keys(),
        values="value",
        join_column_levels_with="_",
    ).reset_index(drop=True)

    return tb


def create_indicators_per_capita(tb: Table, indicator_list: List[str]) -> Table:
    """
    Create indicators per capita for ODA (flows and grant equivalent).
    This function uses the population data from the OECD Data Explorer.
    """

    for indicator in indicator_list:
        tb[f"{indicator}_per_capita"] = tb[indicator] / tb["population_net_disbursements"]

    # Drop population column
    tb = tb.drop(columns="population_net_disbursements")

    return tb


def create_indicators_as_share_of_gni(tb: Table, indicator_list: List[str]) -> Table:
    """
    Create indicators as share of GNI for ODA (flows and grant equivalent).
    I do this because the official figures for net disbursements end in 2017 and the grant equivalents continue from 2018.
    We want both series as complete as possible.
    """

    for indicator in indicator_list:
        tb[f"{indicator}_share_gni"] = tb[indicator] / tb["gni_net_disbursements"] * 100

    # Drop GNI column
    tb = tb.drop(columns="gni_net_disbursements")

    return tb


def add_donor_data_from_recipient_dataset(tb_donor: Table, tb_recipient: Table) -> Table:
    """
    There are additional donor entities in the recipient dataset that are not in the donor dataset.
    This function adds the missing donor data to the donor dataset.
    """

    tb_donor = tb_donor.copy()
    tb_recipient = tb_recipient.copy()

    # Select columns in tb_recipient
    tb_recipient = tb_recipient[
        ["country", "year", "donor", "oda", "grants", "loans", "capital_subscriptions_deposits", "recoveries"]
    ]

    # Select 'All Recipients, Total' in country column
    tb_recipient = tb_recipient[tb_recipient["country"] == "All recipients (OECD)"].reset_index(drop=True)

    # Drop country
    tb_recipient = tb_recipient.drop(columns="country")

    # Rename donor to country
    tb_recipient = tb_recipient.rename(columns={"donor": "country"})

    # Create two tables, one with ODA and another with grants and loans
    tb_recipient_oda = tb_recipient[["country", "year", "oda"]].reset_index(drop=True).copy()
    tb_recipient_grants_loans = (
        tb_recipient[["country", "year", "grants", "loans", "capital_subscriptions_deposits", "recoveries"]]
        .reset_index(drop=True)
        .copy()
    )

    # Merge tables. I don't want to use entities not in the donors dataset for the grants and loans, so I use left join.
    tb = pr.merge(tb_donor, tb_recipient_grants_loans, on=["country", "year"], how="left")
    tb = pr.merge(tb, tb_recipient_oda, on=["country", "year"], how="outer")

    # Use oda for country Multilaterals, Total
    tb.loc[tb["country"] == "Multilateral organizations (OECD)", "i_oda_net_disbursements"] = tb["oda"]

    # When i_oda_net_disbursements is missing, fill with oda
    tb["i_oda_net_disbursements_multilaterals_private_grants"] = tb["i_oda_net_disbursements"].fillna(tb["oda"])

    # Drop oda
    tb = tb.drop(columns="oda")

    return tb


def add_recipient_dataset(tb: Table, tb_recipient: Table) -> Table:
    """
    Add recipient data, combining these donor categories: 'DAC Countries, Total', 'Non-DAC Countries, Total', 'Multilaterals, Total', 'Private Donors, Total'
    """

    tb_recipient = tb_recipient.copy()

    # Assert if the donor categories are in the recipient dataset
    assert set(
        DONORS_TOTALS.keys()
    ).issubset(
        set(tb_recipient["donor"].unique())
    ), f"There are missing donor categories in the recipient dataset: {set(DONORS_TOTALS) - set(tb_recipient['donor'].unique())}"

    # Assert if the official donors aggregation is in the recipient dataset
    assert set(OFFICIAL_DONORS.keys()).issubset(
        set(tb_recipient["donor"].unique())
    ), f"The official donot aggregate set is not in the recipient dataset: {OFFICIAL_DONORS.keys()}"

    # Rename donor categories set in DONORS_TOTALS and OFFICIAL_DONORS
    tb_recipient["donor"] = tb_recipient["donor"].cat.rename_categories(DONORS_TOTALS)
    tb_recipient["donor"] = tb_recipient["donor"].cat.rename_categories(OFFICIAL_DONORS)

    # Create two tables, one with the donor categories and another with the official donors aggregation
    tb_donor_categories = tb_recipient[tb_recipient["donor"].isin(DONORS_TOTALS.values())].reset_index(drop=True).copy()
    tb_official_donor = tb_recipient[tb_recipient["donor"].isin(OFFICIAL_DONORS.values())].reset_index(drop=True).copy()

    # Define columns to sum
    cols = [col for col in tb_donor_categories.columns if col not in ["country", "year", "donor", "oda_share_gni"]]

    # Create a new table with the sum of all the columns not in country year donor and oda_share_gni in tb_donor_categories and name the colum donor as 'Total aid'
    # I set min_count to ensure that the sum is only calculated if all donors have data
    tb_donor_categories_grouped = tb_donor_categories.groupby(["country", "year"], as_index=False, observed=True)[
        cols
    ].sum(min_count=len(DONORS_TOTALS), numeric_only=True)
    tb_donor_categories_grouped = tb_donor_categories_grouped[
        [col for col in tb_donor_categories_grouped.columns if col not in ["oda_share_gni"]]
    ]
    tb_donor_categories_grouped["donor"] = "Total aid"

    # From tb_official_donor extract GNI data, by dividing oda by oda_share_gni
    tb_official_donor["gni"] = tb_official_donor["oda"] / (tb_official_donor["oda_share_gni"] / 100)

    # Use this data in tb_donors_categories_grouped
    tb_donor_categories_grouped = pr.merge(
        tb_donor_categories_grouped, tb_official_donor[["country", "year", "gni"]], on=["country", "year"], how="left"
    )

    # Calculate oda_share_gni
    tb_donor_categories_grouped["oda_share_gni"] = (
        tb_donor_categories_grouped["oda"] / tb_donor_categories_grouped["gni"] * 100
    )

    # Remove gni in tb_official_donor and tb_donor_categories_grouped
    tb_official_donor = tb_official_donor.drop(columns="gni")
    tb_donor_categories_grouped = tb_donor_categories_grouped.drop(columns="gni")

    # Concatenate all the tables
    tb_recipient = pr.concat([tb_donor_categories, tb_official_donor, tb_donor_categories_grouped], ignore_index=True)

    # Add the suffix _recipient to the columns in cols
    tb_recipient = tb_recipient.rename(columns={col: f"{col}_recipient" for col in cols + ["oda_share_gni"]})

    # Create donor column for tb
    tb["donor"] = None

    # Merge tables
    tb = pr.merge(tb, tb_recipient, on=["country", "year", "donor"], how="outer")

    return tb


def create_indicators_per_capita_owid_population(tb: Table, indicator_list: List[str], ds_population: Dataset) -> Table:
    """
    Create indicators per capita for the recipient indicators.
    The per capita values available in the OECD Data Explorer are in current prices, so we want to use the constant values.
    The dataset does not include population data for recipient countries, so I am using the OWID population dataset.
    """

    tb = tb.copy()

    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)

    for indicator in indicator_list:
        tb[f"{indicator}_per_capita"] = tb[indicator] / tb["population"]

    # Drop population column
    tb = tb.drop(columns="population")

    return tb


def add_aid_by_sector_donor_dataset(tb: Table, tb_sector: Table) -> Table:
    """
    Add sector data to the main dataset.
    This data comes from donor data in the DAC5 dataset.
    """

    tb_sector = tb_sector.copy()

    # Assert if the sectors are in the sector dataset
    assert set(SECTORS_DAC5.keys()).issubset(
        set(tb_sector["sector"].unique())
    ), f"There are missing sectors in the sector dataset: {set(SECTORS_DAC5) - set(tb_sector['sector'].unique())}"

    # Filter categories
    tb_sector = tb_sector[tb_sector["sector"].isin(SECTORS_DAC5.keys())].reset_index(drop=True)

    # Rename sectors set in SECTORS_DAC5
    tb_sector["sector"] = tb_sector["sector"].cat.rename_categories(SECTORS_DAC5)

    # Create a new table with "Total", "Emergency response", "Reconstruction relief and rehabilitation"
    tb_sector_humanitarian_aid = (
        tb_sector[tb_sector["sector"].isin(["Total", "Humanitarian aid"])].reset_index(drop=True).copy()
    )

    # Make the table wide
    tb_sector_humanitarian_aid = tb_sector_humanitarian_aid.pivot(
        index=["country", "year"], columns="sector", values="oda_by_sector", join_column_levels_with="_"
    ).reset_index(drop=True)

    # Create the column "Non-humanitarian aid" as the difference between "Total" and "Humanitarian aid"
    tb_sector_humanitarian_aid["Non-humanitarian aid"] = (
        tb_sector_humanitarian_aid["Total"] - tb_sector_humanitarian_aid["Humanitarian aid"]
    )

    # Make the table long again only using the columns "country", "year", "Emergency humanitarian aid" and "Non-emergency aid"
    tb_sector_humanitarian_aid = tb_sector_humanitarian_aid.melt(
        id_vars=["country", "year"],
        value_vars=["Non-humanitarian aid"],
        var_name="sector",
        value_name="oda_by_sector",
    )

    # Concatenate the sector tables
    tb_sector = pr.concat([tb_sector, tb_sector_humanitarian_aid], ignore_index=True)

    # Create an empty sector column in tb
    tb["sector"] = None

    # Merge tables
    tb = pr.merge(tb, tb_sector, on=["country", "year", "sector"], how="outer")

    return tb


def remove_jumps_in_the_data_and_unneeded_cols(tb: Table) -> Table:
    """
    Remove jumps in the data generated by own calculation.
    This is most likely because of aggregations of population and GNI not properly done by the source.
    This is a temporary solution until the source fixes the data. It is already reported.

    Also, remove redundant columns.
    """

    # For i_oda_net_disbursements_share_gni
    tb.loc[
        (tb["country"] == "Non-DAC countries (OECD)") & (tb["year"] <= 1991),
        "i_oda_net_disbursements_share_gni",
    ] = None

    # For i_oda_net_disbursements_per_capita
    tb.loc[
        (tb["country"] == "Non-DAC countries (OECD)") & (tb["year"] == 2007), "i_oda_net_disbursements_per_capita"
    ] = None

    # Remove columns
    tb = tb.drop(
        columns=["oda_bilateral_2_grant_equivalents", "oda_multilateral_2_grant_equivalents", "i_oda_grant_equivalents"]
    )

    return tb


def limit_grant_equivalents_from_2018_only(tb: Table) -> Table:
    """
    Limit grant equivalent indicators from year 2018 onwards.
    """

    tb = tb.copy()

    # Define grant equivalent indicators by looking at all the columns containing the word "grant_equivalents"
    grant_equivalent_indicators = [col for col in tb.columns if "grant_equivalents" in col]

    tb.loc[tb["year"] < 2018, grant_equivalent_indicators] = None

    return tb


def combine_net_and_grant_equivalents(tb: Table) -> Table:
    """
    Combine net disbursements and grant equivalent estimates into a single column.
    This is because the official figures consider net disbursements until 2017 and grant equivalents from 2018.
    """

    tb = tb.copy()

    # Start with grant equivalents
    tb["oda_official_estimate_share_gni"] = tb["oda_grant_equivalents_share_gni"]

    # Fill with net disbursements before 2018
    tb.loc[tb["year"] < 2018, "oda_official_estimate_share_gni"] = tb["i_oda_net_disbursements_share_gni"]

    return tb


def add_oda_components_as_share_of_oda(tb: Table, subcomponent_list: List[str]) -> Table:
    """
    Divide some of the ODA components by the total ODA to get the share of each component.
    Add also the total of these components.
    """

    for subcomponent in subcomponent_list:
        tb[f"{subcomponent}_net_disbursements_share_oda"] = (
            tb[f"{subcomponent}_net_disbursements"] / tb["i_oda_net_disbursements"] * 100
        )

        tb[f"{subcomponent}_grant_equivalents_share_oda"] = (
            tb[f"{subcomponent}_grant_equivalents"] / tb["oda_grant_equivalents"] * 100
        )

    # Also calculate the sum of these components
    tb["oda_indonor_net_disbursements"] = tb[
        [f"{subcomponent}_net_disbursements" for subcomponent in subcomponent_list]
    ].sum(axis=1)

    tb["oda_indonor_net_disbursements_share_oda"] = (
        tb["oda_indonor_net_disbursements"] / tb["i_oda_net_disbursements"] * 100
    )

    tb["oda_indonor_grant_equivalents"] = tb[
        [f"{subcomponent}_grant_equivalents" for subcomponent in subcomponent_list]
    ].sum(axis=1)

    tb["oda_indonor_grant_equivalents_share_oda"] = (
        tb["oda_indonor_grant_equivalents"] / tb["oda_grant_equivalents"] * 100
    )

    return tb
