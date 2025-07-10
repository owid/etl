"""
Load a meadow dataset and create a garden dataset.

NOTE: To extract the log of the process (to review sanity checks, for example), follow these steps:
    1. Define LONG_FORMAT as True.
    2. Run the following command in the terminal:
        nohup uv run etl run government_at_a_glance > output_govt_glance.log 2>&1 &
"""

import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger
from tabulate import tabulate

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()


# Define units and their new names
UNITS = {
    "Percentage of GDP": "share_gdp",
    "Percentage of general government expenditure": "share_gov_exp",
    "Percentage of investment of total economy": "share_investment",
    "Percentage of potential GDP": "share_potential_gdp",
    "US dollars per person, PPP converted": "ppp",
    "US dollars, PPP converted": "ppp",
    "Growth rate": "growth_rate",
    "Percentage of general government outsourcing expenditures": "share_gov_outsourcing_exp",
    "Percentage of general government revenues": "share_gov_revenues",
    "Percentage of general government production costs": "share_gov_production_costs",
    "Percentage of tax revenue": "share_tax_revenue",
    "Percentage of general government expenditure in the same function": "share_gov_exp_same_function",
}


# Define new names for some of the indicators
INDICATORS = {
    "Real government expenditures per capita": "Government expenditure per capita",
    "Real government revenues per capita": "Government revenues per capita",
    "Real government debt per capita": "Government gross debt per capita",
}

# Define new names for functions
FUNCTIONS = {
    "Total": "Total | Total",
    "General public services": "General public services | Total",
    "Executive and legislative organs, financial and fiscal affairs, external affairs": "General public services | Executive and legislative organs, financial and fiscal affairs, external affairs",
    "Foreign economic aid": "General public services | Foreign economic aid",
    "General services": "General public services | General services",
    "Basic research": "General public services | Basic research",
    "R&D General public services": "General public services | R&D General public services",
    "General public services n.e.c.": "General public services | General public services not elsewhere classified",
    "Public debt transactions": "General public services | Public debt transactions",
    "Transfers of a general character between different levels of government": "General public services | Transfers of a general character between different levels of government",
    "Defence": "Defense | Total",
    "Public order and safety": "Public order and safety | Total",
    "Police services": "Public order and safety | Police services",
    "Fire-protection services": "Public order and safety | Fire-protection services",
    "Law courts": "Public order and safety | Law courts",
    "Prisons": "Public order and safety | Prisons",
    "R&D Public order and safety": "Public order and safety | R&D Public order and safety",
    "Public order and safety n.e.c.": "Public order and safety | Public order and safety not elsewhere classified",
    "Economic affairs": "Economic affairs | Total",
    "General economic, commercial and labour affairs": "Economic affairs | General economic, commercial and labour affairs",
    "Agriculture, forestry, fishing and hunting": "Economic affairs | Agriculture, forestry, fishing and hunting",
    "Fuel and energy": "Economic affairs | Fuel and energy",
    "Mining, manufacturing and construction": "Economic affairs | Mining, manufacturing and construction",
    "Transport": "Economic affairs | Transport",
    "Communication": "Economic affairs | Communication",
    "Other industries": "Economic affairs | Other industries",
    "R&D Economic affairs": "Economic affairs | R&D Economic affairs",
    "Economic affairs n.e.c.": "Economic affairs | Economic affairs not elsewhere classified",
    "Environmental protection": "Environmental protection | Total",
    "Waste management": "Environmental protection | Waste management",
    "Waste water management": "Environmental protection | Waste water management",
    "Protection of biodiversity and landscape": "Environmental protection | Protection of biodiversity and landscape",
    "Pollution abatement": "Environmental protection | Pollution abatement",
    "R&D Environmental protection": "Environmental protection | R&D Environmental protection",
    "Environmental protection n.e.c.": "Environmental protection | Environmental protection not elsewhere classified",
    "Housing and community amenities": "Housing and community amenities | Total",
    "Housing development": "Housing and community amenities | Housing development",
    "Community development": "Housing and community amenities | Community development",
    "Water supply": "Housing and community amenities | Water supply",
    "Street lighting": "Housing and community amenities | Street lighting",
    "R&D Housing and community amenities": "Housing and community amenities | R&D Housing and community amenities",
    "Housing and community amenities n.e.c.": "Housing and community amenities | Housing and community amenities not elsewhere classified",
    "Health": "Health | Total",
    "Medical products, appliances and equipment": "Health | Medical products, appliances and equipment",
    "Outpatient services": "Health | Outpatient services",
    "Hospital services": "Health | Hospital services",
    "Public health services": "Health | Public health services",
    "R&D Health": "Health | R&D Health",
    "Health n.e.c.": "Health | Health not elsewhere classified",
    "Recreation, culture and religion": "Recreation, culture and religion | Total",
    "Education": "Education | Total",
    "Pre-primary and primary education": "Education | Pre-primary and primary education",
    "Secondary education": "Education | Secondary education",
    "Post-secondary non-tertiary education": "Education | Post-secondary non-tertiary education",
    "Tertiary education": "Education | Tertiary education",
    "Education not definable by level": "Education | Education not definable by level",
    "Subsidiary services to education": "Education | Subsidiary services to education",
    "R&D Education": "Education | R&D Education",
    "Education n.e.c.": "Education | Education not elsewhere classified",
    "Social protection": "Social protection | Total",
    "Sickness and disability": "Social protection | Sickness and disability",
    "Old age": "Social protection | Old age",
    "Survivors": "Social protection | Survivors",
    "Family and children": "Social protection | Family and children",
    "Unemployment": "Social protection | Unemployment",
    "Housing": "Social protection | Housing",
    "Social exclusion n.e.c.": "Social protection | Social exclusion not elsewhere classified",
    "R&D Social protection": "Social protection | R&D Social protection",
    "Social protection n.e.c.": "Social protection | Social protection not elsewhere classified",
}

# Define table index
INDEX = ["country", "year", "unit", "economic_transaction", "function", "function_subcategory"]

# Add tolerance check for spending by function
TOLERANCE_CHECK = 0.5

# Set table format when printing
TABLEFMT = "pretty"

# Set debug mode
DEBUG = False


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("government_at_a_glance")

    # Read tables from meadow
    tb_public_finance = ds_meadow.read("public_finance")
    tb_size_public_procurement = ds_meadow.read("size_public_procurement")
    tb_public_finance_economic_transaction = ds_meadow.read("public_finance_economic_transaction")
    tb_public_finance_by_function = ds_meadow.read("public_finance_by_function")

    #
    # Process data.
    #
    # Concatenate tables.
    tb = pr.concat(
        [
            tb_public_finance,
            tb_size_public_procurement,
            tb_public_finance_economic_transaction,
            tb_public_finance_by_function,
        ],
        ignore_index=True,
    )

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    # When unit_multiplier is "Millions", multiply value by 1,000,000.
    tb.loc[tb["unit_multiplier"] == "Millions", "value"] *= 1_000_000

    # Check if all unit keys are in the dataset.
    assert set(tb["unit"].unique()) == set(
        UNITS.keys()
    ), f"Some unit keys are not in the dataset: {set(tb['unit'].unique())- set(UNITS.keys())}".format()

    # Rename unit column.
    tb["unit"] = tb["unit"].map(UNITS)

    # Rename some of the indicators.
    tb["indicator"] = tb["indicator"].map(INDICATORS).fillna(tb["indicator"])

    # Check if all the functions are in the dataset, excluding NA values.
    assert (
        set(tb["function"].dropna().unique()) == set(FUNCTIONS.keys())
    ), f"Some function keys are not in the dataset: {set(tb['function'].dropna().unique()) - set(FUNCTIONS.keys())}".format()

    # Rename function column.
    tb["function"] = tb["function"].map(FUNCTIONS)

    # Create two columns from function, function_category and function_subcategory, by splitting with "|".
    tb[["function", "function_subcategory"]] = tb["function"].str.split("|", expand=True)
    tb["function"] = tb["function"].str.strip()
    tb["function_subcategory"] = tb["function_subcategory"].str.strip()

    # Make table wide, using indicator as columns.
    tb = tb.pivot(
        index=INDEX,
        columns=["indicator"],
        values="value",
        join_column_levels_with="_",
    ).reset_index(drop=True)

    sanity_checks(tb)

    # Improve table format.
    tb = tb.format(
        INDEX,
        short_name=paths.short_name,
    )

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_checks(tb: Table) -> None:
    """
    We check if the percentages sum to 100% (in the case of spending by function) or if there are any negative values.
    """

    tb = tb.copy()

    # NEGATIVE VALUES
    # Define mask
    for column in [
        col
        for col in tb.columns
        if col
        not in INDEX
        + [
            "Government fiscal balance",
            "Government gross debt per capita",
            "Government net interest spending",
            "Government primary balance",
            "Government revenues per capita",
            "Government structural balance",
            "Government structural primary balance",
        ]
    ]:
        mask = (tb[column] < 0) & (tb["unit"] != "growth_rate")

        tb_error = tb[mask].reset_index(drop=True)

        if not tb_error.empty:
            if DEBUG:
                log.warning(
                    f"""{len(tb_error)} observations of {column} are negative:
                        {tabulate(tb_error[INDEX + [column]], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
                )

    # SPENDING BY FUNCTION ADDING UP TO 100%
    # Exclude rows with function == "Total"
    tb_function = tb[tb["function"] != "Total"].reset_index(drop=True)

    # Keep only the rows with function_subcategory == "Total"
    tb_function = tb_function[tb_function["function_subcategory"] == "Total"].reset_index(drop=True)

    # Keep only the unit share_gov_exp
    tb_function = tb_function[tb_function["unit"] == "share_gov_exp"].reset_index(drop=True)

    # Calculate the sum of the values for each "country", "year"
    tb_sum = tb_function.groupby(["country", "year"]).sum().reset_index()

    # Create a mask
    mask = (tb_sum["Government expenditure by function"] < 100 + TOLERANCE_CHECK) & (
        tb_sum["Government expenditure by function"] > 100 - TOLERANCE_CHECK
    )

    tb_error = tb_sum[~mask].reset_index(drop=True)
    if not tb_error.empty:
        if DEBUG:
            log.warning(
                f"""{len(tb_error)} observations of Government expenditure by function do not add up to 100%:
                    {tabulate(tb_error[["country", "year", "Government expenditure by function"]], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
            )

    return None
