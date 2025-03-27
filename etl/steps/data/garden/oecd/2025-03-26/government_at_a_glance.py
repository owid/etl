"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Define units and their new names
UNITS = {
    "Percentage of GDP": "share_gdp",
    "Percentage of general government expenditure": "share_gov_exp",
    "Percentage of investment of total economy": "share_investment",
    "Percentage of potential GDP": "share_potential_gdp",
    "US dollars per person, PPP converted": "ppp",
    "US dollars, PPP converted": "ppp",
    "Growth rate": "growth_rate",
}

# Define new names for some of the indicators
INDICATORS = {
    "Real government expenditures per capita": "Government expenditure per capita",
    "Real government revenues per capita": "Government revenues per capita",
    "Real government debt per capita": "Government gross debt per capita",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("government_at_a_glance")

    # Read tables from meadow
    tb_public_finance = ds_meadow.read("public_finance")
    tb_size_public_procurement = ds_meadow.read("size_public_procurement")

    #
    # Process data.
    #
    # Concatenate tables.
    tb = pr.concat([tb_public_finance, tb_size_public_procurement], ignore_index=True)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # When unit_multiplier is "Millions", multiply value by 1,000,000.
    tb.loc[tb["unit_multiplier"] == "Millions", "value"] *= 1_000_000

    # Check if all unit keys are in the dataset.
    assert set(tb["unit"].unique()) == set(
        UNITS.keys()
    ), f"Some unit keys are not in the dataset: {set(UNITS.keys()) - set(tb['unit'].unique())}".format()

    # Rename unit column.
    tb["unit"] = tb["unit"].map(UNITS)

    # Rename some of the indicators.
    tb["indicator"] = tb["indicator"].map(INDICATORS).fillna(tb["indicator"])

    # Make table wide, using indicator as columns.
    tb = tb.pivot(
        index=["country", "year", "unit"], columns=["indicator"], values="value", join_column_levels_with="_"
    ).reset_index(drop=True)

    # Improve table format.
    tb = tb.format(["country", "year", "unit"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
