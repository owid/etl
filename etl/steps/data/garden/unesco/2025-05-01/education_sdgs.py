"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table, VariableMeta

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("education_sdgs")
    ds_expenditure = paths.load_dataset("public_expenditure")

    # Read table from meadow dataset.
    tb = ds_meadow.read("education_sdgs")

    # Load historical expenditure data
    tb_expenditure = ds_expenditure.read("public_expenditure")

    # Retrieve snapshot with the metadata provided via World Bank.

    snap_wb = paths.load_snapshot("edstats_metadata.xls")
    tb_wb = snap_wb.read()

    #
    # Process data.
    #
    country_mapping_path = paths.directory / "education.countries.json"
    excluded_countries_path = paths.directory / "education.excluded_countries.json"
    tb = geo.harmonize_countries(
        df=tb, countries_file=country_mapping_path, excluded_countries_file=excluded_countries_path
    )
    # Drop columns that are not needed
    tb = tb.drop(columns=["magnitude", "qualifier"])

    # Add the long description from the World Bank metadata
    long_definition = {}
    for indicator in tb["indicator_label_en"].unique():
        definition = tb_wb[tb_wb["Indicator Name"] == indicator]["Long definition"].values
        if len(definition) > 0:
            long_definition[indicator] = definition[0]
        else:
            long_definition[indicator] = ""

    tb["long_description"] = tb["indicator_label_en"].map(long_definition)
    # Drop rows with missing indicator labels
    tb = tb[tb["indicator_label_en"].notna()]
    tb["indicator_label_en"] = tb["indicator_label_en"].astype(str) + ", " + tb["indicator_id"].astype(str)

    # # Pivot the table to have the indicators as columns to add descriptions from producer
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")

    for column in tb_pivoted.columns:
        meta = tb_pivoted[column].metadata
        meta.display = {}
        long_definition = tb["long_description"].loc[tb["indicator_label_en"] == column]
        if not long_definition.empty:
            long_definition = long_definition.iloc[0]
            meta.description_from_producer = long_definition
            meta.title = column
        #
        # Update metadata units, short_units and number of decimal places to display depending on what keywords the variable name contains
        #
        # Fill out units and decimal places
        if "%" in column.lower():
            update_metadata(meta, display_decimals=1, unit="%", short_unit="%")
        elif "(days)" in column.lower():
            update_metadata(meta, display_decimals=1, unit="days", short_unit="")
        elif "index" in column.lower():
            update_metadata(meta, display_decimals=1, unit="index", short_unit="")
        elif "(current us$)" in column.lower():
            update_metadata(meta, display_decimals=1, unit="current US$", short_unit="$")
        elif "(number)" in column.lower():
            update_metadata(meta, display_decimals=0, unit="number", short_unit="")
        elif "ppp$" in column.lower():
            update_metadata(meta, display_decimals=1, unit="constant 2019 US$", short_unit="$")

        else:
            # Default metadata update when no other conditions are met.
            update_metadata(meta, 0, " ", " ")

    tb_pivoted = tb_pivoted.reset_index()

    tb_pivoted = tb_pivoted.format(["country", "year"])
    # Combine recent literacy estimates and expenditure data with historical estimates from a migrated dataset
    tb_pivoted = combine_historical_expenditure(tb_pivoted, tb_expenditure)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_pivoted], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_historical_expenditure(tb: Table, tb_expenditure: Table) -> Table:
    """
    Merge historical and recent expenditure data into a single Table.

    This function combines data from a Table containing historical public expenditure on education
    with a primary Table. The function handles missing data by favoring recent data; if this is not available,
    it falls back to historical data, which could also be missing (NaN).

    """
    tb = tb.reset_index()

    # Historical expenditure data
    historic_expenditure = tb_expenditure[
        ["year", "country", "public_expenditure_on_education__tanzi__and__schuktnecht__2000"]
    ].copy()

    # Recent public expenditure from main table
    recent_expenditure = tb[
        ["year", "country", "government_expenditure_on_education_as_a_percentage_of_gdp__pct__xgdp_fsgov"]
    ].copy()

    # Merge historic and recent expenditure data based on 'year' and 'country'
    combined_df = pr.merge(historic_expenditure, recent_expenditure, on=["year", "country"], how="outer")

    # Combine expenditure data, favoring recent over historical
    combined_df["combined_expenditure_share_gdp"] = combined_df[
        "government_expenditure_on_education_as_a_percentage_of_gdp__pct__xgdp_fsgov"
    ].fillna(combined_df["public_expenditure_on_education__tanzi__and__schuktnecht__2000"])

    # Merge the combined expenditure data back into the original table
    tb = pr.merge(
        tb,
        combined_df[["year", "country", "combined_expenditure_share_gdp"]],
        on=["year", "country"],
        how="outer",
    )

    tb = tb.format(["country", "year"])
    return tb


def update_metadata(meta: VariableMeta, display_decimals: int, unit: str, short_unit: str) -> None:
    """
    Update metadata attributes of a specified column in the given table.

    Args:
    meta (obj): Metadata object of the column to update.
    display_decimals (int): Number of decimal places to display.
    unit (str): The full name of the unit of measurement for the column data.
    short_unit (str, optional): The abbreviated form of the unit. Defaults to an empty space.

    Returns:
    None: The function updates the table in-place.
    """
    meta.display["numDecimalPlaces"] = display_decimals
    meta.unit = unit
    meta.short_unit = short_unit
