"""Load a meadow dataset and create a garden dataset."""
from owid.catalog import VariableMeta

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("education_opri")

    # Read table from meadow dataset.
    tb = ds_meadow["education_opri"].reset_index()

    # Retrieve snapshot with the metadata provided via World Bank.

    snap_wb = paths.load_snapshot("edstats_metadata.xls")
    tb_wb = snap_wb.read()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Drop columns that are not needed
    tb = tb.drop(columns=["indicator_id", "magnitude", "qualifier"])

    # Add the long description from the World Bank metadata
    long_definition = {}
    for indicator in tb["indicator_label_en"].unique():
        definition = tb_wb[tb_wb["Indicator Name"] == indicator]["Long definition"].values
        if len(definition) > 0:
            long_definition[indicator] = definition[0]
        else:
            long_definition[indicator] = ""

    tb["long_description"] = tb["indicator_label_en"].map(long_definition)

    # Pivot the table to have the indicators as columns to add descriptions from producer
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")
    for column in tb_pivoted.columns:
        meta = tb_pivoted[column].metadata
        meta.display = {}
        long_definition = tb["long_description"].loc[tb["indicator_label_en"] == column].iloc[0]
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
            update_metadata(meta, display_decimals=1, unit="current US$", short_unit="current $")
        elif "(number)" in column.lower():
            update_metadata(meta, display_decimals=0, unit="number", short_unit="")

        else:
            # Default metadata update when no other conditions are met.
            update_metadata(meta, 0, " ", " ")

    tb_pivoted = tb_pivoted.reset_index()
    tb_pivoted = tb_pivoted.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_pivoted], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


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
