"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Assumed USD year.
DOLLAR_YEAR = 2023


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("fossil_fuel_subsidies")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("fossil_fuel_subsidies")
    tb_indicators = ds_meadow.read("fossil_fuel_subsidies_indicators")
    tb_transport = ds_meadow.read("fossil_fuel_subsidies_transport_oil")

    #
    # Process data.
    #
    # Convert units from millions of dollars to dollars.
    tb["subsidy"] *= 1e6

    # Transpose table.
    tb = tb.pivot(index=["country", "year"], columns="product", values="subsidy", join_column_levels_with="_")

    # Rename conveniently.
    tb = tb.rename(
        columns={column: f"{column}_subsidy" for column in tb.drop(columns=["country", "year"]).columns}, errors="raise"
    )

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Include additional indicators from the other tables.
    tb = tb.merge(tb_indicators, on=["country", "year"], how="outer")
    tb = tb.merge(tb_transport, on=["country", "year"], how="outer")

    # Improve format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, yaml_params={"dollar_year": DOLLAR_YEAR}
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
