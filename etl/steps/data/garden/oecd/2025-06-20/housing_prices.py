"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("housing_prices")

    # Read table from meadow dataset.
    tb = ds_meadow.read("housing_prices")

    # get units for each measure
    units = {}
    for measure in tb["measure"].unique():
        units[measure] = tb[tb["measure"] == measure]["unit"].unique()[0]

    # Pivot table to have measures as columns.
    tb = tb.pivot(
        index=["country", "year"],
        columns="measure",
        values="value",
    ).reset_index()

    # set units for each measure
    for measure, unit in units.items():
        tb[measure].metadata.title = measure
        if unit == "Index":
            tb[measure].metadata.unit = "Index (2015=100)"
            tb[measure].metadata.short_unit = ""
        elif unit == "Percentage of long term average":
            tb[measure].metadata.unit = "Percentage of long term average"
            tb[measure].metadata.short_unit = "%"

    # rename columns to slugs
    tb = tb.rename(
        columns={
            "Nominal house price indices": "nom_house_price_idx",
            "Price to income ratio": "price_to_income_ratio",
            "Price to rent ratio": "price_to_rent_ratio",
            "Real house price indices": "real_house_price_idx",
            "Rent prices": "rent_prices",
            "Standardised price-income ratio": "std_price_income_ratio",
            "Standardised price-rent ratio": "std_price_rent_ratio",
        }
    )

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
