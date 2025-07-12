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
    ds_meadow = paths.load_dataset("trade")

    # Read table from meadow dataset.
    tb = ds_meadow.read("trade")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.pivot(
        index=["country", "year", "counterpart_country"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Calculate total trade for each country-year to compute shares
    # Sum exports and imports across all counterpart countries
    totals = (
        tb.groupby(["country", "year"])[
            [
                "Exports of goods, Free on board (FOB), US dollar",
                "Imports of goods, Cost insurance freight (CIF), US dollar",
            ]
        ]
        .sum()
        .reset_index()
    )

    # Rename total columns
    totals = totals.rename(
        columns={
            "Exports of goods, Free on board (FOB), US dollar": "total_exports",
            "Imports of goods, Cost insurance freight (CIF), US dollar": "total_imports",
        }
    )

    # Merge totals back to main table
    tb = tb.merge(totals, on=["country", "year"], how="left")

    # Calculate shares (fractions of total)
    tb["exports_of_goods__free_on_board__fob__share"] = (
        tb["Exports of goods, Free on board (FOB), US dollar"] / tb["total_exports"] * 100
    )
    tb["imports_of_goods__cost_insurance_freight__cif__share"] = (
        tb["Imports of goods, Cost insurance freight (CIF), US dollar"] / tb["total_imports"] * 100
    )

    # For trade balance, calculate as share of total trade volume (exports + imports)
    tb["total_trade_volume"] = tb["total_exports"] + tb["total_imports"]
    tb["trade_balance_goods__share"] = tb["Trade balance goods, US dollar"] / tb["total_trade_volume"] * 100

    # Keep only the share columns and identifiers
    tb = tb[
        [
            "country",
            "year",
            "counterpart_country",
            "exports_of_goods__free_on_board__fob__share",
            "imports_of_goods__cost_insurance_freight__cif__share",
            "trade_balance_goods__share",
        ]
    ]

    # Improve table format.
    tb = tb.format(["country", "year", "counterpart_country"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
