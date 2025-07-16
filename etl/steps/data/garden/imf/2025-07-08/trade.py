"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


IMF_REGIONS = [
    "Latin America and the Caribbean",
    "Sub-Saharan Africa",
    "Emerging and Developing Asia",
    "Middle East and Central Asia",
    "Advanced Economies",
    "Emerging and Developing Europe",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("trade")
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow.read("trade")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = geo.harmonize_countries(
        df=tb,
        country_col="counterpart_country",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )
    tb = tb.dropna(subset=["value"])  # Add this line

    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="country",
        regions=REGIONS,
    )

    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="counterpart_country",
        regions=REGIONS,
    )

    regions_without_world = [region for region in REGIONS if region != "World"]
    tb_owid = tb[tb["country"].isin(regions_without_world) & tb["counterpart_country"].isin(regions_without_world)]
    members = []
    for region in regions_without_world:
        members.append(
            geo.list_members_of_region(
                region=region,
                ds_regions=ds_regions,
            )
        )
    members = set().union(*members)
    tb_owid_countries = tb[tb["country"].isin(regions_without_world) & tb["counterpart_country"].isin(members)]
    tb_owid_world = tb[(tb["country"] == "World") & (tb["counterpart_country"].isin(members))]
    tb_imf = tb[tb["country"].isin(IMF_REGIONS) & tb["counterpart_country"].isin(IMF_REGIONS)]

    tbs = []
    for t, tb in enumerate([tb_owid, tb_imf, tb_owid_world, tb_owid_countries]):
        tb = tb.pivot(
            index=["country", "year", "counterpart_country"],
            columns="indicator",
            values="value",
        ).reset_index()
        tb.loc[tb["country"] == tb["counterpart_country"], "counterpart_country"] = "Intraregional"

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

        # Calculate share of total trade with each region (exports + imports with region / total trade volume)
        tb["bilateral_trade_volume"] = (
            tb["Exports of goods, Free on board (FOB), US dollar"]
            + tb["Imports of goods, Cost insurance freight (CIF), US dollar"]
        )
        tb["share_of_total_trade"] = tb["bilateral_trade_volume"] / tb["total_trade_volume"] * 100

        # Rename rows where country equals counterpart_country to "Intraregional"
        # Calculate total net trade balance across all counterpart countries
        total_net_balance = tb.groupby(["country", "year"])["Trade balance goods, US dollar"].sum().reset_index()
        total_net_balance["counterpart_country"] = "Total"

        total_net_balance = total_net_balance.merge(
            totals[["country", "year", "total_exports", "total_imports"]], on=["country", "year"], how="left"
        )
        total_net_balance["total_trade_volume"] = (
            total_net_balance["total_exports"] + total_net_balance["total_imports"]
        )
        total_net_balance["trade_balance_goods__share"] = (
            total_net_balance["Trade balance goods, US dollar"] / total_net_balance["total_trade_volume"] * 100
        )

        # Add empty values for other columns to match structure
        total_net_balance["exports_of_goods__free_on_board__fob__share"] = None
        total_net_balance["imports_of_goods__cost_insurance_freight__cif__share"] = None
        total_net_balance["share_of_total_trade"] = None
        total_net_balance = total_net_balance[total_net_balance["counterpart_country"] != "Total"]

        # Select only the columns we need for the total row
        total_net_balance = total_net_balance[
            [
                "country",
                "year",
                "counterpart_country",
                "exports_of_goods__free_on_board__fob__share",
                "imports_of_goods__cost_insurance_freight__cif__share",
                "trade_balance_goods__share",
                "share_of_total_trade",
            ]
        ]

        # Keep only the share columns and identifiers
        tb = tb[
            [
                "country",
                "year",
                "counterpart_country",
                "exports_of_goods__free_on_board__fob__share",
                "imports_of_goods__cost_insurance_freight__cif__share",
                "trade_balance_goods__share",
                "share_of_total_trade",
            ]
        ]

        tb = pr.concat([tb, total_net_balance])
        if t > 1:
            tb = tb.rename(columns={"country": "counterpart_country", "counterpart_country": "country"})
        tbs.append(tb)

    tb = pr.concat(tbs)

    # Improve table format.
    tb = tb.format(["country", "year", "counterpart_country"])

    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
