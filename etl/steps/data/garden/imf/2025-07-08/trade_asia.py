"""Load a meadow dataset and create a garden dataset."""

import shared as sh
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS_OF_INTEREST = [
    "European Union (IMF)",
    "China",
    "United States",
    "North America",
    "South America",
    "Africa",
    "Asia",
    "Europe",
    "Asia (excl. China)",
    "Oceania",
]
REGIONS_OWID = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]
# Column name constants
EXPORT_COL = "Exports of goods, Free on board (FOB), US dollar"
IMPORT_COL = "Imports of goods, Cost insurance freight (CIF), US dollar"


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
    country_mapping_path = paths.directory / "trade.countries.json"
    excluded_countries_path = paths.directory / "trade.excluded_countries.json"

    tb = geo.harmonize_countries(
        df=tb, countries_file=country_mapping_path, excluded_countries_file=excluded_countries_path
    )
    tb = geo.harmonize_countries(
        df=tb,
        country_col="counterpart_country",
        countries_file=country_mapping_path,
        excluded_countries_file=excluded_countries_path,
    )
    tb = tb.dropna(subset=["value"])

    # Remove historical regions after their dissolution dates.
    tb = sh.clean_historical_overlaps(tb, country_col="country")
    tb = sh.clean_historical_overlaps(tb, country_col="counterpart_country")

    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="counterpart_country",
        regions=REGIONS_OWID + ["World"],
    )

    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="country",
        regions=REGIONS_OWID + ["World"],
    )

    # Create "Asia - China" region by aggregating Asian countries excluding China
    asia_members = geo.list_members_of_region(region="Asia", ds_regions=ds_regions)
    asia_minus_china_members = [country for country in asia_members if country != "China"]

    # Add Asia - China as a region for both country columns
    tb = geo.add_region_aggregates(
        tb,
        region="Asia (excl. China)",
        countries_in_region=asia_minus_china_members,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="counterpart_country",
    )

    tb = geo.add_region_aggregates(
        tb,
        region="Asia (excl. China)",
        countries_in_region=asia_minus_china_members,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="country",
    )

    # Define member countries for each OWID region, excluding "World".
    members = set()
    for region in REGIONS_OWID:
        members.update(geo.list_members_of_region(region=region, ds_regions=ds_regions))

    tb = tb[
        (tb["country"].isin(list(members) + REGIONS_OF_INTEREST))
        & (tb["counterpart_country"].isin(REGIONS_OF_INTEREST + ["World"]))
    ]
    tb = calculate_trade_shares_as_share_world(tb)
    tb.loc[tb["country"] == tb["counterpart_country"], "counterpart_country"] = "Intraregional"
    tb = tb[
        [
            "country",
            "year",
            "counterpart_country",
            "exports_of_goods__free_on_board__fob__share",
            "imports_of_goods__cost_insurance_freight__cif__share",
            "share_of_total_trade",
        ]
    ].copy()

    # Improve table format.
    tb = tb.format(["country", "year", "counterpart_country"])
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_trade_shares_as_share_world(tb: Table) -> Table:
    """Calculate trade shares for a given table, using the 'World' rows as totals."""

    tb = tb.pivot(
        index=["country", "year", "counterpart_country"],
        columns="indicator",
        values="value",
    ).reset_index()
    # Convert from millions-of-dollars units to actual dollars
    tb[EXPORT_COL] = tb[EXPORT_COL] * 1_000_000
    tb[IMPORT_COL] = tb[IMPORT_COL] * 1_000_000
    # 1) Extract the world‐total rows and rename their export/import columns
    world_totals = (
        tb[tb["counterpart_country"] == "World"]
        .loc[:, ["country", "year", EXPORT_COL, IMPORT_COL]]
        .rename(
            columns={
                EXPORT_COL: "total_exports",
                IMPORT_COL: "total_imports",
            }
        )
    )
    tb = tb[tb["counterpart_country"] != "World"]

    # Merge totals back to main table
    tb = tb.merge(world_totals, on=["country", "year"], how="left")

    # 3) Now compute shares exactly as before
    tb["exports_of_goods__free_on_board__fob__share"] = tb[EXPORT_COL] / tb["total_exports"] * 100
    tb["imports_of_goods__cost_insurance_freight__cif__share"] = tb[IMPORT_COL] / tb["total_imports"] * 100
    tb["total_trade_volume"] = tb["total_exports"] + tb["total_imports"]
    tb["bilateral_trade_volume"] = tb[EXPORT_COL] + tb[IMPORT_COL]

    tb["share_of_total_trade"] = tb["bilateral_trade_volume"] / tb["total_trade_volume"] * 100

    return tb
