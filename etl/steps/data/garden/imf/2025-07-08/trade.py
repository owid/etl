"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import shared as sh

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]
REGIONS_NO_WORLD_INCOME = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]
INCOME_GROUPS = [
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("trade")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

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

    # Define member countries for each OWID region, excluding "World".
    members = set()
    for region in REGIONS_NO_WORLD_INCOME:
        members.update(geo.list_members_of_region(region=region, ds_regions=ds_regions))

    tb_all_countries = tb[(tb["country"].isin(members)) & (tb["counterpart_country"].isin(members))]
    trading_partners = sh.calculate_trade_relationship_shares(tb_all_countries)
    trading_partners["country"] = "World"

    tb = tb.dropna(subset=["value"])

    # Remove historical regions after their dissolution dates.
    tb = sh.clean_historical_overlaps(tb, country_col="country")
    tb = sh.clean_historical_overlaps(tb, country_col="counterpart_country")

    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        ds_income_groups,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="country",
        regions=REGIONS,
    )

    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        ds_income_groups,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="counterpart_country",
        regions=REGIONS,
    )

    tb_owid = tb[
        (tb["country"].isin(REGIONS_NO_WORLD_INCOME)) & (tb["counterpart_country"].isin(REGIONS_NO_WORLD_INCOME))
    ]
    tb_owid_income = tb[(tb["country"].isin(INCOME_GROUPS)) & (tb["counterpart_country"].isin(INCOME_GROUPS))]
    tb_owid_income_world = tb[(tb["counterpart_country"].isin(INCOME_GROUPS)) & (tb["country"] == "World")]

    tb_owid_countries = tb[(tb["country"].isin(members)) & (tb["counterpart_country"].isin(REGIONS_NO_WORLD_INCOME))]
    tb_all_countries = tb[(tb["country"].isin(members)) & (tb["counterpart_country"].isin(members))]

    tb_owid_world = tb[(tb["counterpart_country"].isin(members)) & (tb["country"] == "World")]

    # Define table subsets with descriptive names
    table_subsets = [
        ("owid_regions", tb_owid),
        ("owid_income_groups", tb_owid_income),
        ("owid_income_groups_world", tb_owid_income_world),
        ("owid_world", tb_owid_world),
        ("owid_countries", tb_owid_countries),
    ]
    tbs = []
    for table_index, (table_name, table_data) in enumerate(table_subsets):
        processed_table = sh.process_table_subset(table_data)
        if table_name in ["owid_regions", "owid_income_groups"]:
            processed_table.loc[
                processed_table["country"] == processed_table["counterpart_country"], "counterpart_country"
            ] = "Intraregional"
        if table_name == "owid_world":
            processed_table = processed_table.rename(
                columns={"country": "counterpart_country", "counterpart_country": "country"}
            )
        tbs.append(processed_table)

    tb_income_total = sh.calculate_income_level_trade_shares(tb_owid_income)

    tb_income_total = tb_income_total.rename(columns={"income_flow": "country"})
    tb_income_total["counterpart_country"] = "World"

    top_import_destinations = sh.calculate_top_import_destination_share(tb_all_countries)
    top_export_destinations = sh.calculate_top_export_destination_share(tb_all_countries)
    tbs.append(tb_income_total)
    tbs.append(trading_partners)
    tb = pr.concat(tbs)
    tb = pr.merge(tb, top_import_destinations, on=["country", "year", "counterpart_country"], how="outer")
    tb = pr.merge(tb, top_export_destinations, on=["country", "year", "counterpart_country"], how="outer")

    # Improve table format.
    tb = tb.format(["country", "year", "counterpart_country"])
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
