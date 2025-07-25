"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import shared as sh

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


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
    tb = tb.dropna(subset=["value"])

    # Remove historical regions after their dissolution dates.
    tb = sh.clean_historical_overlaps(tb, country_col="country")
    tb = sh.clean_historical_overlaps(tb, country_col="counterpart_country")

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
    tb_owid = tb[(tb["country"].isin(regions_without_world)) & (tb["counterpart_country"].isin(regions_without_world))]

    # Define member countries for each OWID region, excluding "World".
    members = set()
    for region in regions_without_world:
        members.update(geo.list_members_of_region(region=region, ds_regions=ds_regions))

    tb_owid_countries = tb[(tb["country"].isin(members)) & (tb["counterpart_country"].isin(regions_without_world))]
    tb_owid_world = tb[(tb["country"].isin(members)) & (tb["counterpart_country"] == "World")]

    # Define table subsets with descriptive names
    table_subsets = [
        ("owid_regions", tb_owid),
        ("owid_world", tb_owid_world),
        ("owid_countries", tb_owid_countries),
    ]

    tbs = []
    for table_index, (table_name, table_data) in enumerate(table_subsets):
        processed_table = sh.process_table_subset(table_data)
        if table_name in ["owid_regions"]:
            #    processed_table = processed_table.rename(
            #        columns={"country": "counterpart_country", "counterpart_country": "country"}
            #    )
            processed_table.loc[processed_table["country"] == processed_table["counterpart_country"], "country"] = (
                "Intraregional"
            )

        tbs.append(processed_table)

    tb = pr.concat(tbs)

    # Improve table format.
    tb = tb.format(["country", "year", "counterpart_country"])
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
