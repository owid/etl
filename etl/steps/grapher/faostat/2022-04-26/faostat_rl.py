from typing import Iterable

from owid import catalog

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh

NAMESPACE = "faostat"
VERSION = "2022-04-26"
DATASET_SHORT_NAME = "faostat_rl"


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(
        DATA_DIR / "garden" / NAMESPACE / VERSION / DATASET_SHORT_NAME
    )
    # short_name should include dataset name and version
    dataset.metadata.short_name = f"{DATASET_SHORT_NAME}__{VERSION}".replace("-", "_")

    # move description to source as that is what is shown in grapher
    # (dataset.description would be displayed under `Internal notes` in the admin UI otherwise)
    dataset.metadata.sources[0].description = dataset.metadata.description
    dataset.metadata.description = ""

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset[DATASET_SHORT_NAME].reset_index()

    table["entity_id"] = gh.country_to_entity_id(table["country"])

    table = table.set_index(["entity_id", "year"])[
        [
            "agricultural_land__area__1000_ha",
            "agriculture__area__1000_ha",
            "agriculture_area_actually_irrigated__area__1000_ha",
            "agriculture_area_certified_organic__area__1000_ha",
            "agriculture_area_under_organic_agric__area__1000_ha",
            "arable_land__area__1000_ha",
            "coastal_waters__area__1000_ha",
            "coastal_waters_used_for_aquac__or_holding_facilities__area__1000_ha",
            "coastal_waters_used_for_capture_fisheries__area__1000_ha",
            "country_area__area__1000_ha",
            "cropland__area__1000_ha",
            "cropland_area_actually_irrigated__area__1000_ha",
            "cropland_area_certified_organic__area__1000_ha",
            "cropland_area_under_conservation_tillage__area__1000_ha",
            "cropland_area_under_conventional_tillage__area__1000_ha",
            "cropland_area_under_organic_agric__area__1000_ha",
            "cropland_area_under_zero_or_no_tillage__area__1000_ha",
            "eez_used_for_aquac__or_holding_facilities__area__1000_ha",
            "eez_used_for_capture_fisheries__area__1000_ha",
            "exclusive_economic_zone__eez__area__1000_ha",
            "farm_buildings__and__farmyards__area__1000_ha",
            "forest_land__area__1000_ha",
            "forest_land__carbon_stock_in_living_biomass__million_tonnes",
            "forestry_area_actually_irrigated__area__1000_ha",
            "inland_waters__area__1000_ha",
            "inland_waters_used_for_aquac__or_holding_facilities__area__1000_ha",
            "inland_waters_used_for_capture_fisheries__area__1000_ha",
            "land_area__area__1000_ha",
            "land_area_actually_irrigated__area__1000_ha",
            "land_area_equipped_for_irrigation__area__1000_ha",
            "land_under_perm__meadows_and_pastures__area__1000_ha",
            "land_under_permanent_crops__area__1000_ha",
            "land_under_protective_cover__area__1000_ha",
            "land_under_temp__meadows_and_pastures__area__1000_ha",
            "land_under_temporary_crops__area__1000_ha",
            "land_used_for_aquaculture__area__1000_ha",
            "land_with_temporary_fallow__area__1000_ha",
            "naturally_regenerating_forest__area__1000_ha",
            "other_land__area__1000_ha",
            "perm__meadows__and__pastures__cultivated__area__1000_ha",
            "perm__meadows__and__pastures__nat__growing__area__1000_ha",
            "perm__meadows__and__pastures_area_actually_irrig__area__1000_ha",
            "perm__meadows__and__pastures_area_certified_organic__area__1000_ha",
            "perm__meadows__and__pastures_area_under_organic_agric__area__1000_ha",
            "planted_forest__area__1000_ha",
            "primary_forest__area__1000_ha",
        ]
    ]

    yield from gh.yield_wide_table(table, na_action="drop")
