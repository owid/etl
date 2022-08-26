from owid import catalog
from owid.catalog import Dataset, Source, Table

from etl import data_helpers
from etl.paths import DATA_DIR, REFERENCE_DATASET


def load_land_area() -> Table:
    d = Dataset(DATA_DIR / "open_numbers/open_numbers/latest/open_numbers__world_development_indicators")
    table = d["ag_lnd_totl_k2"]

    table = table.reset_index()

    # convert iso codes to country names
    reference_dataset = catalog.Dataset(REFERENCE_DATASET)
    countries_regions = reference_dataset["countries_regions"]

    table = (
        table.rename(
            columns={
                "time": "year",
                "ag_lnd_totl_k2": "land_area",
            }
        )
        .assign(country=table.geo.str.upper().map(countries_regions["name"]))
        .dropna(subset=["country"])
        .drop(["geo"], axis=1)
        .pipe(data_helpers.calculate_region_sums)
    )

    return table.set_index(["country", "year"])


def make_table() -> Table:
    t = load_land_area()

    t.metadata.short_name = "land_area"

    source = Source(
        name="Food and Agriculture Organization of the United Nations (via World Bank)",
        description=None,
        url="http://data.worldbank.org/data-catalog/world-development-indicators",
        date_accessed="08-August-2021",
    )

    # variable ID 147839 in grapher
    t.land_area.title = "Land area (sq. km)"
    t.land_area.unit = "sq. km"
    t.land_area.short_unit = "sq. km"
    t.land_area.description = "Land area is a country's total area, excluding area under inland water bodies, national claims to continental shelf, and exclusive economic zones. In most cases the definition of inland water bodies includes major rivers and lakes.\n\nLimitations and exceptions: The data are collected by the Food and Agriculture Organization (FAO) of the United Nations through annual questionnaires. The FAO tries to impose standard definitions and reporting methods, but complete consistency across countries and over time is not possible.\n\nThe data collected from official national sources through the questionnaire are supplemented with information from official secondary data sources. The secondary sources cover official country data from websites of national ministries, national publications and related country data reported by various international organizations.\n\nStatistical concept and methodology: Total land area does not include inland water bodies such as major rivers and lakes. Variations from year to year may be due to updated or revised data rather than to change in area."
    t.land_area.display = {"unit": "Rate"}
    t.land_area.sources = [source]

    return t
