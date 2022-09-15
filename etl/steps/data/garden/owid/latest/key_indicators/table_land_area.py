from pathlib import Path

from owid import catalog
from owid.catalog import Dataset, Source, Table

from etl import data_helpers
from etl.paths import DATA_DIR, REFERENCE_DATASET


DIR_PATH = Path(__file__).parent


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
    t.update_metadata_from_yaml(DIR_PATH / "key_indicators.meta.yml", "land_area")

    # variable ID 147839 in grapher
    t.land_area.display = {"unit": "%"}

    return t
