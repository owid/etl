from owid.catalog import Dataset, Table
from owid import catalog

from etl.paths import DATA_DIR
from etl import data_helpers


def load_land_area() -> Table:
    d = Dataset(
        DATA_DIR
        / "meadow/open_numbers/latest/open_numbers__world_development_indicators"
    )
    table = d["ag_lnd_totl_k2"]

    table = table.reset_index()

    # convert iso codes to country names
    reference_dataset = catalog.Dataset(DATA_DIR / "reference")
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
    return t
