from owid.catalog import Dataset

from etl.paths import DATA_DIR

DATASET_GAPMINDER = DATA_DIR / "garden" / "gapminder" / "2019-12-10" / "population"
DATASET_GAPMINDER_SYSTEMA_GLOBALIS = (
    DATA_DIR / "open_numbers" / "open_numbers" / "latest" / "gapminder__systema_globalis"
)
SOURCE_NAME = "gapminder"

# formar countries
FORMER_COUNTRIES = {
    # ant comes from HyDE
    # "ant": {
    #     "name": "Netherlands Antilles",
    #     "end": 2010,
    # },
    "cheslo": {
        "name": "Czechoslovakia",
        "end": 1993,
    },
    "deu_west": {
        "name": "West Germany",
        "end": 1990,
    },
    "deu_east": {
        "name": "East Germany",
        "end": 1990,
    },
    "eri_a_eth": {
        "name": "Eritrea and Ethiopia",
        "end": 1993,
    },
    "scg": {
        "name": "Serbia and Montenegro",
        "end": 2006,
    },
    "ussr": {
        "name": "USSR",
        "end": 1991,
    },
    "yem_north": {
        "name": "North Yemen (former)",
        "end": 1990,
    },
    "yem_south": {
        "name": "South Yemen (former)",
        "end": 1990,
    },
    "yug": {
        "name": "Yugoslavia",
        "end": 1992,
    },
}


def load_gapminder():
    tb = Dataset(DATASET_GAPMINDER)["population"]
    tb["source"] = SOURCE_NAME
    tb = tb.reset_index()
    return tb


def load_gapminder_sys_glob():
    tb = Dataset(DATASET_GAPMINDER_SYSTEMA_GLOBALIS)["total_population_with_projections"]

    # reset index
    tb = tb.reset_index()

    # add source
    tb["source"] = SOURCE_NAME

    # filter countries
    msk = tb["geo"].isin(FORMER_COUNTRIES)
    tb = tb[msk]

    # rename countries
    tb["country"] = tb["geo"].map({code: data["name"] for code, data in FORMER_COUNTRIES.items()})

    # columns
    tb = tb.rename(columns={"time": "year", "total_population_with_projections": "population"})

    # filter countries
    for _, data in FORMER_COUNTRIES.items():
        country_name = data["name"]
        end_year = data["end"]
        tb = tb[-((tb["country"] == country_name) & (tb["year"] > end_year))]

    # output columns
    tb = tb[["country", "year", "population", "source"]]

    # reset index
    tb = tb.reset_index(drop=True)
    return tb
