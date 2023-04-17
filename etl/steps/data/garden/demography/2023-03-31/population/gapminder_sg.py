"""
We load former country data from Systema Globalis (Open Numbers, Gapminder).

Note that their dataset contains data from Gapminder v3. For more details, download
V3 dataset from https://www.gapminder.org/data/documentation/gd003/ via the downloadable
button labeled with text "» Download Excel-file with data, including interpolations & detailed meta-data (xlsx)".

In there, open sheet "Data" and check the Source per country and year to find out how they
obtained that data.

"""
from owid.catalog import Dataset

from etl.paths import DATA_DIR

DATASET_GAPMINDER_SYSTEMA_GLOBALIS = (
    DATA_DIR / "open_numbers" / "open_numbers" / "latest" / "gapminder__systema_globalis"
)

SOURCE_NAME = "gapminder_sg"

# former countries
# to translate code to name:
# 1. use https://github.com/open-numbers/ddf--gapminder--systema_globalis/blob/master/ddf--entities--geo--country.csv
# 2. then ensure namings are aligned with our reference dataset
FORMER_COUNTRIES = {
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
        "name": "Yemen Arab Republic",
        "end": 1990,
    },
    "yem_south": {
        "name": "Yemen People's Republic",
        "end": 1990,
    },
    "yug": {
        "name": "Yugoslavia",
        "end": 1992,
    },
}

# Gapminder Systema Globalis contains data on the following countries which can
# complement the other sources. That is, contains older data which other sources don't have.
COMPLEMENT_COUNTRIES = {
    "akr_a_dhe": "Akrotiri and Dhekelia",
    "bmu": "Bermuda",
    "vgb": "British Virgin Islands",
    "cym": "Cayman Islands",
    "cok": "Cook Islands",
    "nld_curacao": "Curacao",
    "pyf": "French Polynesia",
    "gib": "Gibraltar",
    "gum": "Guam",
    "gbg": "Guernsey",
    "gbm": "Isle of Man",
    "jey": "Jersey",
    "kos": "Kosovo",
    "mac": "Macao",
    "mnp": "Northern Mariana Islands",
    "stbar": "Saint Barthelemy",
    "shn": "Saint Helena",
    "stmar": "Saint Martin (French part)",
    "sxm": "Sint Maarten (Dutch part)",
    "tkl": "Tokelau",
    "wlf": "Wallis and Futuna",
    "ssd": "South Sudan",
}


def load_gapminder_sys_glob_former():
    """load gapminder dataset's table only with former countries."""
    ds = Dataset(DATASET_GAPMINDER_SYSTEMA_GLOBALIS)
    tb = ds["total_population_with_projections"]

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


def load_gapminder_sys_glob_complement():
    """load gapminder dataset's table only with non-former countries needed to complement the rest of sources."""
    ds = Dataset(DATASET_GAPMINDER_SYSTEMA_GLOBALIS)
    tb = ds["total_population_with_projections"]

    # reset index
    tb = tb.reset_index()

    # add source
    tb["source"] = SOURCE_NAME

    # filter countries
    msk = tb["geo"].isin(COMPLEMENT_COUNTRIES)
    tb = tb[msk]

    # rename countries
    tb["country"] = tb["geo"].map(COMPLEMENT_COUNTRIES)

    # columns
    tb = tb.rename(columns={"time": "year", "total_population_with_projections": "population"})

    # output columns
    tb = tb[["country", "year", "population", "source"]]

    # reset index
    tb = tb.reset_index(drop=True)

    return tb
