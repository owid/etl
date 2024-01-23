from owid.catalog import License, Origin

# Year boundaries
YEAR_HYDE_START = -10000
YEAR_HYDE_END = 1799
YEAR_WPP_START = 1950
YEAR_WPP_PROJECTIONS_START = 2022
YEAR_WPP_END = 2100

# sources names
# this dictionary maps source short names to complete source names
SOURCES_NAMES = {
    "unwpp": "United Nations - World Population Prospects (2022) (https://population.un.org/wpp/Download/Standard/Population/)",
    "gapminder": "Gapminder v7 (2022) (https://www.gapminder.org/data/documentation/gd003/)",
    "gapminder_sg": "Gapminder - Systema Globalis (2023) (https://github.com/open-numbers/ddf--gapminder--systema_globalis)",
    "hyde": "HYDE v3.3 (2023) (https://public.yoda.uu.nl/geo/UU01/AEZZIT.html)",
}

# Gapminder Systema Globalis contains data on the following countries which can
# complement the other sources. That is, contains older data which other sources don't have.
GAPMINDER_SG_COUNTRIES = {
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
# former countries (1): sourced from gapminder systema globalis
# to translate code to name:
# 1. use https://github.com/open-numbers/ddf--gapminder--systema_globalis/blob/master/ddf--entities--geo--country.csv
# 2. then ensure namings are aligned with our reference dataset
GAPMINDER_SG_COUNTRIES_FORMER = {
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
        "name": "Ethiopia (former)",
        "end": 1993,
    },
    # "scg": {
    #     "name": "Serbia and Montenegro",
    #     "end": 2006,
    # },
    # "ussr": {
    #     "name": "USSR",
    #     "end": 1991,
    # },
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
# former countries (2): sourced from our regions dataset
## These countries are added by aggregating their successors' values, and using regions.
COUNTRIES_FORMER_EQUIVALENTS = {"OWID_USS"}

# Gapminder SG origins
GAPMINDER_SG_ORIGINS = [
    Origin(
        producer="Gapminder",
        title="Systema Globalis",
        citation_full="Gapminder - Systema Globalis (2023).",
        url_main="https://github.com/open-numbers/ddf--gapminder--systema_globalis",
        attribution="Gapminder - Systema Globalis (2022)",
        attribution_short="Gapminder",
        date_accessed="2023-03-31",
        date_published="2023-02-21",  # type: ignore
        license=License(
            name="CC BY 4.0",
            url="https://github.com/open-numbers/ddf--gapminder--systema_globalis",
        ),
    )
]
