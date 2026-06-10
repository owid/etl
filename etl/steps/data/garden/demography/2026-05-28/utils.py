# Year boundaries
YEAR_START_HYDE = -10000
YEAR_START_FT = 1800  # Federico-Tena replaces Gapminder for 1800-1938
YEAR_END_FT = 1938
GAP_LO = 1939  # bridged by linear interpolation
GAP_HI = 1949
YEAR_START_WPP = 1950
YEAR_START_WPP_PROJ = 2024
YEAR_END_WPP = 2100

# Backwards-compatible alias used elsewhere as the start of the post-HYDE block
YEAR_START_GAPMINDER = YEAR_START_FT

# sources names
# this dictionary maps source short names to complete source names
SOURCES_NAMES = {
    "unwpp": "United Nations - World Population Prospects (2024) (https://population.un.org/wpp/downloads?folder=Standard%20Projections&group=Population)",
    "ft": "Federico-Tena World Population Database — V2, 1991 borders (2026) (https://edatos.consorciomadrono.es/dataset.xhtml?persistentId=doi:10.21950/GW7SOZ)",
    "ft_interp": "Linear interpolation between Federico-Tena (1938) and UN WPP (1950)",
    "hyde": "HYDE v3.3 (2023) (https://public.yoda.uu.nl/geo/UU01/AEZZIT.html)",
}

# Former countries, sourced from our regions dataset.
## These former states are rebuilt by summing the population of their present-day successor
## countries (so they need no Gapminder Systema Globalis import). Only states whose territory is
## the union of *whole* present-day countries can be reconstructed this way — East/West Germany
## and the two Yemens cannot (each maps to a single modern country, Germany / Yemen, so summing
## can't split them) and are therefore intentionally absent.
COUNTRIES_FORMER_EQUIVALENTS = {
    "OWID_USS",  # USSR -> 15 post-Soviet republics
    "OWID_CZS",  # Czechoslovakia -> Czechia + Slovakia
    "OWID_YGS",  # Yugoslavia -> Bosnia, Croatia, N. Macedonia, Montenegro, Kosovo, Serbia, Slovenia
    "OWID_ERE",  # Ethiopia (former) -> Ethiopia + Eritrea
}

# Successors that should be summed when present but NOT required to have data every year.
## Kosovo only has data from 1950 (UN WPP); before then it is already included in Federico-Tena's
## "Serbia" (Kosovo was not a separate polity at 1991 borders). Requiring it would either drop all
## pre-1950 years of Yugoslavia or, if forced, double-count it. Treating it as optional gives the
## correct sum in both eras: pre-1950 from the 6 core successors (Serbia already covers Kosovo),
## 1950+ with Kosovo added (UN WPP's Serbia excludes it).
OPTIONAL_SUCCESSORS = {
    "OWID_YGS": {"OWID_KOS"},  # Kosovo
}
