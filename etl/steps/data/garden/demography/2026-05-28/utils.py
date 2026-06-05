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
## These countries are added by aggregating their successors' values, and using regions.
## NOTE: Unlike the 2024-07-15 step, no former states are imported from Gapminder Systema
## Globalis (Czechoslovakia, Yugoslavia, East/West Germany, the Yemens, Ethiopia (former)
## are therefore no longer in the dataset).
COUNTRIES_FORMER_EQUIVALENTS = {"OWID_USS"}
