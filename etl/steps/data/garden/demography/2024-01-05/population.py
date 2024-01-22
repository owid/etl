"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import License, Origin, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year boundaries
YEAR_HYDE_START = -10000
YEAR_HYDE_END = 1800
YEAR_WPP_START = 1950
YEAR_WPP_PROJECTIONS_START = 2022
YEAR_WPP_END = 2100

# sources names
# this dictionary maps source short names to complete source names
SOURCES_NAMES = {
    "unwpp": "United Nations - World Population Prospects (2022) (https://population.un.org/wpp/Download/Standard/Population/)",
    "gapminder_v7": "Gapminder v7 (2022) (https://www.gapminder.org/data/documentation/gd003/)",
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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load UN WPP dataset.
    ds_un = paths.load_dataset("un_wpp")
    tb_un = ds_un["population"].reset_index()
    # Load HYDE dataset.
    ds_hyde = paths.load_dataset("all_indicators")
    tb_hyde = ds_hyde["all_indicators"].reset_index()
    # Load Gapminder dataset
    ds_gapminder = paths.load_dataset("population", namespace="gapminder")
    tb_gapminder = ds_gapminder["population"].reset_index()
    # Load Gapminder SG dataset
    ds_gapminder_sg = paths.load_dataset("gapminder__systema_globalis")
    tb_gapminder_sg = ds_gapminder_sg["total_population_with_projections"].reset_index()

    #
    # Process data.
    #
    # Format tables
    tb_hyde = format_hyde(tb=tb_hyde)
    tb_gapminder = format_gapminder(tb_gapminder)
    tb_un = format_wpp(tb_un)
    tb_gapminder_sg = format_gapminder_sg(tb_gapminder_sg)

    # Concat tables
    tb = pr.concat([tb_hyde, tb_gapminder, tb_un, tb_gapminder_sg], ignore_index=True)

    # Apply source-selection logic
    tb = select_source(tb)

    # Dtypes
    tb = tb.astype(
        {
            "year": int,
            "population": "uint64",
        }
    )

    # Add regions
    tb = add_regions(tb)

    # Add world

    tb_un_estimates = tb_un[tb_un["variant"] == "estimates"].drop(columns=["variant"])
    tb_un_projections = tb_un[tb_un["variant"] == "medium"].drop(columns=["variant"])
    columns_un_rename = {
        "location": "country",
        "value": "population",
    }
    tb_un_estimates = tb_un_estimates.rename(columns=columns_un_rename, errors="raise")
    tb_un_projections = tb_un_projections.rename(columns=columns_un_rename, errors="raise")

    # Combine tables
    tb = combine_sources(tb_un_estimates, tb_un_projections, tb_hyde)

    # Get world population share
    tb = add_world_population_share(tb)

    # Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    # Create auxiliary table
    tb_auxiliary = generate_auxiliary_table(tb)

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_auxiliary,
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


#############################################################################################
# FORMAT SOURCE DATA ########################################################################
#############################################################################################


######################
# HYDE ###############
######################
def format_hyde(tb: Table) -> Table:
    """Format UN WPP table."""
    # Rename columns, sort rows
    columns_rename = {
        "country": "country",
        "year": "year",
        "popc_c": "population",
    }
    # Rename columns
    tb = tb.rename(columns=columns_rename, errors="raise")[columns_rename.values()]
    # Set source identifier
    tb["source"] = "hyde"
    return tb


######################
# Gapminder ##########
######################
def format_gapminder(tb: Table) -> Table:
    """Format Gapminder table."""
    # Set source identifier
    tb["source"] = "gapminder"
    return tb


######################
# UN WPP #############
######################
def format_wpp(tb: Table) -> Table:
    """Format UN WPP table."""
    # Only keep data for general population (all sexes, all ages, etc.)
    tb = tb.loc[
        (
            (tb["metric"] == "population")
            & (tb["sex"] == "all")
            & (tb["age"] == "all")
            & (tb["variant"].isin(["estimates", "medium"]))
        ),
        ["location", "year", "variant", "value"],
    ]

    # Sanity checks IN
    assert (
        tb.loc[tb["variant"] == "estimates", "year"].min() == YEAR_WPP_START
    ), f"Unexpected start year for WPP estimates. Should be {YEAR_WPP_START}!"
    assert (
        tb.loc[tb["variant"] == "estimates", "year"].max() == YEAR_WPP_PROJECTIONS_START - 1
    ), f"Unexpected end year for WPP estimates. Should be {YEAR_WPP_PROJECTIONS_START-1}!"
    assert (
        tb.loc[tb["variant"] == "medium", "year"].min() == YEAR_WPP_PROJECTIONS_START
    ), f"Unexpected start year for WPP projections. Should be {YEAR_WPP_PROJECTIONS_START}!"
    assert (
        tb.loc[tb["variant"] == "medium", "year"].max() == YEAR_WPP_END
    ), f"Unexpected end year for WPP projections. Should be {YEAR_WPP_END}!"

    # Rename columns, sort rows
    columns_rename = {
        "location": "country",
        "year": "year",
        "value": "population",
    }
    tb = (
        tb.rename(columns=columns_rename, errors="raise")[columns_rename.values()]
        .assign(source="unwpp")
        .astype(
            {
                "source": "str",
                "country": str,
                "population": "uint64",
                "year": "uint64",
            }
        )
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    # Exclude countries
    countries_exclude = [
        "Northern America",
        "Latin America & Caribbean",
        "Land-locked developing countries (LLDC)",
        "Latin America and the Caribbean",
        "Least developed countries",
        "Less developed regions",
        "Less developed regions, excluding China",
        "Less developed regions, excluding least developed countries",
        "More developed regions",
        "Small island developing states (SIDS)",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
    ]
    tb.loc[~tb.country.isin(countries_exclude)]

    # Sanity checks OUT
    assert tb.groupby(["country", "year"])["population"].count().max() == 1

    return tb


######################
# Gapminder SG #######
######################
def format_gapminder_sg(tb: Table) -> Table:
    """Format Gapminder SG table."""
    # filter countries
    msk = tb["geo"].isin(GAPMINDER_SG_COUNTRIES)
    tb = tb.loc[msk]

    # rename countries
    tb["country"] = tb["geo"].map(GAPMINDER_SG_COUNTRIES)

    # columns
    columns_rename = {
        "country": "country",
        "time": "year",
        "total_population_with_projections": "population",
    }
    tb = tb.rename(columns=columns_rename, errors="raise")[columns_rename.values()]

    # Set source identifier
    tb["source"] = "gapminder_sg"

    # add origins
    tb.population.metadata.origins = GAPMINDER_SG_ORIGINS
    return tb


#############################################################################################
# Combine data ##############################################################################
#############################################################################################


######################
# Select source  #####
######################
def select_source(tb: Table) -> Table:
    """Select the best source for each country/year.

    The prioritisation scheme (i.e. what source is used/preferred) is:
    - For 1800 - 2100: WPP > Gapminder > HYDE.
    - Prior to 1800: HYDE

    """
    paths.log.info("selecting source...")
    tb = tb.loc[tb["population"] > 0]

    # If a country has UN data, then remove all non-UN data after 1949
    has_un_data = set(tb.loc[tb["source"] == "unwpp", "country"])
    tb = tb.loc[~((tb["country"].isin(has_un_data)) & (tb["year"] >= YEAR_WPP_START) & (tb["source"] != "unwpp"))]

    # If a country has Gapminder data, then remove all non-Gapminder data between 1800 and 1949
    has_gapminder_data = set(tb.loc[tb["source"] == "gapminder", "country"])
    tb = tb.loc[
        ~(
            (tb["country"].isin(has_gapminder_data))
            & (tb["year"] > YEAR_HYDE_END)
            & (tb["year"] < YEAR_WPP_START)
            & (tb["source"] != "gapminder_v7")
        )
    ]

    # Check if all countries have only one row per year
    _ = tb.set_index(["country", "year"], verify_integrity=True)

    # # map to source full names
    # tb["source"] = tb["source"].map(SOURCES_NAMES)
    return tb


######################
# Add regions / World
######################
def add_regions(tb: Table) -> Table:
    """Add continents and income groups."""
    paths.log.info("population: adding regions...")
    regions = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "European Union (27)",
    ]
    # make sure to exclude regions if already present
    tb = tb.loc[~tb["country"].isin(regions)]

    # keep sources per countries, remove from tb
    # remove from tb: otherwsie geo.add_region_aggregates will add this column too
    sources = tb[["country", "year", "source"]].copy()
    tb = tb.drop(columns=["source"])

    # re-estimate region aggregates
    for region in regions:
        # TODO: Check if keeping the default countries_that_must_have_data, num_allowed_nans_per_year, and
        #   frac_allowed_nans_per_year makes makes no difference. In that case, keep default.
        tb = geo.add_region_aggregates(
            df=tb,
            region=region,
            population=tb,
            countries_that_must_have_data="auto",
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.2,
        )

    # add sources back
    # these are only added to countries, not aggregates
    tb = tb.merge(sources, on=["country", "year"], how="left")

    # add sources for region aggregates
    # this is done by taking the union of all sources for countries in the region
    for region in regions:
        members = geo.list_countries_in_region(region)
        s = tb.loc[tb["country"].isin(members), "source"].unique()
        sources_region = sorted(s)
        tb.loc[tb["country"] == region, "source"] = "; ".join(sources_region)
    return tb


def add_world(tb: Table) -> Table:
    """Add world aggregate.

    We do this by adding the values for all continents.

    HYDE and UN already provide estimates on world population. Therefore, we only estimate the
    world population for period in between: 1800 - 1950.
    """
    paths.log.info("adding World...")

    # Sanity checks
    ## Min year of 'World' for source UN WPP
    year_min_un = tb.loc[(tb["country"] == "World") & (tb["source"] == "unwpp"), "year"].min()
    year_max_un = tb.loc[(tb["country"] == "World") & (tb["source"] == "unwpp"), "year"].max()
    assert (
        (year_min_un == YEAR_WPP_START) & (year_max_un == YEAR_WPP_END)  # This is the year that the UN data starts.
    ), "World data found in UN WPP outside of [1950, 2100]!"
    ## Min year of 'World' for source HYDE
    year_min_hyde = tb.loc[(tb["country"] == "World") & (tb["source"] == "hyde"), "year"].min()
    year_max_hyde = tb.loc[(tb["country"] == "World") & (tb["source"] == "hyde"), "year"].min()
    assert (
        (year_min_hyde == -YEAR_HYDE_START) & (year_max_hyde == 1940)  # This is the year that the UN data starts.
    ), "World data found in HYDE outside of [-10000, 1940]!"

    # Filter 'World' in HYDE for period [1800, 1950]
    tb = tb.loc[(tb["country"] == "World") & (tb["year"] > YEAR_HYDE_START) & (tb["year"] < YEAR_HYDE_END)]

    #
    tb_world = tb.copy()
    continents = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
    ]
    # Estimate "World" population for years without HYDE and UN WPP data
    tb_world = (
        tb_world[
            (tb_world["country"].isin(continents))
            & (tb_world["year"] > YEAR_HYDE_START)
            & (tb_world["year"] < YEAR_WPP_START)
        ]
        .groupby("year", as_index=False)["population"]
        .sum(numeric_only=True)
        .assign(country="World")
    )
    tb = pr.concat([tb, tb_world], ignore_index=True).sort_values(["country", "year"])

    # add sources for world
    tb.loc[tb["country"] == "World", "source"] = "; ".join(sorted(SOURCES_NAMES.values()))
    return tb


## OTHERS


def generate_auxiliary_table(tb: Table) -> Table:
    """Generate an identical table, with a change in the origins.

    This is to be able to show a simpler attribution in charts where population is used as a secondary indicator, and hence there is very little relevance in showing the full attribution.
    """
    tb_auxiliary = tb.copy().update_metadata(short_name="population_as_auxiliary_indicator")

    # Origins from original table
    origins_raw = tb["population"].metadata.origins
    date_accessed = max(origin.date_accessed for origin in origins_raw)

    # Add origins
    origins = [
        Origin(
            producer="Various sources",
            title="Population",
            attribution="Population based on various sources (2024)",
            attribution_short="Population",
            citation_full="The long-run data on population is based on various sources, described on this page: https://ourworldindata.org/population-sources",
            url_main="https://ourworldindata.org/population-sources",
            date_accessed=date_accessed,
            date_published=paths.version,
            description=(
                "Our World in Data builds and maintains a long-run dataset on population by country, region, and for the world, based on various sources.\n\n"
                + "You can find more information on these sources and how our time series is constructed on this page: https://ourworldindata.org/population-sources"
            ),
            license=License(name="CC BY 4.0", url="https://creativecommons.org/licenses/by/4.0/"),
        )
    ]
    ## Add to indicators
    for col in tb_auxiliary.columns:
        tb_auxiliary[col].origins = origins

    return tb_auxiliary


def combine_sources(tb_wpp_estimates: Table, tb_wpp_projections: Table, tb_hyde: Table) -> Table:
    """Combine all sources."""
    # Sanity check: years
    assert tb_hyde["year"].min() == YEAR_HYDE_START, "Unexpected start year for HYDE"
    assert tb_wpp_estimates["year"].min() == YEAR_WPP_START, "Unexpected start year for WPP estimates"
    assert tb_wpp_projections["year"].min() == YEAR_WPP_PROJECTIONS_START, "Unexpected start year for WPP projections"
    assert tb_wpp_projections["year"].max() == YEAR_WPP_END, "Unexpected end year for WPP projections"

    # Filter HYDE data (upper range)
    tb_hyde = tb_hyde[tb_hyde["year"] < YEAR_WPP_START]

    # Combine tables
    tb = pr.concat(
        [
            tb_hyde,
            tb_wpp_estimates,
            tb_wpp_projections,
        ],
        ignore_index=True,
        short_name=paths.short_name,
    )

    return tb


def add_world_population_share(tb: Table) -> Table:
    """Obtain world's population share for each country/region and year."""
    paths.log.info("adding world population share...")
    # Add a metric "% of world population"
    tb_world = tb.loc[tb["country"] == "World", ["year", "population"]].rename(columns={"population": "world_pop"})
    tb = tb.merge(tb_world, on="year", how="left")
    tb["world_pop_share"] = 100 * tb["population"].div(tb["world_pop"])
    tb = tb.drop(columns="world_pop")
    return tb
