"""Load a meadow dataset and create a garden dataset.

Notes:

    - "Gapminder SG" stands for "Gapminder Systema Globalis"
"""

import json
from typing import Dict, Tuple

import owid.catalog.processing as pr
from owid.catalog import Dataset, License, Origin, Table
from utils import (
    COUNTRIES_FORMER_EQUIVALENTS,
    GAPMINDER_SG_COUNTRIES,
    GAPMINDER_SG_COUNTRIES_FORMER,
    GAPMINDER_SG_ORIGINS,
    SOURCES_NAMES,
    YEAR_END_WPP,
    YEAR_START_GAPMINDER,
    YEAR_START_HYDE,
    YEAR_START_WPP,
    YEAR_START_WPP_PROJ,
)

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Coluns relevant
COLUMS_RELEVANT = [
    "country",
    "year",
    "population",
]


def run(dest_dir: str) -> None:
    """Run main code."""
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
    ds_gapminder_sg = paths.load_dataset(short_name="gapminder__systema_globalis", channel="open_numbers")
    tb_gapminder_sg = ds_gapminder_sg["total_population_with_projections"].reset_index()

    # Load regions table
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"]
    # Load income groups table
    ds_income_groups = paths.load_dataset("income_groups")
    # Load FAO
    ds_land_area = paths.load_dataset("faostat_rl")
    tb_land_area = ds_land_area["faostat_rl_flat"].reset_index()

    #
    # Process data.
    #
    # Format tables
    tb_hyde = format_hyde(tb=tb_hyde)
    tb_gapminder = format_gapminder(tb_gapminder)
    tb_un = format_wpp(tb_un)
    tb_gapminder_sg, tb_gapminder_sg_former = format_gapminder_sg(tb_gapminder_sg)

    # Concat tables
    tb = pr.concat(
        [tb_hyde, tb_gapminder, tb_un, tb_gapminder_sg],
        ignore_index=True,
    )

    # Make table
    tb = (
        tb.pipe(select_source)
        .astype(
            {
                "year": int,
                "population": "uint64",
            }
        )
        .pipe(add_regions, ds_regions, ds_income_groups)
        .pipe(add_world)
        .pipe(add_historical_regions, tb_gapminder_sg_former, tb_regions)
        .pipe(fix_anomalies)
        .astype(
            {
                "year": "int64",
                "population": "uint64",
                "source": "category",
            }
        )
        .pipe(add_world_population_share)
    )

    # Add population growth rate
    tb_growth_rate = make_table_growth_rate(
        tb_population=tb,
    )

    # Add population density
    tb_density = make_table_density(
        tb_population=tb,
        tb_land_area=tb_land_area,
    )

    # Create auxiliary table
    tb_auxiliary = generate_auxiliary_table(tb)

    # Format tables
    tb = tb.format(["country", "year"], short_name="population_original")
    tb_auxiliary = tb_auxiliary.format(["country", "year"], short_name="population")
    tb_density = tb_density.format(["country", "year"], short_name="population_density")
    tb_growth_rate = tb_growth_rate.format(["country", "year"], short_name="population_growth_rate")

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_auxiliary,
        tb_density,
        tb_growth_rate,
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
    tb = tb.rename(columns=columns_rename, errors="raise").loc[:, COLUMS_RELEVANT]
    # Set source identifier
    tb["source"] = "hyde"
    return tb


######################
# Gapminder ##########
######################
def format_gapminder(tb: Table) -> Table:
    """Format Gapminder table."""
    # Select relevant columns
    tb = tb.loc[:, COLUMS_RELEVANT]
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
        ((tb["sex"] == "all") & (tb["age"] == "all") & (tb["variant"].isin(["estimates", "medium"]))),
        ["country", "year", "variant", "population"],
    ]

    # Sanity checks IN
    assert (
        tb.loc[tb["variant"] == "estimates", "year"].min() == YEAR_START_WPP
    ), f"Unexpected start year for WPP estimates. Should be {YEAR_START_WPP}!"
    assert (
        tb.loc[tb["variant"] == "estimates", "year"].max() == YEAR_START_WPP_PROJ - 1
    ), f"Unexpected end year for WPP estimates. Should be {YEAR_START_WPP_PROJ-1}!"
    assert (
        tb.loc[tb["variant"] == "medium", "year"].min() == YEAR_START_WPP_PROJ
    ), f"Unexpected start year for WPP projections. Should be {YEAR_START_WPP_PROJ}!"
    assert (
        tb.loc[tb["variant"] == "medium", "year"].max() == YEAR_END_WPP
    ), f"Unexpected end year for WPP projections. Should be {YEAR_END_WPP}!"

    # Rename columns, sort rows
    tb = (
        tb.loc[:, ["country", "year", "population"]]
        .assign(source="unwpp")
        .astype(
            {
                "source": "string",
                "country": "string",
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
    tb = tb.loc[~tb.country.isin(countries_exclude)]

    # Sanity checks OUT
    assert tb.groupby(["country", "year"])["population"].count().max() == 1

    return tb


######################
# Gapminder SG #######
######################
def format_gapminder_sg(tb: Table) -> Tuple[Table, Table]:
    """Format Gapminder Systema Globalis table."""
    columns_rename = {
        "country": "country",
        "time": "year",
        "total_population_with_projections": "population",
    }

    def _core_formatting(tb: Table, country_rename: Dict[str, str]) -> Table:
        ## rename countries
        tb["country"] = tb["geo"].map(country_rename)
        ## rename columns
        tb = tb.rename(columns=columns_rename, errors="raise").loc[:, COLUMS_RELEVANT]
        # Set source identifier
        tb["source"] = "gapminder_sg"
        # add origins
        tb["population"].metadata.origins = GAPMINDER_SG_ORIGINS
        return tb

    # Data on former countries
    ## only keep former country data
    tb_former = tb.loc[tb["geo"].isin(GAPMINDER_SG_COUNTRIES_FORMER)].copy()

    # core formatting: column and country rename, add source, metadata
    tb_former = _core_formatting(
        tb=tb_former,
        country_rename={code: data["name"] for code, data in GAPMINDER_SG_COUNTRIES_FORMER.items()},
    )

    ## filter years: only keep former countries until they disappear
    for _, data in GAPMINDER_SG_COUNTRIES_FORMER.items():
        tb_former = tb_former[~((tb_former["country"] == data["name"]) & (tb_former["year"] > data["end"]))]

    # Complement
    ## filter countries
    tb = tb.loc[tb["geo"].isin(GAPMINDER_SG_COUNTRIES)]

    # core formatting: column and country rename, add source, metadata
    tb = _core_formatting(
        tb=tb,
        country_rename=GAPMINDER_SG_COUNTRIES,
    )

    return tb, tb_former


#############################################################################################
# Combine and process data ##################################################################
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

    # If a country has UN data, then remove all non-UN data >=1950
    has_un_data = set(tb.loc[tb["source"] == "unwpp", "country"])
    tb = tb.loc[~((tb["country"].isin(has_un_data)) & (tb["year"] >= YEAR_START_WPP) & (tb["source"] != "unwpp"))]

    # If a country has Gapminder data, then remove all non-Gapminder data between 1800 and 1950
    has_gapminder_data = set(tb.loc[tb["source"] == "gapminder", "country"])
    tb = tb.loc[
        ~(
            (tb["country"].isin(has_gapminder_data))
            & (tb["year"] >= YEAR_START_GAPMINDER)
            & (tb["year"] < YEAR_START_WPP)
            & (tb["source"] != "gapminder")
        )
    ]

    # Check if all countries have only one row per year
    _ = tb.set_index(["country", "year"], verify_integrity=True)

    # # map to source full names
    tb["source"] = tb["source"].map(SOURCES_NAMES)
    return tb


######################
# Add regions / World
######################
def add_regions(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
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
    sources = tb.loc[:, ["country", "year", "source"]].copy()
    tb = tb.drop(columns=["source"])

    # re-estimate region aggregates
    aggregations = {"population": "sum"}
    tb = geo.add_regions_to_table(
        tb=tb,
        regions=regions,
        aggregations=aggregations,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        num_allowed_nans_per_year=None,
        frac_allowed_nans_per_year=0.2,
        countries_that_must_have_data={
            # Continents
            "Asia": ["China", "India", "Indonesia", "Pakistan", "Bangladesh"],
            "Africa": ["Nigeria", "Ethiopia", "Egypt"],
            "North America": ["United States", "Canada", "Mexico"],
            "South America": ["Brazil", "Argentina", "Colombia", "Peru"],
            "Oceania": ["Australia", "New Zealand"],
            "Europe": ["Russia", "Germany", "France", "United Kingdom", "Italy", "Spain"],
            # Income groups
            "High-income countries": [
                "United States",
                "Japan",
                "Germany",
                "France",
                "United Kingdom",
                "Italy",
                "South Korea",
            ],
            "Upper-middle-income countries": [
                "China",
                "Brazil",
                "Indonesia",
                "Russia",
                "Mexico",
                "Vietnam",
                "Philippines",
                "Iran",
            ],
            "Lower-middle-income countries": [
                "India",
                "Pakistan",
                "Nigeria",
                "Bangladesh",
                "Philippines",
                "Egypt",
                "Kenya",
            ],
            "Low-income countries": [
                "Ethiopia",
                "Democratic Republic of Congo",
                "Congo",
                "Uganda",
            ],
        },
    )

    # tb = tb.loc[
    #     (
    #         (tb["country"].isin(regions) & (tb["year"] < 1800) & (tb["year"] % 100 != 0))
    #         | (tb["country"].isin(regions) & (tb["year"] < 1800) & (tb["year"] % 10 != 0))
    #     )

    # ]

    # add sources back
    # these are only added to countries, not aggregates
    tb = tb.merge(sources, on=["country", "year"], how="left")

    # add sources for region aggregates
    # this is done by taking the union of all sources for countries in the region
    for region in regions:
        members = geo.list_members_of_region(region, ds_regions, ds_income_groups)
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
    year_min_un = tb.loc[(tb["country"] == "World") & (tb["source"] == SOURCES_NAMES["unwpp"]), "year"].min()
    year_max_un = tb.loc[(tb["country"] == "World") & (tb["source"] == SOURCES_NAMES["unwpp"]), "year"].max()
    assert (
        (year_min_un == YEAR_START_WPP) & (year_max_un == YEAR_END_WPP)  # This is the year that the UN data starts.
    ), "World data found in UN WPP outside of [1950, 2100]!"
    ## Min year of 'World' for source HYDE
    year_min_hyde = tb.loc[(tb["country"] == "World") & (tb["source"] == SOURCES_NAMES["hyde"]), "year"].min()
    year_max_hyde = tb.loc[(tb["country"] == "World") & (tb["source"] == SOURCES_NAMES["hyde"]), "year"].max()
    assert (
        (year_min_hyde == YEAR_START_HYDE) & (year_max_hyde == 1940)  # This is the year that the UN data starts.
    ), "World data found in HYDE outside of [-10000, 1940]!"

    # Filter 'World' in HYDE for period [1800, 1950]
    tb_world = tb.loc[
        (tb["country"] == "World") & (tb["year"] > YEAR_START_HYDE) & (tb["year"] < YEAR_START_GAPMINDER)
    ].copy()

    # Estimate World using reigons
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
            & (tb_world["year"] > YEAR_START_HYDE)
            & (tb_world["year"] < YEAR_START_WPP)
        ]
        .groupby("year", as_index=False)["population"]
        .sum(numeric_only=True)
        .assign(country="World")
    )
    tb = pr.concat([tb, tb_world], ignore_index=True).sort_values(["country", "year"])

    # add sources for world
    tb.loc[tb["country"] == "World", "source"] = "; ".join(sorted(SOURCES_NAMES.values()))
    return tb


## Add historical regions
def add_historical_regions(tb: Table, tb_gm: Table, tb_regions: Table) -> Table:
    """Add historical regions.

    Historical regions are added using different techniques:

    1. Systema Globalis from Gapminder contains historical regions. We add them to the data. These include
    Yugoslavia, USSR, etc. Note that this is added after regions and world regions have been obtained, to avoid double counting.
    2. Add historical regions by grouping and summing current countries.
    """
    # 1. Add from Systema Globalis
    paths.log.info("loading data (Gapminder Systema Globalis)")
    # Add to main table
    tb_gm["source"] = SOURCES_NAMES["gapminder_sg"]
    tb = pr.concat([tb, tb_gm], ignore_index=True)

    # 2. Add historical regions by grouping and summing current countries.
    for code in COUNTRIES_FORMER_EQUIVALENTS:
        # Get former country name and end year (dissolution)
        former_country_name = tb_regions.loc[code, "name"]
        end_year = tb_regions.loc[code, "end_year"]
        # Sanity check: former country not already in table! remember that we are creating it now
        assert former_country_name not in set(
            tb["country"]
        ), f"{former_country_name} already in table (either import it via Systema Globalis or manual aggregation)!"
        # Get list of country successors (equivalent of former state with nowadays' countries) and end year (dissolution of former state)
        codes_successors = json.loads(tb_regions.loc[code, "successors"])
        countries_successors = tb_regions.loc[codes_successors, "name"].tolist()
        # Filter table accordingly
        tb_suc = tb[(tb["year"] <= end_year) & (tb["country"].isin(countries_successors))]
        # Filter rows (only preserve years where all countries have data)
        year_filter = tb_suc.groupby("year")["country"].nunique() == len(countries_successors)
        year_filter = year_filter[year_filter].index.tolist()
        tb_suc = tb_suc[tb_suc["year"].isin(year_filter)]
        # Perform operations
        tb_suc = tb_suc.groupby("year", as_index=False, observed=True).agg(
            {"population": sum, "source": lambda x: "; ".join(sorted(set(x)))}
        )
        tb_suc["country"] = former_country_name
        # Add to main table
        tb = pr.concat([tb, tb_suc], ignore_index=True)
    return tb


def fix_anomalies(tb: Table) -> Table:
    """Make sure that all rows make sense.

    - Remove rows with population = 0.
    - Remove datapoints for the Netherland Antilles after 2010 (it was dissolved then), as HYDE has data after that year.
    """
    paths.log.info("filter rows...")
    # remove datapoints with population = 0
    tb = tb.loc[tb["population"] > 0].copy()
    # remove datapoints for the Netherland Antilles after 2010 (it was dissolved then)
    tb = tb.loc[~((tb["country"] == "Netherlands Antilles") & (tb["year"] > 2010))]
    # remove datapoints for the Netherland Antilles after 2010 (it was dissolved then)
    tb = tb.loc[~((tb["country"] == "Serbia and Montenegro") & (tb["year"] > 2006))]
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


def generate_auxiliary_table(tb: Table) -> Table:
    """Generate an identical table, with a change in the origins.

    This is to be able to show a simpler attribution in charts where population is used as a secondary indicator, and hence there is very little relevance in showing the full attribution.
    """
    tb_auxiliary = tb.copy().update_metadata(short_name="population")

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


######################
# Population density
######################
def make_table_density(tb_population: Table, tb_land_area: Table) -> Table:
    """Create a table with population density data."""
    paths.log.info("build population density table")
    # We use land area of countries as they are defined today (latest reported value)
    column_area = "land_area__00006601__area__005110__hectares"
    tb_land_area = (
        tb_land_area.loc[:, [column_area, "country", "year"]]
        .rename(columns={column_area: "area"})
        .sort_values(["country", "year"])
        .drop_duplicates(subset=["country"], keep="last")
        .drop(columns=["year"])
    )

    # Merge tables
    tb = tb_population.merge(tb_land_area, on="country", how="inner")
    # Drop NaN (no data for area)
    tb = tb.dropna(subset=["area"])
    # Estimate population density as population / land_area(in km2)
    tb["population_density"] = tb["population"] / (0.01 * tb["area"])  # 0.01 to convert from hectares to km2
    # Select relevant columns, order them, set index
    tb = tb.loc[:, ["country", "year", "population_density"]]
    return tb


#########################
# Population growth rate
#########################
def make_table_growth_rate(tb_population: Table) -> Table:
    """Estimate population growth rate as:

    growth_rate = (population[year]/population[previous_year])^(1 / (year-previous_year)) - 1
    """
    # Sorting the DataFrame by country and year to ensure the calculations are accurate
    tb = tb_population.sort_values(by=["country", "year"]).copy()

    # Creating the 'previous_population' and 'previous_year' columns
    tb["previous_population"] = tb.groupby("country")["population"].shift(1)
    tb["previous_year"] = tb.groupby("country")["year"].shift(1)
    tb["next_population"] = tb.groupby("country")["population"].shift(-1)
    tb["next_year"] = tb.groupby("country")["year"].shift(-1)

    # Only since 1700
    tb = tb.loc[tb["year"] > 1700]

    # Drop rows without previous year
    tb = tb.dropna(subset=["previous_year"])

    # Only estimate popultion growth rate if distance to latest datapoint is lower than 10 years
    tb = tb.loc[tb["year"] - tb["previous_year"] <= 10]

    # Estimate population growth rate
    tb["growth_rate"] = 100 * (
        (tb["population"] / tb["previous_population"]) ** (1 / (tb["year"] - tb["previous_year"])) - 1
    )

    # Fix 1950 and 1800
    tb["previous_growth_rate"] = tb.groupby("country")["growth_rate"].shift(1)
    tb["next_growth_rate"] = tb.groupby("country")["growth_rate"].shift(-1)

    mask_1800 = tb["year"] == 1800
    mask_1950 = tb["year"] == 1950
    tb.loc[mask_1800, "growth_rate"] = (
        tb.loc[mask_1800, "previous_growth_rate"] + tb.loc[mask_1800, "next_growth_rate"]
    ) / 2
    tb.loc[mask_1950, "growth_rate"] = (
        tb.loc[mask_1950, "previous_growth_rate"] + tb.loc[mask_1950, "next_growth_rate"]
    ) / 2

    # Keep relevant columns
    tb = tb.loc[:, ["country", "year", "growth_rate"]]
    return tb
