"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import License, Origin, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year boundaries
YEAR_WPP_START = 1950
YEAR_WPP_PROJECTIONS_START = 2022
YEAR_WPP_END = 2100
YEAR_HYDE_START = -10000


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load UN WPP dataset.
    ds_un = paths.load_dataset("un_wpp")
    tb_un = ds_un["un_wpp"].reset_index()
    ds_hyde = paths.load_dataset("all_indicators")
    tb_hyde = ds_hyde["all_indicators"].reset_index()

    #
    # Process data.
    #
    columns_index = ["country", "year"]
    # Format hyde
    tb_hyde = tb_hyde.loc[:, columns_index + ["popc_c"]]
    tb_hyde = tb_hyde.rename(columns={"popc_c": "population"}, errors="raise")

    # Format wpp
    tb_un = tb_un.loc[
        (tb_un["metric"] == "population") & (tb_un["sex"] == "all") & (tb_un["age"] == "all"),
        ["location", "year", "variant", "value"],
    ]
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


def combine_sources(tb_wpp_estimates: Table, tb_wpp_projections: Table, tb_hyde: Table) -> Table:
    """Combine all sources"""
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


def add_world_population_share(tb: Table) -> Table:
    """Obtain world's population share for each country/region and year."""
    paths.log.info("adding world population share...")
    # Add a metric "% of world population"
    tb_world = tb.loc[tb["country"] == "World", ["year", "population"]].rename(columns={"population": "world_pop"})
    tb = tb.merge(tb_world, on="year", how="left")
    tb["world_pop_share"] = 100 * tb["population"].div(tb["world_pop"])
    tb = tb.drop(columns="world_pop")
    return tb
