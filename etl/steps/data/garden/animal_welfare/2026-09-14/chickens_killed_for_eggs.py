"""Estimate the number of chickens killed each year to produce eggs.

Starting from the number of laying hens reported by FAOSTAT, this step estimates how many hens are killed each year to
keep that flock in production, and how many male chickens (the brothers of those hens) are killed as well, either as
day-old chicks or later on. In countries that use in-ovo sexing, a share of the male embryos is removed before hatching;
those are counted separately, and not as killed chickens.
"""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# FAOSTAT item and element codes for the number of laying hens and the number of eggs produced ("Eggs from hens").
FAOSTAT_ITEM_CODE_HEN_EGGS = "00001062"
FAOSTAT_ELEMENT_CODE_LAYING = "005313"
FAOSTAT_ELEMENT_CODE_EGGS_PRODUCED = "005513"

# Average number of weeks a hen spends in the laying flock before being replaced. Taken from the model of the global
# number of chicks culled published by Faunalytics (https://faunalytics.org/global-chick-culling-2019-2024/). Unlike
# that model, we ignore mortality before and during the laying period (a few percent), as we do for animals slaughtered
# for meat.
WEEKS_IN_LAY = 62
# Sex ratio at hatch: number of male chicks hatched per female chick hatched.
MALES_PER_FEMALE = 1.0

# Countries assumed to receive the in-ovo sexed hens of the European Union, and the first year they do.
# NOTE: The producer of the in-ovo sexing data only publishes a total for the EU. Until they provide country figures,
# we allocate that total among the countries that have banned (or agreed to phase out) the culling of male chicks,
# from the year before the ban took effect (when hatcheries started the transition), in proportion to their number of
# laying hens. Germany is included from the start, since the technology was first rolled out for its market.
EU_IN_OVO_COUNTRIES = {
    "Germany": 2019,
    "France": 2022,
    "Austria": 2022,
    "Netherlands": 2026,
    "Italy": 2027,
}
# Countries whose in-ovo sexing share is reported directly by the producer.
IN_OVO_COUNTRIES_REPORTED = ["Norway", "Switzerland", "United States", "Brazil"]
# Name of the EU aggregate in the FAOSTAT and in-ovo sexing data.
EU = "European Union (27)"

# Regions for which we compute aggregates (as the sum of their member countries).
REGIONS = [
    "World",
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    EU,
    "High-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "Low-income countries",
]
# Suffix of FAOSTAT's own regions (which are dropped, like the OWID regions already included in the FAOSTAT dataset).
FAOSTAT_AGGREGATE_SUFFIX = "(FAO)"
# Gaps in a country's series are filled with its latest reported value, for at most this many years. Many countries
# (in the latest year, 14 EU countries) have no data for some years, and a few have not reported since 2020.
# NOTE: FAOSTAT's own World and EU aggregates include the missing countries; when we compared them with the sum of
# reported countries plus the carried-forward values of the missing ones, they agreed within 0.5% in every year, so
# this is also (in effect) what FAOSTAT does.
MAX_YEARS_TO_FILL = 5
# Columns with counts, that are estimated from the number of laying hens.
COUNT_COLUMNS = ["hens_killed", "male_embryos_removed", "male_chickens_killed", "chickens_killed"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT QCL dataset and read its main (long) table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl", safe_types=False)

    # Load in-ovo sexing market penetration dataset and read its main table.
    ds_in_ovo = paths.load_dataset("in_ovo_sexing_market_penetration")
    tb_in_ovo = ds_in_ovo.read("in_ovo_sexing_market_penetration")

    #
    # Process data.
    #
    # Select the number of laying hens and the number of eggs produced.
    tb = select_faostat_data(tb_qcl=tb_qcl)

    # Fill gaps in country series with the latest reported value.
    tb = fill_gaps(tb=tb)

    # Prepare the share of hens sexed in ovo for each country and year.
    tb_in_ovo = prepare_in_ovo_shares(tb_in_ovo=tb_in_ovo, tb=tb)

    # Keep only countries (drop FAOSTAT's regions, and the OWID regions included in the FAOSTAT dataset, which are
    # recomputed below).
    tb = tb[~(tb["country"].str.endswith(FAOSTAT_AGGREGATE_SUFFIX) | tb["country"].isin(REGIONS))].reset_index(
        drop=True
    )

    # Add the share of hens sexed in ovo (zero where the technology is not used).
    tb = tb.merge(tb_in_ovo, on=["country", "year"], how="left")
    tb["share_in_ovo"] = tb["share_in_ovo"].fillna(0)

    # Estimate the number of hens and male chickens killed each year.
    tb = estimate_chickens_killed(tb=tb)

    # Add region aggregates.
    tb_aggregates = paths.regions.add_aggregates(
        tb=tb[["country", "year", "laying_hens", "eggs_produced"] + COUNT_COLUMNS],
        regions=REGIONS,
        min_num_values_per_year=1,
    )
    tb_aggregates = tb_aggregates[tb_aggregates["country"].isin(REGIONS)].reset_index(drop=True)
    tb = pr.concat([tb, tb_aggregates], ignore_index=True)

    # Share of male chicks spared thanks to in-ovo sexing (computed after aggregating, so that it is a ratio of sums).
    tb["share_of_males_spared"] = (
        100 * tb["male_embryos_removed"] / (tb["male_embryos_removed"] + tb["male_chickens_killed"])
    )

    # Add the number of chickens killed per 100 eggs produced.
    tb["chickens_killed_per_100_eggs"] = 100 * tb["chickens_killed"] / tb["eggs_produced"]

    # Add per capita indicators.
    tb = paths.regions.add_per_capita(tb=tb, columns=COUNT_COLUMNS, warn_on_missing_countries=False)

    # Run sanity checks on outputs.
    sanity_check_outputs(tb=tb, tb_qcl=tb_qcl)

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()


def select_faostat_data(tb_qcl: Table) -> Table:
    """Extract the number of laying hens and the number of eggs produced from the FAOSTAT QCL table."""
    tb_qcl = tb_qcl[tb_qcl["item_code"] == FAOSTAT_ITEM_CODE_HEN_EGGS]
    error = "Unexpected units in FAOSTAT data."
    assert set(tb_qcl[tb_qcl["element_code"] == FAOSTAT_ELEMENT_CODE_LAYING]["unit"]) == {"animals"}, error
    assert set(tb_qcl[tb_qcl["element_code"] == FAOSTAT_ELEMENT_CODE_EGGS_PRODUCED]["unit"]) == {"eggs"}, error

    columns = {FAOSTAT_ELEMENT_CODE_LAYING: "laying_hens", FAOSTAT_ELEMENT_CODE_EGGS_PRODUCED: "eggs_produced"}
    tb = tb_qcl[tb_qcl["element_code"].isin(columns)][["country", "year", "element_code", "value"]]
    tb = tb.pivot(index=["country", "year"], columns="element_code", values="value", join_column_levels_with="_")
    tb = tb.rename(columns=columns, errors="raise")
    tb = tb.astype({"country": str}).reset_index(drop=True)

    # Only keep rows with a number of laying hens (eggs produced is a secondary variable).
    tb = tb.dropna(subset=["laying_hens"]).reset_index(drop=True)

    return tb


def fill_gaps(tb: Table) -> Table:
    """Fill gaps in country series with the latest reported value, for at most MAX_YEARS_TO_FILL years.

    Historical regions (e.g. USSR) are not filled, otherwise they would overlap with their successors.
    """
    tb_regions = paths.regions.tb_regions
    historical = set(tb_regions[tb_regions["is_historical"]]["name"])
    columns = ["laying_hens", "eggs_produced"]

    tb_current = tb[~tb["country"].isin(historical)]
    tb_current = (
        tb_current.set_index(["country", "year"])
        .reindex(
            pd.MultiIndex.from_product(
                [tb_current["country"].unique(), range(tb["year"].min(), tb["year"].max() + 1)],
                names=["country", "year"],
            )
        )
        .reset_index()
        .sort_values(["country", "year"])
    )
    for column in columns:
        tb_current[column] = tb_current.groupby("country")[column].transform(lambda x: x.ffill(limit=MAX_YEARS_TO_FILL))
    tb_current = tb_current.dropna(subset=["laying_hens"])

    tb_filled = pr.concat([tb_current, tb[tb["country"].isin(historical)]], ignore_index=True)

    return tb_filled


def prepare_in_ovo_shares(tb_in_ovo: Table, tb: Table) -> Table:
    """Create a table with the share (0 to 1) of laying hens sexed in ovo, for each country and year.

    The producer reports a share for the EU as a whole, and for a few non-EU countries. Each observation is assigned to
    the calendar year of its date. The EU number of in-ovo sexed hens (taken from the producer where given, otherwise
    estimated as their share of all hens times the number of laying hens in the EU) is then allocated among the EU
    countries that have banned chick culling.
    """
    tb_in_ovo = tb_in_ovo.copy()
    tb_in_ovo["year"] = tb_in_ovo["date"].str[:4].astype(int)

    # For countries reported directly, use the central estimate, or the midpoint of the range if there is none.
    share = tb_in_ovo["share_of_commercial_hens_sexed_in_ovo"].astype("Float64")
    share_from_range = (
        tb_in_ovo["share_of_commercial_hens_sexed_in_ovo_low"] + tb_in_ovo["share_of_commercial_hens_sexed_in_ovo_high"]
    ) / 2
    tb_in_ovo["share_in_ovo"] = share.fillna(share_from_range) / 100
    tb_in_ovo["share_in_ovo"] = tb_in_ovo["share_in_ovo"].copy_metadata(
        tb_in_ovo["share_of_commercial_hens_sexed_in_ovo"]
    )
    tb_reported = tb_in_ovo[tb_in_ovo["country"].isin(IN_OVO_COUNTRIES_REPORTED)][["country", "year", "share_in_ovo"]]
    tb_reported = tb_reported.dropna(subset=["share_in_ovo"])
    error = "Each reported country should have at most one observation per year."
    assert not tb_reported.duplicated(subset=["country", "year"]).any(), error

    # For the EU, use the producer's number of in-ovo sexed hens where given. Otherwise, estimate it as their share of
    # all hens (including backyard flocks, which matches the scope of FAOSTAT's laying hens) times FAOSTAT's EU flock.
    tb_eu = tb_in_ovo[(tb_in_ovo["country"] == EU)][["year", "hens_sexed_in_ovo", "share_of_all_hens_sexed_in_ovo"]]
    tb_eu = tb_eu.dropna(subset=["hens_sexed_in_ovo", "share_of_all_hens_sexed_in_ovo"], how="all")
    error = "The EU should have at most one observation per year."
    assert not tb_eu["year"].duplicated().any(), error
    tb_eu = tb_eu.merge(
        tb[tb["country"] == EU][["year", "laying_hens"]].rename(columns={"laying_hens": "eu_laying_hens"}),
        on="year",
        how="inner",
    )
    estimated = tb_eu["share_of_all_hens_sexed_in_ovo"] / 100 * tb_eu["eu_laying_hens"]
    tb_eu["eu_hens_in_ovo"] = tb_eu["hens_sexed_in_ovo"].astype("Float64").fillna(estimated)
    tb_eu["eu_hens_in_ovo"] = tb_eu["eu_hens_in_ovo"].copy_metadata(tb_eu["share_of_all_hens_sexed_in_ovo"])

    # Allocate the EU in-ovo sexed hens among the countries assumed to receive them, in proportion to their flocks.
    tb_allocated = tb[tb["country"].isin(EU_IN_OVO_COUNTRIES)].copy()
    tb_allocated = tb_allocated[tb_allocated["year"] >= tb_allocated["country"].map(EU_IN_OVO_COUNTRIES).astype(int)]
    tb_allocated = tb_allocated.merge(tb_eu[["year", "eu_hens_in_ovo"]], on="year", how="inner")
    tb_allocated["share_in_ovo"] = tb_allocated["eu_hens_in_ovo"] / tb_allocated.groupby("year")[
        "laying_hens"
    ].transform("sum")
    tb_allocated["share_in_ovo"] = tb_allocated["share_in_ovo"].copy_metadata(tb_eu["eu_hens_in_ovo"])
    error = (
        "The EU in-ovo sexed hens exceed the laying hens of the countries they are allocated to. Revisit the allocation "
        f"in EU_IN_OVO_COUNTRIES:\n{tb_allocated[tb_allocated['share_in_ovo'] > 1]}"
    )
    assert (tb_allocated["share_in_ovo"] <= 1).all(), error

    tb_shares = pr.concat([tb_reported, tb_allocated[["country", "year", "share_in_ovo"]]], ignore_index=True)
    error = "Shares of hens sexed in ovo should be between 0 and 1."
    assert tb_shares["share_in_ovo"].between(0, 1).all(), error

    return tb_shares


def estimate_chickens_killed(tb: Table) -> Table:
    """Estimate the number of hens and male chickens killed each year from the number of laying hens."""
    # Hens killed each year: all hens in the flock are replaced (and killed) every WEEKS_IN_LAY weeks.
    tb["hens_killed"] = tb["laying_hens"] * 52 / WEEKS_IN_LAY
    # Male chicks hatched alongside the female chicks that replace those hens.
    males = tb["hens_killed"] * MALES_PER_FEMALE
    # Male embryos removed before hatching thanks to in-ovo sexing.
    tb["male_embryos_removed"] = males * tb["share_in_ovo"]
    # Male chickens killed, either as day-old chicks, or later on if they were raised for meat.
    tb["male_chickens_killed"] = males - tb["male_embryos_removed"]
    # All chickens killed to produce eggs.
    tb["chickens_killed"] = tb["hens_killed"] + tb["male_chickens_killed"]

    tb = tb.drop(columns=["share_in_ovo"])

    return tb


def sanity_check_outputs(tb: Table, tb_qcl: Table) -> None:
    # Aggregates can be missing in years without country data; the checks below apply to informed rows.
    tb = tb.dropna(subset=["laying_hens"] + COUNT_COLUMNS)
    error = "All numbers should be non-negative."
    assert (tb[["laying_hens"] + COUNT_COLUMNS] >= 0).all().all(), error

    error = "Total chickens killed should be the sum of hens and male chickens killed."
    residual = (tb["chickens_killed"] - tb["hens_killed"] - tb["male_chickens_killed"]).abs()
    assert (residual <= 1e-6 * tb["chickens_killed"] + 1e-6).all(), error

    error = "Hens killed should be fewer than laying hens (the flock turns over in more than a year)."
    assert (tb["hens_killed"] <= tb["laying_hens"]).all(), error

    error = "Male chickens killed should never exceed the number of male chicks hatched."
    assert (tb["male_chickens_killed"] <= tb["hens_killed"] * MALES_PER_FEMALE * (1 + 1e-6)).all(), error

    error = "The share of males spared should be between 0 and 100."
    assert tb["share_of_males_spared"].between(0, 100).all(), error

    # A hen lays a few hundred eggs per year, so there should be roughly one chicken killed per 100 eggs. Much larger
    # values point to inconsistent FAOSTAT data (e.g. Benin reports about 7 eggs per hen from 1991 to 2015).
    error = "Chickens killed per 100 eggs should be a small number."
    assert tb["chickens_killed_per_100_eggs"].dropna().between(0, 100).all(), error
    suspicious = tb[tb["chickens_killed_per_100_eggs"] > 10]
    if not suspicious.empty:
        log.warning(
            "Countries with more than 10 chickens killed per 100 eggs (inconsistent FAOSTAT data on hens and eggs): "
            f"{suspicious.groupby('country')['year'].agg(['min', 'max']).to_dict('index')}"
        )

    # Our World and EU aggregates should be close to FAOSTAT's own figures (which include missing countries).
    for region in ["World", EU]:
        fao = tb_qcl[
            (tb_qcl["country"] == region)
            & (tb_qcl["item_code"] == FAOSTAT_ITEM_CODE_HEN_EGGS)
            & (tb_qcl["element_code"] == FAOSTAT_ELEMENT_CODE_LAYING)
        ].set_index("year")["value"]
        owid = tb[tb["country"] == region].set_index("year")["laying_hens"]
        ratio = (owid / fao).dropna()
        error = f"Our {region} total of laying hens deviates by more than 5% from FAOSTAT's:\n{ratio[(ratio - 1).abs() > 0.05]}"
        assert ((ratio - 1).abs() <= 0.05).all(), error
