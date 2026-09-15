"""Create a garden dataset on the adoption of in-ovo sexing in egg production.

The dataset has two tables:
- The producer's estimates as published, one row per country and date of the estimate.
- The share of laying hens sexed in ovo for every country and year, which is what other steps need. The producer
  reports a share for a few countries and a total for the European Union; the EU total is allocated among the member
  countries that have banned the culling of male chicks, in proportion to their laying flocks (taken from FAOSTAT). All
  other countries and years get a share of zero, since the technology is not used there.
"""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Metrics expected in the snapshot, and how their central value and bounds are named in the output.
METRICS = [
    "share_of_commercial_hens_sexed_in_ovo",
    "share_of_all_hens_sexed_in_ovo",
    "hens_sexed_in_ovo",
    "female_chicks_produced_with_in_ovo_sexing",
    "cumulative_male_embryos_removed",
]

# Name of the EU aggregate in the producer's data and in FAOSTAT.
EU = "European Union (27)"
# Entities in the producer's data that are not countries with a directly reported share.
AGGREGATES = [EU, "World"]

# Countries assumed to receive the in-ovo sexed hens of the European Union, and the first year they do.
# NOTE: The producer only publishes a total for the EU. Until they provide country figures, we allocate that total among
# the countries that have banned (or agreed to phase out) the culling of male chicks, from the year before the ban took
# effect (when hatcheries started the transition), in proportion to their number of laying hens. Germany is included
# from the start, since the technology was first rolled out for its market.
EU_IN_OVO_COUNTRIES = {
    "Germany": 2019,
    "France": 2022,
    "Austria": 2022,
    "Netherlands": 2026,
    "Italy": 2027,
}

# FAOSTAT item and element codes for the number of laying hens.
FAOSTAT_ITEM_CODE_HEN_EGGS = "00001062"
FAOSTAT_ELEMENT_CODE_LAYING = "005313"
# Gaps of at most this many years in a country's FAOSTAT series are filled with its latest reported value; longer gaps
# are left empty.
MAX_YEARS_TO_FILL = 5


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("in_ovo_sexing_market_penetration")
    tb = ds_meadow.read("in_ovo_sexing_market_penetration")

    # Load FAOSTAT QCL dataset and read its main (long) table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl", safe_types=False)

    #
    # Process data.
    #
    sanity_check_inputs(tb=tb)

    # Reshape from one row per metric to one column per metric (with separate columns for the bounds of the range).
    tb = tb.pivot(
        index=["country", "date"],
        columns="metric",
        values=["value", "value_low", "value_high"],
        join_column_levels_with="__",
        fill_dimensions=False,
    )
    tb = tb.rename(
        columns={
            f"{prefix}__{metric}": metric + suffix
            for metric in METRICS
            for prefix, suffix in [("value", ""), ("value_low", "_low"), ("value_high", "_high")]
        },
        errors="raise",
    )
    # Drop bound columns that are empty for all rows.
    tb = tb.dropna(axis=1, how="all")

    # Convert millions to units, and round away the float32 noise introduced when the meadow table was stored.
    for column in tb.drop(columns=["country", "date"]).columns:
        if column.startswith("share_"):
            tb[column] = tb[column].astype("Float64").round(1)
        else:
            tb[column] = (tb[column].astype("Float64") * 1e6).round(-2)

    # Estimate the share of laying hens sexed in ovo for the countries and years where the technology is used.
    tb_laying_hens = select_laying_hens(tb_qcl=tb_qcl)
    tb_shares = estimate_shares_by_country(tb=tb, tb_laying_hens=tb_laying_hens)

    # Extend to all countries and years, with a share of zero where the technology is not used.
    tb_shares = extend_to_all_countries_and_years(tb_shares=tb_shares, tb_laying_hens=tb_laying_hens)

    # Improve table format.
    tb = tb.format(keys=["country", "date"], short_name=paths.short_name)
    tb_shares = tb_shares.format(short_name="share_of_hens_sexed_in_ovo")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_shares])

    # Save changes in the new garden dataset.
    ds_garden.save()


def select_laying_hens(tb_qcl: Table) -> Table:
    """Extract the number of laying hens from FAOSTAT, filling short gaps in each country's series."""
    tb = tb_qcl[
        (tb_qcl["item_code"] == FAOSTAT_ITEM_CODE_HEN_EGGS) & (tb_qcl["element_code"] == FAOSTAT_ELEMENT_CODE_LAYING)
    ]
    error = "Unexpected units in FAOSTAT data."
    assert set(tb["unit"]) == {"animals"}, error
    tb = tb[["country", "year", "value"]].rename(columns={"value": "laying_hens"}).astype({"country": str})

    # Fill gaps in each country's series with its latest reported value, for at most MAX_YEARS_TO_FILL years.
    tb = (
        tb.set_index(["country", "year"])
        .reindex(
            pd.MultiIndex.from_product(
                [tb["country"].unique(), range(tb["year"].min(), tb["year"].max() + 1)], names=["country", "year"]
            )
        )
        .reset_index()
        .sort_values(["country", "year"])
    )
    missing = tb["laying_hens"].isna()
    gap_length = missing.groupby([tb["country"], (missing != missing.shift()).cumsum()]).transform("size")
    short_gap = missing & (gap_length <= MAX_YEARS_TO_FILL)
    tb.loc[short_gap, "laying_hens"] = tb.groupby("country")["laying_hens"].ffill()[short_gap]
    tb = tb.dropna(subset=["laying_hens"]).reset_index(drop=True)

    return tb


def estimate_shares_by_country(tb: Table, tb_laying_hens: Table) -> Table:
    """Create a table with the share (0 to 100) of laying hens sexed in ovo, for each country and year.

    Each observation is assigned to the calendar year of its date. For the countries reported directly, the share is
    the producer's central estimate, or the midpoint of the range if there is none. For the EU, the number of in-ovo
    sexed hens (taken from the producer where given, otherwise estimated as their share of all hens times the number of
    laying hens in the EU) is allocated among the EU countries that have banned chick culling.
    """
    tb = tb.copy()
    tb["year"] = tb["date"].str[:4].astype(int)

    # Countries reported directly.
    share = tb["share_of_commercial_hens_sexed_in_ovo"].astype("Float64")
    share_from_range = (
        tb["share_of_commercial_hens_sexed_in_ovo_low"] + tb["share_of_commercial_hens_sexed_in_ovo_high"]
    ) / 2
    tb["share_of_hens_sexed_in_ovo"] = share.fillna(share_from_range)
    tb["share_of_hens_sexed_in_ovo"] = tb["share_of_hens_sexed_in_ovo"].copy_metadata(
        tb["share_of_commercial_hens_sexed_in_ovo"]
    )
    tb_reported = tb[~tb["country"].isin(AGGREGATES)][["country", "year", "share_of_hens_sexed_in_ovo"]]
    tb_reported = tb_reported.dropna(subset=["share_of_hens_sexed_in_ovo"])
    error = "Each reported country should have at most one observation per year."
    assert not tb_reported.duplicated(subset=["country", "year"]).any(), error

    # EU: number of in-ovo sexed hens per year.
    tb_eu = tb[tb["country"] == EU][["year", "hens_sexed_in_ovo", "share_of_all_hens_sexed_in_ovo"]]
    tb_eu = tb_eu.dropna(subset=["hens_sexed_in_ovo", "share_of_all_hens_sexed_in_ovo"], how="all")
    error = "The EU should have at most one observation per year."
    assert not tb_eu["year"].duplicated().any(), error
    tb_eu = tb_eu.merge(
        tb_laying_hens[tb_laying_hens["country"] == EU][["year", "laying_hens"]].rename(
            columns={"laying_hens": "eu_laying_hens"}
        ),
        on="year",
        how="inner",
    )
    estimated = tb_eu["share_of_all_hens_sexed_in_ovo"] / 100 * tb_eu["eu_laying_hens"]
    tb_eu["eu_hens_in_ovo"] = tb_eu["hens_sexed_in_ovo"].astype("Float64").fillna(estimated)
    tb_eu["eu_hens_in_ovo"] = tb_eu["eu_hens_in_ovo"].copy_metadata(tb_eu["share_of_all_hens_sexed_in_ovo"])

    # Allocate the EU in-ovo sexed hens among the countries assumed to receive them, in proportion to their flocks.
    tb_allocated = tb_laying_hens[tb_laying_hens["country"].isin(EU_IN_OVO_COUNTRIES)].copy()
    tb_allocated = tb_allocated[tb_allocated["year"] >= tb_allocated["country"].map(EU_IN_OVO_COUNTRIES).astype(int)]
    tb_allocated = tb_allocated.merge(tb_eu[["year", "eu_hens_in_ovo"]], on="year", how="inner")
    error = "Some EU in-ovo sexed hens cannot be allocated to any country. Revisit EU_IN_OVO_COUNTRIES."
    assert set(tb_eu["year"]) == set(tb_allocated["year"]), error
    tb_allocated["share_of_hens_sexed_in_ovo"] = (
        100 * tb_allocated["eu_hens_in_ovo"] / tb_allocated.groupby("year")["laying_hens"].transform("sum")
    )
    tb_allocated["share_of_hens_sexed_in_ovo"] = tb_allocated["share_of_hens_sexed_in_ovo"].copy_metadata(
        tb_eu["eu_hens_in_ovo"]
    )
    error = (
        "The EU in-ovo sexed hens exceed the laying hens of the countries they are allocated to. Revisit the allocation "
        f"in EU_IN_OVO_COUNTRIES:\n{tb_allocated[tb_allocated['share_of_hens_sexed_in_ovo'] > 100]}"
    )
    assert (tb_allocated["share_of_hens_sexed_in_ovo"] <= 100).all(), error

    tb_shares = pr.concat(
        [tb_reported, tb_allocated[["country", "year", "share_of_hens_sexed_in_ovo"]]], ignore_index=True
    )
    error = "Shares of hens sexed in ovo should be between 0 and 100."
    assert tb_shares["share_of_hens_sexed_in_ovo"].between(0, 100).all(), error

    return tb_shares


def extend_to_all_countries_and_years(tb_shares: Table, tb_laying_hens: Table) -> Table:
    """Extend the table to all countries (including historical ones) and all years, with zeros where not used.

    The years span the FAOSTAT laying hens series and the producer's estimates.
    """
    tb_regions = paths.regions.tb_regions
    countries = sorted(tb_regions[tb_regions["region_type"] == "country"]["name"])
    error = f"Unknown countries in the in-ovo sexing shares: {set(tb_shares['country']) - set(countries)}"
    assert set(tb_shares["country"]) <= set(countries), error
    years = range(tb_laying_hens["year"].min(), max(tb_laying_hens["year"].max(), tb_shares["year"].max()) + 1)

    tb_all = Table(pd.MultiIndex.from_product([countries, years], names=["country", "year"]).to_frame(index=False))
    tb_all = tb_all.merge(tb_shares, on=["country", "year"], how="left")
    tb_all["share_of_hens_sexed_in_ovo"] = tb_all["share_of_hens_sexed_in_ovo"].fillna(0)

    return tb_all


def sanity_check_inputs(tb: Table) -> None:
    error = f"Unexpected metrics in the snapshot: {set(tb['metric']) - set(METRICS)}"
    assert set(tb["metric"]) == set(METRICS), error

    error = "Shares should be between 0 and 100."
    shares = tb[tb["metric"].str.startswith("share_")]
    values = shares[["value", "value_low", "value_high"]].fillna(0)
    assert ((values >= 0) & (values <= 100)).all().all(), error

    error = "Ranges should contain the central value."
    ranges = tb.dropna(subset=["value", "value_low", "value_high"])
    assert ((ranges["value_low"] <= ranges["value"]) & (ranges["value"] <= ranges["value_high"])).all(), error

    error = "The cumulative series should be non-decreasing."
    cumulative = tb[tb["metric"] == "cumulative_male_embryos_removed"].sort_values("date")
    assert cumulative["value"].is_monotonic_increasing, error
