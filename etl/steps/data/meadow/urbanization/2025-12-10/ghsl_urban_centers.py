"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# City size cutoffs (in population).
CITY_SIZE_CUTOFFS = {
    "50k_300k": (50000, 300000),
    "50k_1m": (50000, 1000000),
    "1m_5m": (1000000, 5000000),
    "5m_10m": (5000000, 10000000),
    "above_10m": (10000000, float("inf")),
}

# The source rates the plausibility of every urban centre's population figure:
# 0 = low, 1 = moderate, 2 = high, 3 = very high.
#
# We drop the values it rates "low", but only for the indicators that describe ONE named city
# (capital, largest city, top 100). There, a low-plausibility figure is a false claim about a
# specific place: in Angola, for instance, the source puts ~1 million people in the small town of
# Uíge while Luanda's urban centre is absent until 1995, and it flags exactly those figures as low.
#
# The city-size aggregates below are deliberately NOT filtered. They sum every city in a country, so
# dropping a misplaced city would remove people who really do live in that country and make the
# totals too low. Filtering them would also be far more destructive than it looks: 38 country-years
# would lose every one of their cities, and several countries would lose a fifth to two-thirds of
# their urban population.
LOW_PLAUSIBILITY = 0


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ghsl_urban_centers.xlsx")

    # Load data from snapshot (only once).
    tb_raw = snap.read(safe_types=False, sheet_name="UC_STATS")

    # Process data.
    #
    # Rename columns to be more interpretable.
    tb_raw = tb_raw.rename(
        columns={
            "UNLocName": "country",
            "UCname": "urban_center_name",
            "Year": "year",
            "AREA_km2": "urban_area",
            "POP": "urban_pop",
            "BU_km2": "built_up_area",
            "CapitalFlag": "capital",
            "ID_UC_G0": "ID_MTUC_G0",
            "Plausibility": "plausibility",
        }
    )

    # Replace zeros with NaNs in the urban_pop column (when the urban center did not meet the criteria).
    tb_raw["urban_pop"] = tb_raw["urban_pop"].replace(0, pd.NA)
    # Convert the urban_pop column to a numeric dtype.
    tb_raw["urban_pop"] = pd.to_numeric(tb_raw["urban_pop"], errors="coerce")

    # Filter the Table where urban_center_name is NaN or "N/A".
    tb_raw = tb_raw.dropna(subset=["urban_center_name"])
    tb_raw = tb_raw[tb_raw["urban_center_name"] != "N/A"]

    # Sanity check: the plausibility flag must be present and use the documented 0-3 scale, since we
    # rely on it below to decide which per-city figures to publish.
    assert "plausibility" in tb_raw.columns, "Source is missing the Plausibility column."
    unexpected_flags = set(tb_raw["plausibility"].dropna().unique()) - {0, 1, 2, 3}
    assert not unexpected_flags, f"Unexpected plausibility flags: {sorted(unexpected_flags)}"

    # Create working table with selected columns for capitals/top 100.
    tb = tb_raw[
        ["ID_MTUC_G0", "country", "urban_center_name", "capital", "year", "urban_pop", "urban_area", "plausibility"]
    ].copy()

    # Calculate urban density.
    tb["urban_density"] = tb["urban_pop"] / tb["urban_area"]

    # Population and density of the capital city.
    # Some countries have multiple capitals - select the official/administrative capital.
    tb_capitals = tb[tb["capital"] == 1].copy()

    # Define which capital to use for countries with multiple capitals.
    capital_preference = {
        "Benin": "Porto-Novo",
        "Bolivia (Plurinational State of)": "La Paz",
        "Burundi": "Gitega",
        "Chile": "Santiago",
        "Côte d'Ivoire": "Yamoussoukro",
        "Malaysia": "Putrajaya",
        "Netherlands": "The Hague",
        "United Republic of Tanzania": "Dodoma",
        "Yemen": "Şan'ā' (Sana'a)",
        "India": "New Delhi",
        "Pakistan": "Islāmābād",
        "South Africa": "Pretoria",
    }

    # Filter to keep only preferred capitals for multi-capital countries (vectorized).
    multi_capital_mask = tb_capitals["country"].isin(capital_preference.keys())

    # For multi-capital countries, create a mask for preferred capitals.
    tb_capitals["preferred_capital"] = tb_capitals["country"].map(capital_preference)
    preferred_mask = tb_capitals["urban_center_name"] == tb_capitals["preferred_capital"]

    # Keep all single-capital countries OR preferred capitals from multi-capital countries.
    tb_capitals = tb_capitals[~multi_capital_mask | preferred_mask].copy()
    tb_capitals = tb_capitals.drop(columns=["ID_MTUC_G0", "capital", "preferred_capital"])

    # Drop the capital-city figures the source rates as low plausibility.
    # A single-step drop is safe here: the capital is identified by the source's own capital flag (and
    # our preference list above), never by ranking cities on population, so removing a capital's row
    # cannot promote another city into its place. It just leaves a gap.
    tb_capitals = tb_capitals[tb_capitals["plausibility"] != LOW_PLAUSIBILITY].drop(columns=["plausibility"])

    # Population and density of the largest city by country and year (by population).
    # Take whole rows rather than a column-wise groupby().first(), so that the plausibility flag we
    # check next belongs to the same city as the population figure it is rating.
    tb_largest_city = (
        tb.dropna(subset=["urban_pop"])
        .sort_values(["country", "year", "urban_pop"], ascending=[True, True, False])
        .drop_duplicates(subset=["country", "year"], keep="first")[
            ["country", "year", "urban_pop", "urban_density", "plausibility"]
        ]
        .copy()
    )

    # Blank the largest-city figures the source rates as low plausibility -- and do NOT re-rank.
    # This indicator is derived by sorting a country's cities and taking the top one, so simply
    # dropping the flagged city would let the sort fall through and publish the SECOND-biggest city as
    # the largest. That is a worse error than the one we are fixing: Laos would be credited with Pakse
    # (142k) instead of Vientiane (237k) for 2050-2100. So we blank the country-year instead.
    implausible_largest = tb_largest_city["plausibility"] == LOW_PLAUSIBILITY
    tb_largest_city.loc[implausible_largest, ["urban_pop", "urban_density"]] = float("nan")
    tb_largest_city = tb_largest_city.drop(columns=["plausibility"])

    tb_largest_city = tb_largest_city.rename(
        columns={"urban_pop": "largest_city_pop", "urban_density": "largest_city_density"}
    )

    # Select the top 100 most populous cities in 2020.
    # The ranking is deliberately taken from the unfiltered data: it defines which cities the indicator
    # covers, and that membership should not depend on the quality flag of a single year's figure.
    tb_2020 = tb[tb["year"] == 2020]
    top_100_pop_2020 = tb_2020.nlargest(100, "urban_pop").drop_duplicates(subset=["ID_MTUC_G0"])

    # Filter the original Table to select the top urban centers.
    tb_top = tb[tb["ID_MTUC_G0"].isin(top_100_pop_2020["ID_MTUC_G0"])].copy()

    # Drop the individual city-year figures the source rates as low plausibility. Each row here is one
    # named city in one year, so a flagged figure can be removed on its own without affecting any other
    # city or year, and the city stays in the top 100.
    tb_top = tb_top[tb_top["plausibility"] != LOW_PLAUSIBILITY]

    tb_top = tb_top.drop(columns=["urban_area", "ID_MTUC_G0", "capital", "plausibility"])
    tb_top = tb_top.rename(columns={"urban_density": "urban_density_top_100", "urban_pop": "urban_pop_top_100"})

    # Format the country column for top 100.
    tb_top["country"] = tb_top["urban_center_name"] + " (" + tb_top["country"] + ")"
    tb_top = tb_top.drop(columns=["urban_center_name"])

    # Create city size aggregates using already-loaded data.
    tb_all_cities = tb_raw[["country", "year", "urban_pop"]].copy()

    # Drop missing population values.
    tb_all_cities = tb_all_cities.dropna(subset=["urban_pop"])
    tb_all_cities = tb_all_cities[tb_all_cities["urban_pop"] > 0]

    # Sanity check: all urban centres should have at least 50,000 people.
    below_50k = tb_all_cities[tb_all_cities["urban_pop"] < 50000]
    if not below_50k.empty:
        raise AssertionError(
            f"Found {len(below_50k)} cities with population below 50,000. "
            f"Urban centres should have at least 50,000 people.\n"
            f"Examples:\n{below_50k[['country', 'year', 'urban_pop']].head(10)}"
        )

    # Create columns for each city size category using vectorized operations.
    urban_pop = tb_all_cities["urban_pop"]
    for size_name, (min_pop, max_pop) in CITY_SIZE_CUTOFFS.items():
        mask = (urban_pop >= min_pop) & (urban_pop < max_pop)
        tb_all_cities[f"pop_{size_name}"] = urban_pop.where(mask, 0)

    # Add aggregate columns calculated directly from raw data.
    # 300k or more.
    tb_all_cities["pop_above_300k"] = urban_pop.where(urban_pop >= 300000, 0)
    # 1 million or more.
    tb_all_cities["pop_above_1m"] = urban_pop.where(urban_pop >= 1000000, 0)

    # Aggregate by country and year.
    agg_dict = {f"pop_{size_name}": "sum" for size_name in CITY_SIZE_CUTOFFS.keys()}
    agg_dict["pop_above_300k"] = "sum"
    agg_dict["pop_above_1m"] = "sum"
    tb_city_sizes = tb_all_cities.groupby(["country", "year"], as_index=False)[list(agg_dict.keys())].sum()

    # Merge capital, largest city, top 100, and city size tables.
    tb = pr.merge(tb_capitals, tb_largest_city, on=["country", "year"], how="outer")
    tb = pr.merge(tb, tb_top, on=["country", "year"], how="outer")
    tb = pr.merge(tb, tb_city_sizes, on=["country", "year"], how="outer")

    # Ensure metadata is propagated.
    metadata_cols = [
        "urban_pop",
        "urban_density",
        "largest_city_pop",
        "largest_city_density",
        "urban_density_top_100",
        "urban_pop_top_100",
    ]
    # Add city size columns.
    metadata_cols.extend([f"pop_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()])
    # Add aggregate columns.
    metadata_cols.extend(["pop_above_300k", "pop_above_1m"])

    for col in metadata_cols:
        if col in tb.columns:
            tb[col].metadata.origins = tb["country"].metadata.origins

    # Format the table.
    tb = tb.format(["country", "year"])

    #
    # Create a raw city-level table for matching purposes.
    # This one keeps the plausibility flag, unfiltered, so the producer's quality rating stays
    # inspectable downstream -- it is what lets us screen for cases like Uíge in the first place.
    #
    tb_cities_raw = tb_raw[["country", "urban_center_name", "year", "urban_pop", "plausibility"]].copy()
    tb_cities_raw = tb_cities_raw.dropna(subset=["urban_center_name", "urban_pop"])
    tb_cities_raw = tb_cities_raw[tb_cities_raw["urban_pop"] > 0]

    # Handle duplicate city names by keeping the first occurrence
    # (some cities appear multiple times with same name in different regions)
    tb_cities_raw = tb_cities_raw.drop_duplicates(subset=["country", "urban_center_name", "year"], keep="first")
    # Copy the list per column, so the three columns don't share one mutable origins list.
    for col in ["urban_pop", "plausibility"]:
        tb_cities_raw[col].metadata.origins = list(tb_cities_raw["country"].metadata.origins)

    tb_cities_raw = tb_cities_raw.format(["country", "urban_center_name", "year"], short_name="ghsl_urban_centers_raw")

    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb, tb_cities_raw], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
