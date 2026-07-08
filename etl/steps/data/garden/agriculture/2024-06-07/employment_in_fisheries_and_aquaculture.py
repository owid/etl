"""Load the employment in fisheries and aquaculture meadow dataset and create a garden dataset.

"""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map the source's verbatim period labels to representative years.
# "2000s" and "2010s" are decade averages, plotted here at the decade midpoint.
PERIOD_TO_YEAR = {
    "1995": 1995,
    "2000s": 2005,
    "2010s": 2015,
    "2020": 2020,
    "2022": 2022,
}

# Map FAO's continental regions to their OWID entity names (FAO's own regional groupings).
REGION_TO_COUNTRY = {
    "Africa": "Africa (FAO)",
    "Asia": "Asia (FAO)",
    "Europe": "Europe (FAO)",
    "Latin America and the Caribbean": "Latin America and the Caribbean (FAO)",
    "Northern America": "Northern America (FAO)",
    "Oceania": "Oceania (FAO)",
    "World": "World",
}
# Continental regions only (i.e. excluding the world aggregate), used in sanity checks.
CONTINENTS = [v for k, v in REGION_TO_COUNTRY.items() if k != "World"]

# Map the source's subsector labels to indicator column names.
SUBSECTOR_TO_COLUMN = {
    "Aquaculture": "aquaculture",
    "Inland fisheries": "inland_fisheries",
    "Marine fisheries": "marine_fisheries",
    "Unspecified": "unspecified",
    "Fisheries and aquaculture, total": "total",
}

# Tolerance (in number of people) for reconciliation checks: the source is rounded to thousands, so
# component sums and regional sums can differ from the reported totals by a few thousand people.
RECONCILIATION_TOLERANCE = 3000


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["subsector"]) == set(SUBSECTOR_TO_COLUMN), "Unexpected set of subsectors in the source table."
    assert set(tb["region"]) == set(REGION_TO_COUNTRY), "Unexpected set of regions in the source table."
    assert set(tb["period"]) == set(PERIOD_TO_YEAR), "Unexpected set of periods in the source table."
    assert not tb.duplicated(subset=["region", "subsector", "period"]).any(), (
        "Duplicate (region, subsector, period) rows."
    )
    assert tb["employment_thousands"].min() >= 0, "Negative employment found in the source table."

    # For each subsector and period, the continents should sum to the reported world total.
    for subsector in SUBSECTOR_TO_COLUMN:
        for period in PERIOD_TO_YEAR:
            mask = (tb["subsector"] == subsector) & (tb["period"] == period)
            # Cast to plain int: the source column is unsigned, so a bare subtraction would underflow.
            world = int(tb[mask & (tb["region"] == "World")]["employment_thousands"].sum())
            continents = int(tb[mask & (tb["region"] != "World")]["employment_thousands"].sum())
            assert abs(world - continents) <= 3, (
                f"Continents do not sum to the world total for {subsector} in {period}: "
                f"world={world}, sum of continents={continents}."
            )


def sanity_check_outputs(tb: Table) -> None:
    indicators = ["aquaculture", "inland_fisheries", "marine_fisheries", "capture_fisheries", "unspecified", "total"]
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    assert (tb[indicators].min() >= 0).all(), "Negative employment found in the output."

    # Capture fisheries must equal inland + marine fisheries.
    assert (tb["capture_fisheries"] == tb["inland_fisheries"] + tb["marine_fisheries"]).all(), (
        "Capture fisheries does not equal inland + marine fisheries."
    )

    # The subsector components must reconcile with the reported total (treating missing values as 0).
    components = tb[["aquaculture", "inland_fisheries", "marine_fisheries", "unspecified"]].fillna(0).sum(axis=1)
    assert ((components - tb["total"]).abs() <= RECONCILIATION_TOLERANCE).all(), (
        "Subsector components do not reconcile with the reported total."
    )

    # For each year and indicator, the FAO continents must sum to the world value.
    tb_flat = tb.reset_index()
    for year in tb_flat["year"].unique():
        year_mask = tb_flat["year"] == year
        world = tb_flat[year_mask & (tb_flat["country"] == "World")]
        continents = tb_flat[year_mask & (tb_flat["country"].isin(CONTINENTS))]
        for indicator in ["aquaculture", "inland_fisheries", "marine_fisheries", "capture_fisheries", "total"]:
            diff = abs(world[indicator].sum() - continents[indicator].sum())
            assert diff <= RECONCILIATION_TOLERANCE, (
                f"FAO continents do not sum to the world value for {indicator} in {year} (diff={diff})."
            )

    # Magnitude check: this guards the thousands -> people conversion (world total is ~62 million).
    world_total_2022 = tb.loc[("World", 2022), "total"]
    assert 5e7 < world_total_2022 < 7e7, f"World total in 2022 out of expected range: {world_total_2022}."

    # Asia dominates global employment (~85% of the world total in 2022).
    asia_share = tb.loc[("Asia (FAO)", 2022), "total"] / world_total_2022
    assert 0.8 < asia_share < 0.9, f"Asia's share of world employment in 2022 out of expected range: {asia_share}."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("employment_in_fisheries_and_aquaculture")
    tb = ds_meadow["employment_in_fisheries_and_aquaculture"].reset_index()

    #
    # Process data.
    #
    sanity_check_inputs(tb)

    # Convert from thousands of people to number of people. Cast to a wider integer type first: the
    # source values are read as UInt16, which would overflow when multiplied by 1000.
    tb["employment"] = tb["employment_thousands"].astype("Int64") * 1000

    # Relabel regions, periods and subsectors (these are key columns, not indicators).
    tb["country"] = tb["region"].astype("string").map(REGION_TO_COUNTRY)
    tb["year"] = tb["period"].astype("string").map(PERIOD_TO_YEAR).astype(int)
    tb["subsector"] = tb["subsector"].astype("string").map(SUBSECTOR_TO_COLUMN)

    # Reshape so each subsector becomes its own indicator.
    tb = tb.pivot(index=["country", "year"], columns="subsector", values="employment", join_column_levels_with="_")

    # Add a derived "capture fisheries" indicator (inland + marine fisheries).
    tb["capture_fisheries"] = tb["inland_fisheries"] + tb["marine_fisheries"]

    # Set an index and sort.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
