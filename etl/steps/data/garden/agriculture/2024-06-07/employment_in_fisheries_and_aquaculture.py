"""Load the employment in fisheries and aquaculture meadow dataset and create a garden dataset."""

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

# Map the source's subsector labels to indicator column names.
SUBSECTOR_TO_COLUMN = {
    "Aquaculture": "aquaculture",
    "Inland fisheries": "inland_fisheries",
    "Marine fisheries": "marine_fisheries",
    "Unspecified": "unspecified",
    "Fisheries and aquaculture, total": "total",
}

# Label of the source row that holds the grand total across subsectors.
TOTAL_SUBSECTOR = "Fisheries and aquaculture, total"


def _assert_reconciles(tb: Table, part_mask, total_mask, groupby: list[str], what: str) -> None:
    """Assert the summed parts equal the reported totals in every group, within tolerance."""
    parts = tb[part_mask].groupby(groupby, observed=True)["employment_thousands"].sum()
    totals = tb[total_mask].set_index(groupby)["employment_thousands"].reindex(parts.index)
    # Cast to signed numpy ints: the source column is unsigned, so a bare subtraction would underflow.
    diff = parts.to_numpy().astype("int64") - totals.to_numpy().astype("int64")
    # Reconciliation tolerance, in thousands of people: the source is rounded to thousands, so summed parts can differ from a reported total by a couple of thousand.
    assert (abs(diff) <= 3).all(), f"Reconciliation failed: {what}."


def sanity_check_inputs(tb: Table) -> None:
    # The table is transcribed by hand, so check it reconciles. harmonize_names separately warns if a
    # region is missing from or unused in the country mapping, so the region set is not checked here.
    _assert_reconciles(
        tb, tb["region"] != "World", tb["region"] == "World", ["subsector", "period"], "continents vs world total"
    )
    _assert_reconciles(
        tb,
        tb["subsector"] != TOTAL_SUBSECTOR,
        tb["subsector"] == TOTAL_SUBSECTOR,
        ["region", "period"],
        "subsectors vs grand total",
    )


def sanity_check_outputs(tb: Table) -> None:
    # Capture fisheries is derived as inland + marine fisheries.
    assert (tb["capture_fisheries"] == tb["inland_fisheries"] + tb["marine_fisheries"]).all(), (
        "Capture fisheries does not equal inland + marine fisheries."
    )
    # Magnitude guard for the thousands -> people conversion (the world total is ~62 million in 2022).
    world_total_2022 = tb.loc[("World", 2022), "total"]
    assert 5e7 < world_total_2022 < 7e7, f"World total in 2022 out of the expected range: {world_total_2022}."


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

    # Harmonize FAO's region names to their OWID entity names (see the .countries.json mapping).
    tb["region"] = tb["region"].astype("string")
    tb = tb.rename(columns={"region": "country"})
    tb = paths.regions.harmonize_names(tb, countries_file=paths.country_mapping_path)

    # Relabel periods and subsectors (key columns, not indicators).
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
