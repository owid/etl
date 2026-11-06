"""Load the three meadow datasets of historical US polio cases and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Years taken from CDC's weekly data are provisional: the CDC has not yet published its finalized annual tables
# for them. The CDC snapshot links each of those years to this page.
PROVISIONAL_SOURCE = "https://data.cdc.gov/NNDSS/NNDSS-Weekly-Data/x9gk-5huc"

# Indicators that get a separate provisional series.
INDICATORS = ["cases", "case_rate"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets and read their tables.
    tb_phr = paths.load_dataset("polio_cases", namespace="public_health_reports").read("polio_cases")
    tb_census = paths.load_dataset("polio_cases", namespace="us_census_bureau").read("polio_cases")
    tb_cdc = paths.load_dataset("polio_cases", namespace="cdc").read("polio_cases")

    #
    # Process data.
    #
    sanity_check_inputs(tb_phr=tb_phr, tb_census=tb_census, tb_cdc=tb_cdc)

    # Combine the three sources, which cover different years.
    tb = pr.concat([tb_phr, tb_census, tb_cdc], axis=0, ignore_index=True)

    # Flag the provisional years before dropping the source links.
    tb["is_provisional"] = tb["source"] == PROVISIONAL_SOURCE
    tb = tb.drop(columns=["source"])

    # Add the number of cases per 100,000 people.
    tb = paths.regions.add_population(tb, population_col="population", warn_on_missing_countries=False)
    tb["case_rate"] = tb["cases"] / tb["population"] * 100000
    tb = tb.drop(columns=["population"])

    # Split each indicator into a finalized and a provisional series, so charts can show the provisional years dashed.
    tb = split_final_and_provisional(tb)

    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name="polio_cases")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()


def split_final_and_provisional(tb: Table) -> Table:
    """Move the provisional years of each indicator into a separate `<indicator>_provisional` column.

    The provisional series also holds the last finalized year, so that on a line chart the dashed provisional
    segment starts where the solid finalized line ends, e.g. final 1910-2023 and provisional 2023-2025.
    """
    last_final_year = tb.loc[~tb["is_provisional"], "year"].max()
    is_bridge_year = tb["year"] == last_final_year
    for indicator in INDICATORS:
        tb[f"{indicator}_provisional"] = tb[indicator].copy()
        tb.loc[~(tb["is_provisional"] | is_bridge_year), f"{indicator}_provisional"] = None
        tb.loc[tb["is_provisional"], indicator] = None
    tb = tb.drop(columns=["is_provisional"])

    return tb


def sanity_check_inputs(tb_phr: Table, tb_census: Table, tb_cdc: Table) -> None:
    tables = {"Public Health Reports": tb_phr, "US Census Bureau": tb_census, "CDC": tb_cdc}
    for name, tb in tables.items():
        assert set(tb.columns) == {"country", "year", "cases", "source"}, (
            f"{name}: unexpected columns {set(tb.columns)}."
        )
        assert set(tb["country"]) == {"United States"}, f"{name}: unexpected countries {set(tb['country'])}."
        assert (tb["cases"] >= 0).all(), f"{name}: negative case counts."
        assert tb["cases"].notna().all(), f"{name}: missing case counts."

    # Each year must come from exactly one source.
    years = [year for tb in tables.values() for year in tb["year"]]
    duplicated = sorted({year for year in years if years.count(year) > 1})
    assert not duplicated, f"Years reported by more than one source: {duplicated}."

    # Provisional years come only from the CDC, and only after its last finalized year.
    assert not (tb_phr["source"] == PROVISIONAL_SOURCE).any(), "Provisional years in Public Health Reports."
    assert not (tb_census["source"] == PROVISIONAL_SOURCE).any(), "Provisional years in US Census Bureau."
    provisional = tb_cdc["source"] == PROVISIONAL_SOURCE
    if provisional.any():
        assert tb_cdc.loc[provisional, "year"].min() > tb_cdc.loc[~provisional, "year"].max(), (
            "Provisional CDC years must all come after the last finalized year."
        )


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert tb["case_rate"].notna().sum() == tb["cases"].notna().sum(), "Case rate missing for some years with cases."
    assert tb["case_rate_provisional"].notna().sum() == tb["cases_provisional"].notna().sum(), (
        "Provisional case rate missing for some provisional years."
    )

    # Final and provisional series overlap only in the bridge year (the last finalized year).
    final_years = set(tb.loc[tb["cases"].notna(), "year"])
    provisional_years = set(tb.loc[tb["cases_provisional"].notna(), "year"])
    if provisional_years:
        assert final_years & provisional_years == {max(final_years)}, (
            "Final and provisional series should share only the last finalized year."
        )
        assert min(provisional_years) == max(final_years), "Provisional series should start at the last final year."

    # The highest values are around 42,000 cases (1949, all reported cases), about 29 per 100,000 people.
    assert tb[["cases", "cases_provisional"]].max().max() < 60_000, "Implausibly high case count."
    assert tb[["case_rate", "case_rate_provisional"]].max().max() < 50, "Implausibly high case rate."
