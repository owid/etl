"""Load the three meadow datasets of historical US polio cases and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Years taken from CDC's weekly data are provisional: the CDC has not yet published its finalized annual tables
# for them. The CDC snapshot links each of those years to this page.
PROVISIONAL_SOURCE = "https://data.cdc.gov/NNDSS/NNDSS-Weekly-Data/x9gk-5huc"

# NOTE: The metadata (description_key_provisional and the chart footnotes) names these years. When the provisional
# years change, update that text too.
PROVISIONAL_YEARS = [2024, 2025]


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
    # From 1944 we use CDC's counts, so that 1944-1983 all come from one table (the CDC's 1993 summary of notifiable
    # diseases). Public Health Reports gives slightly different counts for some of the overlapping years 1944-1949.
    tb_phr = tb_phr[tb_phr["year"] < tb_cdc["year"].min()].reset_index(drop=True)

    sanity_check_inputs(tb_phr=tb_phr, tb_census=tb_census, tb_cdc=tb_cdc)

    # Combine the three sources, which cover different years.
    tb = pr.concat([tb_phr, tb_census, tb_cdc], axis=0, ignore_index=True)
    tb = tb.drop(columns=["source"])

    # Add the number of cases per 100,000 people.
    tb = paths.regions.add_population(tb, population_col="population", warn_on_missing_countries=False)
    tb["case_rate"] = tb["cases"] / tb["population"] * 100000
    tb = tb.drop(columns=["population"])

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

    # Provisional years come only from the CDC, and are the ones the metadata names.
    assert not (tb_phr["source"] == PROVISIONAL_SOURCE).any(), "Provisional years in Public Health Reports."
    assert not (tb_census["source"] == PROVISIONAL_SOURCE).any(), "Provisional years in US Census Bureau."
    provisional = tb_cdc["source"] == PROVISIONAL_SOURCE
    provisional_years = sorted(int(year) for year in tb_cdc.loc[provisional, "year"])
    assert provisional_years == PROVISIONAL_YEARS, (
        f"Provisional years {provisional_years} differ from PROVISIONAL_YEARS "
        f"{PROVISIONAL_YEARS}: update the constant and the metadata text that names them."
    )
    assert min(PROVISIONAL_YEARS) > tb_cdc.loc[~provisional, "year"].max(), (
        "Provisional CDC years must all come after the last finalized year."
    )


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert tb["case_rate"].notna().sum() == tb["cases"].notna().sum(), "Case rate missing for some years with cases."

    # The highest values are around 58,000 cases (1952, all reported cases), about 37 per 100,000 people.
    assert tb["cases"].max() < 60_000, "Implausibly high case count."
    assert tb["case_rate"].max() < 50, "Implausibly high case rate."
