"""Load the two meadow datasets of historical US polio deaths and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Years taken from CDC WONDER's provisional mortality data are provisional: the CDC has not yet finalized them.
# The CDC snapshot links each of those years to this page.
PROVISIONAL_SOURCE = "https://wonder.cdc.gov/mcd-icd10-provisional.html"

# NOTE: The metadata (description_key_provisional and the chart footnotes) names these years. When the provisional
# years change, update that text too.
PROVISIONAL_YEARS = [2025]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets and read their tables.
    tb_phr = paths.load_dataset("polio_deaths", namespace="public_health_reports").read("polio_deaths")
    tb_cdc = paths.load_dataset("polio_deaths", namespace="cdc").read("polio_deaths")

    #
    # Process data.
    #
    sanity_check_inputs(tb_phr=tb_phr, tb_cdc=tb_cdc)

    # Combine the two sources, which cover different years.
    tb = pr.concat([tb_phr, tb_cdc], axis=0, ignore_index=True)
    tb = tb.drop(columns=["source"])

    # Add the number of deaths per 100,000 people.
    tb = paths.regions.add_population(tb, population_col="population", warn_on_missing_countries=False)
    tb["death_rate"] = tb["deaths"] / tb["population"] * 100000
    tb = tb.drop(columns=["population"])

    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name="polio_deaths")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb_phr: Table, tb_cdc: Table) -> None:
    tables = {"Public Health Reports": tb_phr, "CDC": tb_cdc}
    for name, tb in tables.items():
        assert set(tb.columns) == {"country", "year", "deaths", "source"}, (
            f"{name}: unexpected columns {set(tb.columns)}."
        )
        assert set(tb["country"]) == {"United States"}, f"{name}: unexpected countries {set(tb['country'])}."
        assert (tb["deaths"] >= 0).all(), f"{name}: negative death counts."
        assert tb["deaths"].notna().all(), f"{name}: missing death counts."

    # Each year must come from exactly one source.
    years = [year for tb in tables.values() for year in tb["year"]]
    duplicated = sorted({year for year in years if years.count(year) > 1})
    assert not duplicated, f"Years reported by more than one source: {duplicated}."

    # Provisional years come only from the CDC, and are the ones the metadata names.
    assert not (tb_phr["source"] == PROVISIONAL_SOURCE).any(), "Provisional years in Public Health Reports."
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
    assert tb["death_rate"].notna().sum() == tb["deaths"].notna().sum(), (
        "Death rate missing for some years with deaths."
    )

    # The highest values are around 7,000 deaths (1916), about 7 per 100,000 people.
    assert tb["deaths"].max() < 10_000, "Implausibly high death count."
    assert tb["death_rate"].max() < 15, "Implausibly high death rate."
