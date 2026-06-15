"""Garden step for the historical Ebola outbreak chronology (CDC).

Harmonizes country names and aggregates to one row per country and year (a handful of country-years
had two separate outbreaks in the meadow table; here their cases and deaths are summed and the case
fatality rate is recomputed from the totals).
"""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

paths = PathFinder(__file__)
log = get_logger()


def sanity_check_inputs(tb: Table) -> None:
    assert tb["species"].str.startswith("Orthoebolavirus").all(), "Unexpected Ebola species in input."
    both = tb.dropna(subset=["cases", "deaths"])
    assert (both["deaths"] <= both["cases"]).all(), "Deaths exceed cases in an input outbreak."


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Country-year is not unique after aggregation."
    assert (tb["deaths"].dropna() <= tb["cases"].dropna()).all(), "Aggregated deaths exceed cases."
    cfr = tb["case_fatality_rate"].dropna()
    assert ((cfr >= 0) & (cfr <= 100)).all(), "Case fatality rate outside [0, 100]."
    # The CDC chronology should always reach back to the first known outbreaks in 1976.
    assert tb["year"].min() == 1976, f"Earliest outbreak year is {tb['year'].min()}, expected 1976."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("ebola_outbreaks")
    tb = ds_meadow["ebola_outbreaks"].reset_index()

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Capture the source origin to reattach after groupby (aggregation drops column metadata).
    origins = tb["cases"].metadata.origins

    # Collapse the few country-years with multiple outbreaks into a single total per country-year.
    species = (
        tb.groupby(["country", "year"], observed=True)["species"].agg(lambda s: ", ".join(sorted(set(s)))).reset_index()
    )
    tb = tb.groupby(["country", "year"], observed=True).agg(
        cases=("cases", "sum"),
        deaths=("deaths", "sum"),
        outbreaks=("outbreak_index", "size"),
    )
    tb = tb.reset_index().merge(species, on=["country", "year"])

    # Recompute the case fatality rate from the totals (the source's per-outbreak percentages
    # can't simply be added when two outbreaks share a country-year).
    tb["case_fatality_rate"] = (tb["deaths"] / tb["cases"]) * 100

    # Reattach the source origin to every indicator (groupby/derivation dropped it).
    for col in ["cases", "deaths", "outbreaks", "species", "case_fatality_rate"]:
        tb[col].metadata.origins = origins

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name="ebola_outbreaks")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
