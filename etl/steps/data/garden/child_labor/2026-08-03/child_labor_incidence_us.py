"""Load a meadow dataset and create a garden dataset.

The source reports youth (ages 10-15) labor-force participation rates in the United States
by sex, both as originally published in the census and as corrected by the authors. Values
are already percentages.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Source column -> descriptive indicator name.
COLUMNS = {
    "incidence_boys_pct": "incidence_boys",
    "incidence_girls_pct": "incidence_girls",
    "corrected_incidence_boys_pct": "corrected_incidence_boys",
    "corrected_incidence_girls_pct": "corrected_incidence_girls",
}
SHARE_COLUMNS = list(COLUMNS.values())


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["country"].unique()) == {"United States"}, "Expected United States as the only entity."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert tb["year"].between(1870, 1930).all(), "Year outside the plausible census range."
    for col in COLUMNS:
        assert tb[col].dropna().between(0, 100).all(), f"{col} has values outside 0-100%."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    for col in SHARE_COLUMNS:
        assert tb[col].dropna().between(0, 100).all(), f"{col} outside 0-100%."
    # The correction can only raise the published rate (it recovers undercounted young workers).
    assert (tb["corrected_incidence_boys"] >= tb["incidence_boys"] - 0.05).all(), "Corrected boys rate below published."
    assert (tb["corrected_incidence_girls"] >= tb["incidence_girls"] - 0.05).all(), (
        "Corrected girls rate below published."
    )
    # Boys' participation exceeds girls' in every census year, in both the published and corrected series.
    assert (tb["incidence_boys"] >= tb["incidence_girls"]).all(), "Published boys rate below girls rate."
    assert (tb["corrected_incidence_boys"] >= tb["corrected_incidence_girls"]).all(), (
        "Corrected boys rate below girls rate."
    )


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("child_labor_incidence_us")
    tb = ds_meadow["child_labor_incidence_us"].reset_index()

    #
    # Process data.
    #
    sanity_check_inputs(tb)

    tb = tb.rename(columns=COLUMNS, errors="raise")

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
