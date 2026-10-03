"""Load a meadow dataset and create a garden dataset.

The source reports the incidence of child work (share of children aged 10-14 who are
economically active) in Italy by sex. Values are already percentages.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Source column -> descriptive indicator name.
COLUMNS = {
    "incidence_both_sexes_pct": "incidence_both_sexes",
    "incidence_boys_pct": "incidence_boys",
    "incidence_girls_pct": "incidence_girls",
}
SHARE_COLUMNS = list(COLUMNS.values())


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["country"].unique()) == {"Italy"}, "Expected Italy as the only entity."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    assert tb["year"].between(1881, 1961).all(), "Year outside the plausible census range."
    for col in COLUMNS:
        vals = tb[col].dropna()
        assert vals.between(0, 100).all(), f"{col} has values outside 0-100%."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    for col in SHARE_COLUMNS:
        assert tb[col].dropna().between(0, 100).all(), f"{col} outside 0-100%."
    # The both-sexes incidence is a population-weighted average of boys and girls, so it must
    # lie between the two (with a small tolerance for rounding in the transcribed source).
    lo = tb[["incidence_boys", "incidence_girls"]].min(axis=1) - 0.05
    hi = tb[["incidence_boys", "incidence_girls"]].max(axis=1) + 0.05
    assert (tb["incidence_both_sexes"].between(lo, hi)).all(), "Both-sexes incidence not bracketed by boys/girls."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("child_work_incidence_italy")
    tb = ds_meadow["child_work_incidence_italy"].reset_index()

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
