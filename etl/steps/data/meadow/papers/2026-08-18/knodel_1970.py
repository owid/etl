"""Load the transcribed Knodel (1970) Bavarian village figures and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# The paper's own Total row, which is the couple-weighted average of the five cohorts. Not transcribed
# as a row, but used here to reconcile the transcription: children ever born, surviving to the first
# birthday, surviving to the fifteenth, and the number of couples.
PUBLISHED_TOTAL = {
    "children_ever_born": 5.6,
    "children_surviving_to_age_1": 3.6,
    "children_surviving_to_age_15": 3.2,
    "couples": 184,
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("knodel_1970.csv")
    tb = snap.read_csv()

    #
    # Process data.
    #
    _sanity_check(tb)

    tb = tb.format(["village", "marriage_cohort_start"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()


def _sanity_check(tb) -> None:
    """Reconcile the transcribed cohorts against the four figures the paper's Total row prints.

    A row-shift or a mistyped digit survives every schema check, so the guard is that the five cohorts
    have to reproduce the published totals - the couple count exactly, and each average to the tenth
    the paper rounds to.
    """
    assert len(tb) == 5, f"Table 11 has five marriage cohorts, transcribed {len(tb)}"
    assert (tb["marriage_cohort_start"] < tb["marriage_cohort_end"]).all(), "A marriage cohort runs backwards"
    assert tb["marriage_cohort_start"].is_monotonic_increasing, "The cohorts are out of order"

    couples = tb["couples"]
    assert couples.sum() == PUBLISHED_TOTAL["couples"], (
        f"The cohorts cover {couples.sum()} couples, but the paper's Total row prints {PUBLISHED_TOTAL['couples']}"
    )

    for column in ("children_ever_born", "children_surviving_to_age_1", "children_surviving_to_age_15"):
        weighted = float((tb[column] * couples).sum() / couples.sum())
        published = PUBLISHED_TOTAL[column]
        assert abs(round(weighted, 1) - published) < 1e-9, (
            f"The cohorts give a couple-weighted {column} of {weighted:.3f}, which rounds to "
            f"{round(weighted, 1)}, but the paper's Total row prints {published}"
        )

    # Survivors can only fall with age, and never exceed the children born.
    assert (tb["children_surviving_to_age_15"] <= tb["children_surviving_to_age_1"]).all(), (
        "More children survive to 15 than to their first birthday"
    )
    assert (tb["children_surviving_to_age_1"] <= tb["children_ever_born"]).all(), (
        "More children survive infancy than were born"
    )
