"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Midpoint of the severity group that corresponds to each provisional range of scores in the source.
PROVISIONAL_RANGE_MIDPOINTS = {"<= 9.9*": 5, "10-19.9*": 15, "20-34.9*": 27.5, "35-49.9*": 42.5}


def sanity_check_inputs(tb: Table, tb_provisional: Table) -> None:
    scores = tb["global_hunger_index"].astype(str)
    numeric = scores.str.fullmatch(r"\d+(\.\d+)?")
    assert set(scores[~numeric]) <= {"<5", "—"}, f"Unexpected non-numeric scores: {set(scores[~numeric])}"
    # The source shows every score below 5 as "<5", so all numeric scores should be at least 5.
    assert scores[numeric].astype(float).min() >= 5, (
        "Numeric score below 5 found; the source may have changed how it shows low scores."
    )
    unknown_ranges = set(tb_provisional["provisional_range"]) - set(PROVISIONAL_RANGE_MIDPOINTS)
    assert not unknown_ranges, f"Unexpected provisional ranges: {unknown_ranges}"
    # Every country with a provisional range should be in the scores table, without a score in that year.
    tb_check = pr.merge(tb, tb_provisional, on=["country", "year"], how="inner")
    assert len(tb_check) == len(tb_provisional), "Some countries with a provisional range are missing in the scores."
    assert (tb_check["global_hunger_index"] == "—").all(), "Some countries with a provisional range also have a score."


def sanity_check_outputs(tb: Table) -> None:
    assert tb["global_hunger_index"].notnull().all(), "Missing scores left in the output."
    assert tb["global_hunger_index"].between(0, 100).all(), "Scores outside the 0-100 scale."
    latest_year = tb.index.get_level_values("year").max()
    n_countries_latest = (tb.index.get_level_values("year") == latest_year).sum()
    assert n_countries_latest >= 110, f"Only {n_countries_latest} countries have a score in {latest_year}."


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("global_hunger_index")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("global_hunger_index")
    tb_provisional = ds_meadow.read("global_hunger_index_provisional")

    #
    # Process data.
    #
    sanity_check_inputs(tb, tb_provisional)

    # Give countries without a score the midpoint of their provisional range.
    tb = pr.merge(tb, tb_provisional, on=["country", "year"], how="left")
    provisional = tb["provisional_range"].notnull()
    tb.loc[provisional, "global_hunger_index"] = (
        tb.loc[provisional, "provisional_range"].map(PROVISIONAL_RANGE_MIDPOINTS).astype(str)
    )
    tb = tb.drop(columns=["provisional_range"])

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Remove scores that are not available (shown as "—").
    tb["global_hunger_index"] = tb["global_hunger_index"].astype(str)
    tb = tb[tb["global_hunger_index"] != "—"].reset_index(drop=True)

    # Show scores below 5 (shown as "<5") as 2.5, the middle of the range from 0 to 5.
    tb.loc[tb["global_hunger_index"] == "<5", "global_hunger_index"] = "2.5"
    tb["global_hunger_index"] = pr.to_numeric(tb["global_hunger_index"])

    # Improve table format.
    tb = tb.format(["country", "year"])

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
