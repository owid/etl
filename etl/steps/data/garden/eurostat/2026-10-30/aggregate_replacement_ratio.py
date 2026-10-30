"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Eurostat sex codes and the labels we use for the sex dimension.
SEX_LABELS = {"T": "both sexes", "M": "men", "F": "women"}

# Euro area aggregates. We drop them: their membership changes over time (EA20, EA21) and the EU aggregate already covers the region.
EURO_AREA_CODES = ["EA20", "EA21"]

# Observation flags we know how to handle (empty: no flag; b: break in series; p: provisional).
EXPECTED_FLAGS = {"", "b", "p"}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("aggregate_replacement_ratio")

    # Read table from meadow dataset.
    tb = ds_meadow.read("aggregate_replacement_ratio")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Drop the euro area aggregates.
    tb = tb[~tb["geo"].isin(EURO_AREA_CODES)].reset_index(drop=True)

    # Relabel sex codes, and drop columns that carry no information (one unit; flags are only checked).
    tb["sex"] = tb["sex"].map(SEX_LABELS)
    tb = tb.drop(columns=["unit", "flag"], errors="raise")

    # Convert the ratio into a percentage (Eurostat labels the unit "Percentage", but the values are ratios like 0.60).
    tb = tb.rename(columns={"geo": "country", "value": "aggregate_replacement_ratio"}, errors="raise")
    # Round to undo the float noise of the stored ratios (Eurostat publishes two decimals, so whole percentages).
    tb["aggregate_replacement_ratio"] = (tb["aggregate_replacement_ratio"] * 100).round(1)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Drop known broken series (see the .corrections.yml file).
    tb = paths.apply_corrections(tb)

    # Improve table format.
    tb = tb.format(["country", "year", "sex"])

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["sex"]) == set(SEX_LABELS), f"Unexpected sex codes: {set(tb['sex'])}"
    assert set(tb["unit"]) == {"PC"}, f"Unexpected units: {set(tb['unit'])}"
    assert set(tb["flag"]) <= EXPECTED_FLAGS, f"New observation flags to handle: {set(tb['flag']) - EXPECTED_FLAGS}"
    assert not tb.duplicated(subset=["geo", "sex", "year"]).any(), "Duplicate (geo, sex, year) rows."
    assert (tb["value"] >= 0).all(), "Negative ratios in the source data."
    # The values are ratios, not percentages: the EU ratio has been between 0.5 and 0.7 in every year.
    eu = tb[(tb["geo"] == "EU27_2020")]
    assert eu["value"].between(0.5, 0.7).all(), f"EU ratio out of the expected range: {eu['value'].tolist()}"
    assert set(EURO_AREA_CODES) <= set(tb["geo"]), "Euro area codes changed: update EURO_AREA_CODES."


def sanity_check_outputs(tb: Table) -> None:
    tb = tb.reset_index()
    column = "aggregate_replacement_ratio"
    # Magnitude: the ratio is now in percent, so the EU value should be between 50% and 70%.
    eu = tb[tb["country"] == "European Union (27)"]
    assert eu[column].between(50, 70).all(), f"EU value out of the expected range: {eu[column].tolist()}"
    assert set(eu["sex"]) == set(SEX_LABELS.values()), "EU aggregate is missing a sex breakdown."
    # Outside Turkey (whose ratio is far above 100% in some years), pensions don't exceed about 1.1 times earnings.
    others = tb[tb["country"] != "Turkey"]
    assert others[column].between(10, 110).all(), (
        f"Implausible values: {others[~others[column].between(10, 110)][['country', 'year', 'sex', column]]}"
    )
    assert not tb[column].isna().any(), "Missing values in the output."
    assert tb["year"].min() == 2010, f"Unexpected first year: {tb['year'].min()}"
    # Coverage shouldn't shrink: 37 countries plus the EU in the 2026-10-20 release.
    assert tb["country"].nunique() >= 38, f"Fewer countries than expected: {tb['country'].nunique()}"
