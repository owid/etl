"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Normalization of scenario names
SCENARIOS = {
    "estimates_past": "estimates_past",
    "Reference": "reference",
    "Slower Met Need and Education": "slower",
    "Faster Met Need and Education": "faster",
    "Fastest Met Need and Education": "fastest",
    "SDG Met Need and Education": "sdg",
}
AGES = {
    "1 to 4": "1_4",
    "5 to 9": "5_9",
    "10 to 14": "10_14",
    "15 to 19": "15_19",
    "20 to 24": "20_24",
    "25 to 29": "25_29",
    "30 to 34": "30_34",
    "35 to 39": "35_39",
    "40 to 44": "40_44",
    "45 to 49": "45_49",
    "50 to 54": "50_54",
    "55 to 59": "55_59",
    "60 to 64": "60_64",
    "65 to 69": "65_69",
    "70 to 74": "70_74",
    "75 to 79": "75_79",
    "80 to 84": "80_84",
    "85 to 89": "85_89",
    "90 to 94": "90_94",
    "95 plus": "95_plus",
    "All Ages": "total",
    "Early Neonatal": "early_neonatal",
    "Late Neonatal": "late_neonatal",
    "Post Neonatal": "post_neonatal",
}
SEX = {
    "Female": "female",
    "Male": "male",
    "Both": "total",
}

# Columns that can be used as index
COLS_INDEX = ["country", "year", "sex", "age", "scenario"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ihme_2020")

    # Prepare population tables
    tb_pop = ds_meadow.read("population")
    tb_pop = prepare_population_forecast(tb_pop, "population")
    tb_pop_past = ds_meadow.read("population_retro")
    tb_pop_past = prepare_population_past(tb_pop_past, "population")

    # Prepare TFR table
    tb_tfr = ds_meadow.read("fertility")
    tb_tfr = prepare_tfr(tb_tfr, "tfr")

    # Prepare life expectancy table
    tb_le = ds_meadow.read("life_expectancy")  # country, year, scenario, sex, val-upper-lower
    tb_le = prepare_le(tb_le, "life_expectancy")

    # Prepare migration table
    tb_mig = ds_meadow.read("migration")  # country, year, scenario, val-upper-lower
    tb_mig = prepare_mig(tb_mig, "net_migration")

    #
    # Process data.
    #
    # Harmonize country names.
    tb_pop = paths.regions.harmonize_names(tb=tb_pop)
    tb_pop_past = paths.regions.harmonize_names(tb=tb_pop_past)
    tb_tfr = paths.regions.harmonize_names(tb=tb_tfr)
    tb_le = paths.regions.harmonize_names(tb=tb_le)
    tb_mig = paths.regions.harmonize_names(tb=tb_mig)

    # TODO: combine tables, keep relevant dimensions, etc.

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def prepare_population_forecast(tb, indicator_name):
    # Sanity check values of some columns to be dropped. We are dropping them bc they are redundant (only one value)
    assert (tb["measure_name"].unique() == ["Population"]).all()
    assert (tb["metric_name"].unique() == ["Number"]).all()
    assert (tb["sex"].unique() == ["Both"]).all()
    assert (tb["age_group_name"].unique() == ["All Ages"]).all()

    # Keep relevant columns and rename them.
    columns = {
        "location_name": "country",
        "year_id": "year",
        "scenario_name": "scenario",
        "val": indicator_name,
        # "upper": "upper",
        # "lower": "lower",
    }
    tb = tb[columns.keys()].rename(columns=columns)

    # Rename column values
    tb["scenario"] = _rename_columns(tb, "scenario", SCENARIOS)

    # Check uniqueness
    cols_index = tb.columns.intersection(COLS_INDEX)
    _ = tb.format(cols_index)

    return tb


def prepare_population_past(tb, indicator_name):
    # Sanity check values of some columns to be dropped. We are dropping them bc they are redundant (only one value)
    assert (tb["measure_name"].unique() == ["Population"]).all()
    assert (tb["metric_name"].unique() == ["Number"]).all()
    assert set(tb["sex"].unique()) == {"Female", "Male"}

    # Keep relevant columns and rename them.
    columns = {
        "location_name": "country",
        "year_id": "year",
        "sex": "sex",
        "age_group_name": "age",
        "val": indicator_name,
    }
    tb = tb[columns.keys()].rename(columns=columns)

    # Rename sex values
    tb["sex"] = tb["sex"].str.lower()

    # Rename age groups
    tb["age"] = _rename_columns(tb, "age", AGES)

    # Estimate sex='total' values & add to main table
    tb_total = tb.groupby(["country", "year", "age"], observed=True, as_index=False)["population"].sum()
    tb_total["sex"] = "total"
    tb_total = tb_total[tb.columns]
    tb = pr.concat([tb, tb_total], ignore_index=True)

    # Check uniqueness
    cols_index = tb.columns.intersection(COLS_INDEX)
    _ = tb.format(cols_index)

    return tb


def prepare_tfr(tb, indicator_name):
    # Sanity check values of some columns to be dropped. We are dropping them bc they are redundant (only one value)
    assert (tb["measure_name"].unique() == ["Total Fertility Rate"]).all()
    assert (tb["metric_name"].unique() == ["Rate"]).all()

    # Keep relevant columns and rename them.
    columns = {
        "location_name": "country",
        "year_id": "year",
        "scenario_name": "scenario",
        "val": indicator_name,
        # "upper": "upper",
        # "lower": "lower",
    }
    tb = tb[columns.keys()].rename(columns=columns)

    # Rename column values
    tb["scenario"] = _rename_columns(tb, "scenario", SCENARIOS)

    # Check uniqueness
    cols_index = tb.columns.intersection(COLS_INDEX)
    _ = tb.format(cols_index)

    return tb


def prepare_le(tb, indicator_name):
    # Sanity check values of some columns to be dropped. We are dropping them bc they are redundant (only one value)
    assert (tb["measure_name"].unique() == ["Life expectancy"]).all()
    assert (tb["metric_name"].unique() == ["Years"]).all()
    assert set(tb["sex"].unique()) == {"Female", "Male", "Both"}

    # Keep relevant columns and rename them.
    columns = {
        "location_name": "country",
        "year_id": "year",
        "sex": "sex",
        "scenario_name": "scenario",
        "val": indicator_name,
        # "upper": "upper",
        # "lower": "lower",
    }
    tb = tb[columns.keys()].rename(columns=columns)

    # Rename column values
    tb["scenario"] = _rename_columns(tb, "scenario", SCENARIOS)

    # Rename age groups
    tb["sex"] = _rename_columns(tb, "sex", SEX)

    # Check uniqueness
    cols_index = tb.columns.intersection(COLS_INDEX)
    _ = tb.format(cols_index)

    return tb


def prepare_mig(tb, indicator_name):
    # Sanity check values of some columns to be dropped. We are dropping them bc they are redundant (only one value)
    assert (tb["measure_name"].unique() == ["Net Migration"]).all()
    assert (tb["metric_name"].unique() == ["Number"]).all()
    assert set(tb["age_group_name"].unique()) == {"All Ages"}
    assert set(tb["sex"].unique()) == {"Both"}

    # Keep relevant columns and rename them.
    columns = {
        "location_name": "country",
        "year_id": "year",
        "val": indicator_name,
        # "upper": "upper",
        # "lower": "lower",
    }
    tb = tb[columns.keys()].rename(columns=columns)

    # Check uniqueness
    cols_index = tb.columns.intersection(COLS_INDEX)
    _ = tb.format(cols_index)

    return tb


def _rename_columns(tb, column, mapping):
    """Rename values in a column based on a mapping. Raises an error if there are unexpected values in the column that are not in the mapping."""
    unexpected_age_groups = set(tb[column].unique()) - set(mapping.keys())
    assert not unexpected_age_groups, f"Unexpected age group values: {unexpected_age_groups}"
    return tb[column].map(mapping)
