"""Load Defra's poultry and poultry meat statistics and its June survey of agriculture, and create a garden dataset with
the UK series on laying hens and chicks."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to take from each table of the poultry statistics, and their new names. Defra publishes them in millions.
# NOTE: Commercial layer chicks placed are the female chicks placed into rearing (Defra's series is identical to the
# "females reared for laying" that the UK reported to Eurostat until 2019); the male chicks are not placed.
COLUMNS = {
    "chicks_placed": {"commercial_layers_uk": "layer_chicks_placed"},
    "poultry_slaughtered": {"boiling_fowl_uk": "boiling_fowl_slaughtered"},
}
MILLION = 1e6
# Columns to take from the June survey (numbers of birds at 1 June).
JUNE_SURVEY_COLUMNS = ["total_breeding_and_laying_fowl", "hens_and_pullets_laying_eggs_for_eating", "breeding_flock"]
# NOTE: The UK number of boiling fowl slaughtered looks incomplete from 2024 onwards (in 2024 it equals the number for
# England and Wales alone). Check in the next update whether Defra has revised those years.


def run() -> None:
    #
    # Load inputs.
    #
    # Load the meadow dataset and read its June survey table.
    ds_meadow = paths.load_dataset("uk_laying_hens_and_chicks")
    tb_june = ds_meadow.read("june_survey")

    #
    # Process data.
    #
    # Select the series on laying hens and chicks from each table, converting millions into birds (or eggs).
    tb = None
    for table_name, columns in COLUMNS.items():
        tb_table = ds_meadow.read(table_name)[["year"] + list(columns)].rename(columns=columns, errors="raise")
        for column in columns.values():
            tb_table[column] = tb_table[column] * MILLION
        tb = tb_table if tb is None else pr.merge(tb, tb_table, on="year", how="outer")
    tb = pr.merge(tb, tb_june[["year"] + JUNE_SURVEY_COLUMNS], on="year", how="outer")
    tb["country"] = "United Kingdom"

    sanity_check_outputs(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()


def sanity_check_outputs(tb: Table) -> None:
    columns = [column for column in tb.columns if column not in ["country", "year"]]
    error = "All numbers should be non-negative."
    assert (tb[columns].fillna(0) >= 0).all().all(), error
    # The hatchery and slaughter series should have no gaps (the June survey series are missing some years).
    for column in [column for columns_ in COLUMNS.values() for column in columns_.values()]:
        years = tb[tb[column].notna()]["year"]
        error = f"Annual series should have no gaps ({column})."
        assert years.max() - years.min() + 1 == len(years), error

    # In the June survey, hens and pullets laying eggs for eating plus the breeding flock should add up to the total
    # breeding and laying fowl.
    parts = ["hens_and_pullets_laying_eggs_for_eating", "breeding_flock"]
    total = "total_breeding_and_laying_fowl"
    residual = ((tb[parts].sum(axis=1, min_count=len(parts)) - tb[total]) / tb[total]).dropna()
    error = f"{parts} do not add up to {total}."
    assert len(residual) > 10 and (residual.abs() < 1e-6).all(), error

    error = "Indicators should carry the origin of the snapshot."
    assert all(len(tb[column].metadata.origins) == 1 for column in columns), error
