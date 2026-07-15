"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Count columns that must never be negative.
COUNT_COLUMNS = [
    "public_charging_points",
    "public_fast_charging_points",
    "public_slow_charging_points",
]


def sanity_check_inputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows found."
    assert tb["year"].between(2000, 2030).all(), "Year outside the expected range."
    assert (tb[COUNT_COLUMNS].min(skipna=True) >= 0).all(), "Negative charging-point count found."
    share = tb["fast_chargers_share_of_total"]
    assert share[share.notna()].between(0, 100).all(), "Fast-charger share outside the 0-100 range."
    assert (tb["electric_cars_per_charging_point"].dropna() >= 0).all(), "Negative electric cars per charging point."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    assert not tb.index.duplicated().any(), "Duplicate index entries in the output table."


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ev_charging_points")
    tb = ds_meadow["ev_charging_points"].reset_index()

    #
    # Process data.
    #
    sanity_check_inputs(tb)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "year"])

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
