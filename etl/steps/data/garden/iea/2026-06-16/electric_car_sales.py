"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def sanity_check_inputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in input table."
    # Shares should not exceed 100% (allow a small tolerance for rounding in the source).
    for col in ["ev_sales_share", "ev_stock_share", "bev_share_car_sales", "phev_share_car_sales"]:
        assert tb[col].max() <= 100.5, f"{col} exceeds 100%."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    assert not tb.index.duplicated().any(), "Duplicate index in output table."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("electric_car_sales")
    tb = ds_meadow.read("electric_car_sales")

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
