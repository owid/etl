"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns that are counts of cars (non-negative expected).
COUNT_COLUMNS = [
    "bev_sales",
    "bev_stock",
    "phev_sales",
    "phev_stock",
    "ev_sales",
    "ev_stock",
    "total_cars_sold",
    "non_ev_cars_sold",
]

# Columns that are percentages.
SHARE_COLUMNS = [
    "ev_sales_share",
    "ev_stock_share",
    "bev_share_ev_cars",
    "phev_share_ev_cars",
    "bev_share_car_sales",
    "phev_share_car_sales",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("electric_car_sales")

    # Read table from meadow dataset.
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
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    assert not tb.empty, "Input table is empty."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    # The source/derived columns carry some implausible values (negative counts, shares outside
    # 0-100). We don't silently fix them here — instead we surface them, since they originate
    # from the producer's published explorer.
    for col in COUNT_COLUMNS:
        if (tb[col] < 0).any():
            n = int((tb[col] < 0).sum())
            paths.log.warning(f"{col}: {n} negative value(s) in a count column (source artifact).")
    for col in SHARE_COLUMNS:
        out_of_range = ((tb[col] < 0) | (tb[col] > 100)).sum()
        if out_of_range:
            paths.log.warning(f"{col}: {int(out_of_range)} value(s) outside [0, 100] (source artifact).")


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.columns[tb.isna().all()].size, "Output has a fully-NaN column."
    assert tb.index.get_level_values("year").min() >= 2000, "Unexpected year before 2000."
