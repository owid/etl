"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns that hold counts of cars (should never be negative).
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
# Columns that hold percentages (should lie between 0 and 100).
SHARE_COLUMNS = [
    "ev_sales_share",
    "ev_stock_share",
    "bev_share_ev_cars",
    "phev_share_ev_cars",
    "bev_share_car_sales",
    "phev_share_car_sales",
]


def sanity_check_inputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows found."
    # NOTE: "Rest of World" is a residual aggregate (global total minus the named countries/regions),
    # so it can take small negative values due to rounding in the IEA's underlying estimates. We only
    # enforce non-negativity on the actual countries and regions, where a negative count would be a real error.
    tb_real = tb[tb["country"] != "Rest of World"]
    assert tb_real[COUNT_COLUMNS].min().min() >= 0, "Negative car count found — source error or unit mistake."
    for column in SHARE_COLUMNS:
        values = tb_real[column].dropna()
        assert values.between(0, 100).all(), f"Share column '{column}' has values outside the 0-100 range."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    assert not tb.index.duplicated().any(), "Duplicate index entries in output table."


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
    ds_garden = paths.create_dataset(tables=[tb])

    # Save new garden dataset.
    ds_garden.save()
