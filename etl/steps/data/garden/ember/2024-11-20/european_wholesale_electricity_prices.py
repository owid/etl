"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Select and rename columns.
COLUMNS = {
    "country": "country",
    "date": "date",
    "price__eur_mwhe": "price",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("european_wholesale_electricity_prices")

    # Read table from meadow dataset.
    tb_monthly = ds_meadow.read("european_wholesale_electricity_prices")

    #
    # Process data.
    #
    # Select and rename columns.
    tb_monthly = tb_monthly[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb_monthly = geo.harmonize_countries(df=tb_monthly, countries_file=paths.country_mapping_path)

    # Ember provides monthly data, so we can create a monthly table of wholesale electricity prices.
    # But we also need to create an annual table of average wholesale electricity prices.
    tb_annual = tb_monthly.copy()
    tb_annual["year"] = tb_annual["date"].str[:4].astype("Int64")
    # NOTE: We will include only complete years. This means that the latest year will not be included. But also, we will disregard country-years like Ireland 2022, which only has data for a few months, for some reason.
    n_months = tb_annual.groupby(["country", "year"], observed=True, as_index=False)["date"].transform("count")
    tb_annual = (
        tb_annual[n_months == 12].groupby(["country", "year"], observed=True, as_index=False).agg({"price": "mean"})
    )

    # Improve table formats.
    tb_monthly = tb_monthly.format(["country", "date"], short_name="european_wholesale_electricity_prices_monthly")
    tb_annual = tb_annual.format(short_name="european_wholesale_electricity_prices_annual")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_monthly, tb_annual], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
