"""Garden step for Shift data on energy production from fossil fuels.

"""
import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def split_ussr_and_russia(tb: Table) -> Table:
    """Split data for USSR & Russia into two separate entities (given that Shift treats them as the same entity).

    Parameters
    ----------
    tb : Table
        Shift data after harmonizing country names.

    Returns
    -------
    tb: Table
        Shift data after separating data for USSR and Russia as separate entities.

    """
    tb = tb.copy()

    # Name that The Shift Data Portal uses for Russia and USSR.
    shift_ussr_russia_name = "Russian Federation and USSR (Shift)"
    # The relevant part of the data is originally from EIA, who have the first data point for Russia in 1992.
    # Therefore we use this year to split USSR and Russia.
    russia_start_year = 1992
    # Filter to select rows of USSR & Russia data.
    ussr_russia_filter = tb["country"] == shift_ussr_russia_name
    ussr_data = (
        tb[ussr_russia_filter & (tb["year"] < russia_start_year)]
        .replace({shift_ussr_russia_name: "USSR"})
        .reset_index(drop=True)
    )
    russia_data = (
        tb[ussr_russia_filter & (tb["year"] >= russia_start_year)]
        .replace({shift_ussr_russia_name: "Russia"})
        .reset_index(drop=True)
    )
    # Remove rows where Russia and USSR are combined.
    tb = tb[~ussr_russia_filter].reset_index(drop=True)
    # Combine original data (without USSR and Russia as one entity) with USSR and Russia as separate entities.
    tb = (
        pr.concat([tb, ussr_data, russia_data], ignore_index=True)
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    return tb


def correct_historical_regions(data: Table) -> Table:
    """Correct some issues in Shift data involving historical regions.

    Parameters
    ----------
    data : Table
        Shift data after harmonization of country names.

    Returns
    -------
    data : Table
        Shift data after doing some corrections related to historical regions.

    """
    data = data.copy()

    # For coal and oil, Czechoslovakia's data become Czechia and Slovakia in 1993.
    # However, for gas, Czechia appear at an earlier date.
    # We correct those rows to be part of Czechoslovakia.
    data_to_add = pr.merge(
        data[(data["year"] < 1980) & (data["country"] == "Czechoslovakia")]
        .reset_index(drop=True)
        .drop(columns=["gas"]),
        data[(data["year"] < 1980) & (data["country"] == "Czechia")].reset_index(drop=True)[["year", "gas"]],
        how="left",
        on="year",
    )
    select_rows_to_correct = (data["country"].isin(["Czechia", "Czechoslovakia"])) & (data["year"] < 1980)
    data = (
        pr.concat([data[~select_rows_to_correct], data_to_add], ignore_index=True)
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    return data


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("energy_production_from_fossil_fuels")
    tb_meadow = ds_meadow["energy_production_from_fossil_fuels"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb_meadow, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Remove rows that only have nans.
    tb = tb.dropna(subset=["coal", "oil", "gas"], how="all").reset_index(drop=True)

    # Treat USSR and Russia as separate entities.
    tb = split_ussr_and_russia(tb=tb)

    # Correct gas data where Czechia and Czechoslovakia overlap.
    tb = correct_historical_regions(data=tb)

    # Create aggregate regions.
    tb = geo.add_regions_to_table(
        tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
        ignore_overlaps_of_zeros=True,
    )

    # Prepare output data.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset (with the same metadata as the meadow version).
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
