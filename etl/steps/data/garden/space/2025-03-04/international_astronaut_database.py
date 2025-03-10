"""Load a meadow dataset and create a garden dataset."""

import json
import re

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def harmonize_country_names(tb, tb_regions):
    # Start by applying the normal country harmonization (where, e.g. "Soviet Union" is mapped to "USSR").
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Now handle historical regions specifically.
    # * "Czechoslovakia" split into "Czechia" and "Slovakia", but so far only Slovakia has an entry.
    error = "We are mapping Czechoslovakia to Slovakia, but Czechia now has an entry. Decide how to handle."
    assert tb[tb["country"] == "Czechia"].empty, error

    # * "East Germany" reunified with West Germany (not in the data), so we can map it to "Germany".
    error = "West Germany was not expected to appear in the data."
    assert "West Germany" not in set(tb["country"]), error

    # * "Soviet Union" split into "Russia", "Ukraine", and "Kazakhstan" (and other countries not in the data).
    ussr_successors = tb_regions[
        tb_regions["code"].isin(json.loads(tb_regions[tb_regions["name"] == "USSR"]["successors"].item()))
    ]["name"].tolist()
    error = "USSR successors have changed in the data. Decide how to handle."
    assert set(ussr_successors) & set(tb["country"]) == set(["Russia", "Ukraine", "Kazakhstan"]), error
    error = "Expected Russia to account for more than 95% of entries in the data."
    assert (
        len(tb[tb["country"].isin(["Russia"])]) / len(tb[tb["country"].isin(["Kazakhstan", "Ukraine", "Russia"])]) * 100
        > 95
    ), error

    # NOTE: If any of the previous assertions fail, rethink the mapping.
    historical_regions_mapping = {
        "Czechoslovakia": "Slovakia",
        "East Germany": "Germany",
        "USSR": "Russia",
    }
    tb["country"] = map_series(
        tb["country"],
        mapping=historical_regions_mapping,
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    return tb


def fill_missing_country_years(tb):
    # Ensure all curves span from the minimum year to the maximum year in the data, filling gaps with zeros.
    tb_all_country_years = (
        pd.MultiIndex.from_product(
            [tb["country"].unique(), range(tb["year"].min(), tb["year"].max() + 1)], names=["country", "year"]
        )
        .to_frame()
        .reset_index(drop=True)
    )
    tb = (
        tb.merge(Table(tb_all_country_years), on=["country", "year"], how="right")
        .fillna(0)
        .astype({"n_launches": int, "n_new_astronauts": int})
    )

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("international_astronaut_database")

    # Read table from meadow dataset.
    tb = ds_meadow.read("international_astronaut_database")

    # Load regions dataset (used for sanity checks).
    ds_regions = paths.load_dataset("regions")

    # Read table from regions dataset.
    tb_regions = ds_regions.read("regions")

    #
    # Process data.
    #
    # Column "flights" contains years in parentheses. Extract them and explode each year in a different row.
    tb["year"] = [re.findall(r"\((\d{4})\)", flights) for flights in tb["flights"]]
    error = "Mismatch between 'total_flights' and number of years found in 'flights' column."
    assert (tb["year"].str.len() == tb["total_flights"]).all(), error
    tb = tb.explode("year").reset_index(drop=True).astype({"year": int})
    tb = tb.sort_values(["country", "year", "name"]).reset_index(drop=True)

    # Harmonize country names.
    tb = harmonize_country_names(tb=tb, tb_regions=tb_regions)

    # Calculate the number of annual launches per country-year (regardless of the astronaut).
    tb_annual_launches = (
        tb.groupby(["country", "year"], as_index=False).agg({"name": "count"}).rename(columns={"name": "n_launches"})
    )

    # Calculate the number of astronauts launched for the first time per country-year.
    tb_n_first_launches = (
        tb.drop_duplicates(subset=["country", "name"])
        .groupby(["country", "year"], as_index=False)
        .agg({"name": "nunique"})
        .rename(columns={"name": "n_new_astronauts"})
    )

    # Combine the two tables.
    tb = tb_annual_launches.merge(tb_n_first_launches, on=["country", "year"], how="left")
    # Fill with 0 on years where no new astronaut was launched.
    tb["n_new_astronauts"] = tb["n_new_astronauts"].fillna(0).astype(int)

    # Ensure all curves span from the minimum year to the maximum year in the data, filling gaps with zeros.
    tb = fill_missing_country_years(tb=tb)

    # Add global totals for various columns.
    tb = pr.concat(
        [
            tb,
            tb.groupby("year", as_index=False)
            .agg({"n_launches": "sum", "n_new_astronauts": "sum"})
            .assign(**{"country": "World"}),
        ],
        ignore_index=True,
    )

    # Add a column for the cumulative number of launches per country over the years.
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    tb["n_cumulative_launches"] = tb.groupby("country")["n_launches"].cumsum()

    # Add a column for the cumulative number of new astronauts per country over the years.
    tb["n_cumulative_new_astronauts"] = tb.groupby("country")["n_new_astronauts"].cumsum()

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
