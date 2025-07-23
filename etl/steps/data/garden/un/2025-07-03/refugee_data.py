"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("refugee_data")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("refugee_data")

    # filter out data before data availability starts (s. https://www.unhcr.org/refugee-statistics/methodology/, "Data publication timeline")
    tb["asylum_seekers"] = tb["asylum_seekers"].where(tb["year"] >= 2000, pd.NA)
    tb["refugees"] = tb["refugees"].where(tb["year"] >= 1951, pd.NA)
    tb["returned_refugees"] = tb["returned_refugees"].where(tb["year"] >= 1965, pd.NA)
    tb["idps"] = tb["idps"].where(tb["year"] >= 1993, pd.NA)
    tb["stateless"] = tb["stateless"].where(tb["year"] >= 2004, pd.NA)
    # others of concern
    tb["ooc"] = tb["ooc"].where(tb["year"] >= 2018, pd.NA)
    tb["returned_idps"] = tb["returned_idps"].where(tb["year"] >= 1997, pd.NA)

    # TODO: check for refugees in the same country of origin and asylum
    # TODO: check that idps are the same for asylum and origin

    tb_asylum = tb.copy()
    tb_asylum = tb_asylum.rename(columns={"country_of_asylum": "country"})

    tb_origin = tb.copy()
    tb_origin = tb_origin.rename(columns={"country_of_origin": "country"})

    # harmonize countries
    tb_asylum = geo.harmonize_countries(
        df=tb_asylum,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    tb_origin = geo.harmonize_countries(
        df=tb_origin,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    # Add population to table
    tb_asylum = geo.add_population_to_table(tb=tb_asylum, ds_population=ds_population)
    tb_origin = geo.add_population_to_table(tb=tb_origin, ds_population=ds_population)

    # Calculate shares
    # asylum table
    tb_asylum["asylum_seekers_per_100k"] = tb_asylum["asylum_seekers"] / tb_asylum["population"] * 100_000
    tb_asylum["refugees_per_100k"] = tb_asylum["refugees"] / tb_asylum["population"] * 100_000
    tb_asylum["refugees_per_1000"] = tb_asylum["refugees"] / tb_asylum["population"] * 1000
    tb_asylum["returned_refugees_per_100k"] = tb_asylum["returned_refugees"] / tb_asylum["population"] * 100_000
    tb_asylum["stateless_per_100k"] = tb_asylum["stateless"] / tb_asylum["population"] * 100_000

    # origin table
    tb_origin["asylum_seekers_per_100k"] = tb_origin["asylum_seekers"] / tb_origin["population"] * 100_000
    tb_origin["refugees_per_100k"] = (tb_origin["refugees"] / tb_origin["population"]) * 100_000
    tb_origin["refugees_per_1000"] = (tb_origin["refugees"] / tb_origin["population"]) * 1000
    tb_origin["returned_refugees_per_100k"] = tb_origin["returned_refugees"] / tb_origin["population"] * 100_000
    tb_origin["stateless_per_100k"] = tb_origin["stateless"] / tb_origin["population"] * 100_000
    tb_origin["returned_idps_per_1000"] = tb_origin["returned_idps"] / tb_origin["population"] * 1000

    # drop idps from asylum table as we have them in the origin table + drop population
    tb_asylum = tb_asylum.drop(columns=["idps", "returned_idps", "population"])
    tb_origin = tb_origin.drop(columns=["population"])

    # Replace unknown countries with either "Unknown Origin" or "Unknown Destination"
    tb_asylum["country"] = tb_asylum["country"].replace("Unknown", "Unknown Destination")
    tb_asylum["country_of_orign"] = tb_asylum["country_of_origin"].replace("Unknown", "Unknown Origin")

    tb_origin["country"] = tb_origin["country"].replace("Unknown", "Unknown Origin")
    tb_origin["country_of_asylum"] = tb_origin["country_of_asylum"].replace("Unknown", "Unknown Destination")

    # Improve table format.
    tb_asylum = tb_asylum.format(["country", "country_of_origin", "year"], short_name="refugee_data_asylum")
    tb_origin = tb_origin.format(["country", "country_of_asylum", "year"], short_name="refugee_data_origin")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_asylum, tb_origin], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
