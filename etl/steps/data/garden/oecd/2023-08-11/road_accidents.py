"""
Load a meadow dataset and combine with a migrated grapher dataset to create a garden dataset.

The migrated dataset has a collection of data from different sources, primarilty the OECD.

Here we add the most recent years data from the OECD. If there are overlapping country-years, we use the new data.
"""

from typing import cast

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("road_accidents"))
    ds_migrate = cast(Dataset, paths.load_dependency("road_deaths_and_injuries"))
    # Read table from meadow dataset.
    tb = ds_meadow["road_accidents"].reset_index()
    tb_migrate = ds_migrate["road_deaths_and_injuries"].reset_index()
    tb_migrate["source"] = "old"
    #
    # Process data new data
    #
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.drop(columns=["frequency", "flag_codes", "indicator"])
    tb = tb.pivot_table(index=["country", "year"], columns=["subject", "measure"], values="value")
    tb.columns = [" ".join(col).strip() for col in tb.columns.values]
    tb = tb.reset_index()
    tb = tb.rename(
        columns={
            "ACCIDENTCASUAL NBR": "accidents_involving_casualties",
            "DEATH 1000000HAB": "deaths_per_million_inhabitants",
            "DEATH 1000000VEH": "deaths__per_million_vehicles",
            "DEATH NBR": "deaths",
            "INJURE NBR": "injuries",
        }
    )
    tb["source"] = "new"
    # Combine with old data
    #
    tb_combined = pr.concat([tb, tb_migrate], ignore_index=True)
    tb_combined = (
        tb_combined.sort_values(by=["source", "country", "year"])
        .drop_duplicates(subset=["country", "year"], keep="first")
        .sort_values(by=["country", "year"])
    )
    tb_combined = tb_combined.drop(columns=["source", "road_deaths_per_100_million_vehicle_kilometres"])
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True)
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
