"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("schistosomiasis")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow["schistosomiasis"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.drop(columns=["region", "age_group", "country_code"])
    tb = geo.add_regions_to_table(
        tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
    )
    # Replace regional values in percentage columns with NaN
    tb.loc[tb["country"].isin(REGIONS), ["programme_coverage__pct", "national_coverage__pct"]] = np.nan
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
