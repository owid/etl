"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("urbanization_urban_rural")

    # Read table from meadow dataset.
    tb = ds_meadow["urbanization_urban_rural"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Convert population columns to thousands
    columns_to_convert = [
        "annual_urban_population_at_mid_year_by_region__subregion_and_country__1950_2050__thousands",
        "annual_rural_population_at_mid_year_by_region__subregion_and_country__1950_2050__thousands",
    ]

    for col in columns_to_convert:
        if col in tb.columns:
            # Convert to thousands
            tb[col] = tb[col] * 1000
            # Remove 'thousands' from column name
            tb.rename(columns={col: col.replace("__thousands", "")}, inplace=True)

    # Create two new dataframes to separate data into estimates and projections (pre-2019 and post-2019)
    past_estimates = tb[tb["year"] < 2019].copy()
    future_projections = tb[tb["year"] >= 2019].copy()

    # Now, for each column in the original dataframe, split it into two
    for col in tb.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < 2019, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= 2019, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    tb_merged = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")
    tb_merged = tb_merged.set_index(["country", "year"], verify_integrity=True)

    # Remove '__1950_2050' from column names
    for col in tb_merged.columns:
        if "__1950_2050" in col:
            tb_merged = tb_merged.rename(columns={col: col.replace("__1950_2050", "")})
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_merged], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
