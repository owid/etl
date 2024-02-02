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

    # Read HYDE dataset to combine past with future projections
    ds_garden_hyde = paths.load_dataset("all_indicators")
    tb_hyde = ds_garden_hyde["all_indicators"].reset_index()
    cols_hyde = ["country", "year", "urbc_c", "urbc_c_share", "rurc_c", "rurc_c_share"]
    tb_hyde = tb_hyde[cols_hyde]

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

    # Now, for each column in the original dataframe, split it into two (projections and estimates)
    for col in tb.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < 2019, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= 2019, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    tb_merged = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")

    # Remove '__1950_2050' from column names
    for col in tb_merged.columns:
        if "__1950_2050" in col:
            tb_merged = tb_merged.rename(columns={col: col.replace("__1950_2050", "")})

    # Combine historical estimates from Hyde with projections from UN
    cols_to_combine_with_hyde = [
        "year",
        "country",
        "annual_percentage_of_population_at_mid_year_residing_in_urban_areas_by_region__subregion_and_country_projections",
        "annual_urban_population_at_mid_year_by_region__subregion_and_country_projections",
        "annual_rural_population_at_mid_year_by_region__subregion_and_country_projections",
    ]

    future_proj_combine_with_hyde = tb_merged[cols_to_combine_with_hyde][tb_merged["year"] > 2023].copy()
    future_proj_combine_with_hyde = future_proj_combine_with_hyde.rename(
        columns={
            "annual_percentage_of_population_at_mid_year_residing_in_urban_areas_by_region__subregion_and_country_projections": "urbc_c_share",
            "annual_urban_population_at_mid_year_by_region__subregion_and_country_projections": "urbc_c",
            "annual_rural_population_at_mid_year_by_region__subregion_and_country_projections": "rurc_c",
        }
    )
    future_proj_combine_with_hyde["rurc_c_share"] = 100 - future_proj_combine_with_hyde["urbc_c_share"]

    tb_proj_hyde_un = pr.concat([future_proj_combine_with_hyde, tb_hyde], ignore_index=True)

    tb_final = pr.merge(tb_proj_hyde_un, tb_merged, on=["year", "country"], how="outer")
    tb_final = tb_final.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_final], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
