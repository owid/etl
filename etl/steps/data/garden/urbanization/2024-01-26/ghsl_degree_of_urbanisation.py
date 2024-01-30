"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ghsl_degree_of_urbanisation")

    # Read table from meadow dataset.
    tb = ds_meadow["ghsl_degree_of_urbanisation"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Pivot the table to each indicator as a column.
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value")

    tb = tb.underscore().reset_index()
    # Convert share of urban population to percentage.
    tb["share_of_urban_population"] = tb["share_of_urban_population"] * 100
    # Create two new dataframes to separate data into estimates and projections (pre-2025 and post-2025 (five year intervals)))
    past_estimates = tb[tb["year"] < 2025].copy()
    future_projections = tb[tb["year"] >= 2025].copy()

    # Now, for each column in the original dataframe, split it into two (projections and estimates)
    for col in tb.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < 2025, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= 2025, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    tb_merged = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")
    tb_merged = tb_merged.set_index(["country", "year"], verify_integrity=True)

    print(tb_merged.columns)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_merged], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
