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
    ds_meadow = paths.load_dataset("urban_agglomerations_size_class")

    # Read table from meadow dataset.
    tb = ds_meadow["urban_agglomerations_size_class"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Pivot table to have one column per size class of urban settlement
    tb_pivot = tb.pivot(
        index=["country", "year"], columns=["size_class_of_urban_settlement", "type_of_data"], values="value"
    )
    tb_pivot.columns = ["_".join(col).strip() for col in tb_pivot.columns.values]

    # Convert population columns to thousands
    for col in tb_pivot.columns:
        if "_Population" in col:
            tb_pivot[col] = tb_pivot[col] * 1000

    tb_pivot = tb_pivot.reset_index()
    # Create two new dataframes to separate data into estimates and projections (pre-2019 and post-2019)
    past_estimates = tb_pivot[tb_pivot["year"] < 2019].copy()
    future_projections = tb_pivot[tb_pivot["year"] >= 2019].copy()

    # Now, for each column in the original dataframe, split it into two (projections and estimates)
    for col in tb_pivot.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb_pivot.loc[tb_pivot["year"] < 2019, col]
            future_projections[f"{col}_projections"] = tb_pivot.loc[tb_pivot["year"] >= 2019, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    tb_merged = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")
    tb_merged = tb_merged.underscore().set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_merged], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
