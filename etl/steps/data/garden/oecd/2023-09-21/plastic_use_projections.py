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
    # Load meadow datasets for global plastic emissions by gas, application type and polymer and read tables.
    ds_meadow = paths.load_dataset("plastic_use_projections")
    tb = ds_meadow["plastic_use_projections"].reset_index()
    #
    # Process data.
    #
    # Convert million to actual number
    tb["plastic_use"] = tb["plastic_use"] * 1e6
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Create a global estimate
    total_df = tb.groupby("year")["plastic_use"].sum().reset_index()
    total_df["country"] = "World"
    # Merge with the original dataframe
    combined_df = pr.merge(total_df, tb, on=["country", "year", "plastic_use"], how="outer").copy_metadata(
        from_table=tb
    )

    tb = combined_df.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()
    tb["cumulative_plastic_use"] = tb.groupby("country")["plastic_use"].cumsum()

    #
    # Save outputs.
    #

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
