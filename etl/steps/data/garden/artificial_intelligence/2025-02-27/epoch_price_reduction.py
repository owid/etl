"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("epoch_price_reduction")

    # Read table from meadow dataset.
    tb = ds_meadow.read("epoch_price_reduction")

    #
    # Process data.
    #

    # Pivot the table so that each column represents a benchmark
    tb_pivot = tb.pivot(
        index=["threshold_model", "year"], columns="bench", values="price_reduction_factor_per_year"
    ).reset_index()

    # Flatten the columns
    tb_pivot.columns.name = None
    tb_pivot.columns = [str(col) for col in tb_pivot.columns]

    tb_pivot = tb_pivot.rename(columns={"threshold_model": "country"})

    tb_pivot = tb_pivot.format(["country", "year"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_pivot], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch.end")
