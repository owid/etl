"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("invasive_species")

    # Read table from meadow dataset.
    tb = ds_meadow["invasive_species"].reset_index()

    #
    # Add cumulative
    cols = tb.columns.drop(["continent", "year"]).tolist()
    for col in cols:
        tb[f"{col}_cumulative"] = tb.groupby("continent")["values"].apply(lambda x: x.fillna(0).cumsum())

    # Process data.
    #
    tb = tb.format(["continent", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
