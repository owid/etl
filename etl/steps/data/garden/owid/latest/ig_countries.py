"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers.misc import expand_time_column
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ig_countries")

    # Read table from meadow dataset.
    tb = ds_meadow["ig_countries"].reset_index()

    #
    # Process data.
    #
    ## Dtypes
    tb = tb.astype(
        {
            "country": "string",
            "date": "datetime64[ns]",
        }
    )

    tb = tb.sort_values("date")
    tb["counts_cum"] = tb.groupby("country")["count"].cumsum()

    # expand_time_column(tb)
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
