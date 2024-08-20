"""Load a garden dataset and create a grapher dataset."""

from shared import to_grapher_date

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("vaccinations_us")

    # Read table from garden dataset.
    tb = ds_garden["vaccinations_us"]

    #
    # Process data.
    #
    # Grapher date
    tb = to_grapher_date(tb, "2021-01-01")

    # Rename state -> country for grapher
    tb = tb.rename_index_names(
        {
            "state": "country",
        }
    )

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
