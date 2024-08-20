"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("monkeypox")

    # Read table from garden dataset.
    tb = ds_garden["monkeypox"].reset_index()
    # For variables on deaths we should show that data reported by the WHO show _only_ confirmed cases, in an annotation
    mask = (tb["country"] == "Democratic Republic of Congo") & tb.columns.str.contains("deaths")
    tb.loc[mask, "annotation"] = (
        tb.loc[mask, "annotation"] + "Democratic Republic of Congo: Includes only confirmed deaths as reported by WHO"
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
