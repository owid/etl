"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("urban_agglomerations_largest_cities")

    # Read table from garden dataset.
    tb = ds_garden["urban_agglomerations_largest_cities"]
    #
    # Process data.
    #
    tb = tb.reset_index()
    # Rename urban_agglomeration to country for the grapher.
    tb = tb.rename(columns={"urban_agglomeration": "country", "country": "country_code"})
    tb = tb.drop(columns=["rank_order"])
    tb["country_code"].metadata.unit = ""
    tb["country_code"].metadata.title = "Country color code"

    tb = tb.set_index(["country", "year"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
