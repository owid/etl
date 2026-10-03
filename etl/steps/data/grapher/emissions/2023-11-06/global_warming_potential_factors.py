"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("global_warming_potential_factors")
    tb = ds_garden["global_warming_potential_factors"].reset_index()

    #
    # Process data.
    #
    # Grapher requires a column named "country", and another named "year".
    # We assign the year of publication of the document where the data was extracted from.
    tb = tb.rename(columns={"greenhouse_gas": "country"})
    tb["year"] = int(tb["gwp_100"].metadata.origins[0].date_published.split("-")[0])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.save()
