"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets for global plastic emissions by gas, application type and polymer and read tables.

    ds_meadow = paths.load_dataset("plastic_use_application")
    tb = ds_meadow["plastic_use_application"].reset_index()
    # Convert million to actual number
    tb["value"] = tb["value"] * 1e6
    #
    # Process data.
    #
    tb = tb.pivot(index=["country", "year"], columns="application", values="value")
    # Combine textile and transportation sectors into signle indicators
    tb["transportation"] = tb["Transportation - other"] + tb["Transportation - tyres"]
    tb["textile_sector"] = tb["Textile sector - others"] + tb["Textile sector - clothing"]

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
