"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets for global plastic emissions by gas, application type and polymer and read tables.

    ds_meadow = paths.load_dataset("plastic_emissions")
    tb = ds_meadow["plastic_emissions"].reset_index()
    # Convert million to actual number
    tb["value"] = tb["value"] * 1e6
    # Replace specific strings in the 'lifecycle_stage' column
    tb["lifecycle_stage"] = tb["lifecycle_stage"].replace({"Production & Conversion": "Production and conversion"})

    # Pivot dataframe by gas_type
    tb = tb.pivot(index=["country", "year", "lifecycle_stage"], columns="gas_type", values="value")
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
