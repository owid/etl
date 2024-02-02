"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets for global plastic emissions by gas, application type and polymer and read tables.
    ds_meadow = paths.load_dataset("plastic_environment_leakage_projections")
    tb = ds_meadow["plastic_environment_leakage_projections"]
    #
    # Process data.
    #
    # Convert million to actual number
    tb["value"] = tb["value"] * 1e6
    tb = tb.reset_index()
    tb["scenario_type"]

    # Replace specific strings in the 'application' column
    tb["scenario_type"] = tb["scenario_type"].replace(
        {
            "Global Ambition policy scenario": "Global ambition policy",
            "Regional Action policy scenario": "Regional action policy",
        }
    )
    tb = (
        tb.underscore()
        .set_index(["country", "year", "scenario_type", "plastic_type"], verify_integrity=True)
        .sort_index()
    )

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
