"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("flu_vaccine_policy")

    # Read table from garden dataset.
    tb_garden = ds_garden["flu_vaccine_policy"]

    #
    # Process data.
    #
    # Dropping some columsn which don't make sense to display in grapher
    drop_cols = [
        "at_what_time_of_the_year_is_influenza_vaccine_generally_offered",
        "is_influenza_vaccination_recommended_for_other_groups",
        "what_are_the_other_vaccine_types_used",
        "what_time_period_are_influenza_vaccination_policy_and_vaccine_availability_reported_on",
    ]
    tb_garden = tb_garden.drop(columns=drop_cols)
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
