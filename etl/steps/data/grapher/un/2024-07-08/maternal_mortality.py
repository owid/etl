"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("maternal_mortality")

    # Read table from garden dataset.
    tb = ds_garden["maternal_mortality"]

    # only include columns not in long run dataset data://garden/maternal_mortality/2024-07-08/maternal_mortality.py
    cols_to_keep = [
        "hiv_related_indirect_maternal_deaths",
        "hiv_related_indirect_mmr",
        "hiv_related_indirect_percentage",
        "lifetime_risk",
        "lifetime_risk_1_in",
        "pm",
    ]

    tb = tb[cols_to_keep]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
