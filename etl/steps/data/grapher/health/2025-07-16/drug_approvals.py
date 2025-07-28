"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("drug_approvals")

    # Read table from garden dataset.
    tb = ds_garden.read("total_drug_approvals")
    tb = tb.rename(
        columns={
            "application_type": "country",
        },
        errors="raise",
    )

    tb_designations = ds_garden.read("drug_approvals_designations")
    tb_designations = tb_designations.rename(
        columns={
            "designation": "country",
        },
        errors="raise",
    )

    tb_designations = tb_designations.format(["year", "country"])
    tb = tb.format(["year", "country"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb, tb_designations], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
