"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import processing as pr

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

    tb = tb.replace({"BLA": "Biologics License Application", "NDA": "New Molecular Entity"})

    tb_designations = ds_garden.read("drug_approvals_designations")
    tb_desig_original = tb_designations.copy()

    tb_designations = pr.melt(
        tb_designations,
        id_vars=["year"],
        value_vars=[
            "orphan_drug_approvals",
            "accelerated_approvals",
            "breakthrough_therapy_approvals",
            "fast_track_approvals",
            "qualified_infectious_disease_approvals",
            "vaccine_approvals",
        ],
        var_name="country",
        value_name="approvals",
    )

    tb_designations = tb_designations.replace(
        {
            "orphan_drug_approvals": "Orphan Drug",
            "accelerated_approvals": "Accelerated Approval",
            "breakthrough_therapy_approvals": "Breakthrough Therapy",
            "fast_track_approvals": "Fast Track",
            "qualified_infectious_disease_approvals": "Qualified Infectious Disease Product",
            "vaccine_approvals": "Vaccine",
        }
    )

    tb_designations["approvals"].m.title = "Number of approvals"
    tb_designations["approvals"].m.description = "The number of approvals for each designation in a given year."
    tb_designations["approvals"].m.unit = "approvals"
    tb_designations["approvals"].m.short_unit = ""
    tb_designations = tb_designations.format(["year", "country"], short_name="drug_approvals_designations_melted")
    tb = tb.format(
        ["year", "country"],
        short_name="total_drug_approvals",
    )

    tb_desig_original["country"] = "United States"
    tb_desig_original = tb_desig_original.format(["year", "country"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(
        tables=[tb, tb_designations, tb_desig_original], default_metadata=ds_garden.metadata
    )

    # Save grapher dataset.
    ds_grapher.save()
