"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("diagnosis_routes_by_route")

    # Read table from garden dataset.
    tb = ds_garden["diagnosis_routes_by_route"].reset_index()
    tb = tb.drop(columns=["country"])
    # Define mapping with lowercase values
    cancer_mapping = {
        "Bladder": "Bladder",
        "Brain": "Brain",
        "Breast": "Breast",
        "Cervix": "Cervical",
        "Colorectal": "Colorectal",
        "Kidney": "Kidney",
        "Liver": "Liver",
        "Lung": "Lung",
        "Oesophagus": "Oesophageal",
        "Ovary": "Ovarian",
        "Pancreas": "Pancreatic",
        "Prostate": "Prostate",
        "Stomach": "Stomach",
        "Testis": "Testicular",
        "Uterus": "Uterine",
    }

    # Map cancer types to descriptive labels in lowercase
    tb["site"] = tb["site"].map(cancer_mapping)

    # Make cancer type appear as country.
    tb = tb.rename(columns={"site": "country"})
    tb = tb.format(["country", "year", "stage", "route"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
