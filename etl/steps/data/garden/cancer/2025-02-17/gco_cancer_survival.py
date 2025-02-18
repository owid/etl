"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gco_cancer_survival")

    # Read table from meadow dataset.
    tb = ds_meadow.read("gco_cancer_survival")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb[tb["survival_year"] == 5]
    tb = tb.drop("survival_year", axis=1)
    # Replace specific values in the "cancer" column
    tb["cancer"] = tb["cancer"].replace(
        {
            "Colon": "Colon",
            "Colorectal": "Colorectal",
            "Liver": "Liver",
            "Lung": "Lung",
            "Oesophagus": "Oesophageal",
            "Pancreas": "Pancreatic",
            "Rectum": "Rectal",
            "Ovary": "Ovarian",
            "Stomach": "Stomach",
        }
    )
    tb = tb.format(["country", "year", "gender", "cancer"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
