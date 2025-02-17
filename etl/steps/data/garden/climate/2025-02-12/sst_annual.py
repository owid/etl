"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_garden = paths.load_dataset("sst")

    # Read table from meadow dataset.
    tb = ds_garden.read("sst")

    #
    # Process data.
    #
    # Calculate the annual average for the dataset
    tb_annual = tb.groupby(["country", "year"]).mean().reset_index()

    # Classify the year based on nino_classification
    def classify_year(group):
        if (group["nino_classification"] == 1).sum() > 6:
            return 1
        elif (group["nino_classification"] == 2).sum() > 6:
            return 2
        else:
            return 0

    tb_annual["annual_nino_classification"] = (
        tb.groupby(["country", "year"]).apply(classify_year).reset_index(drop=True)
    )
    tb_annual = tb_annual.rename(columns={"oni_anomaly": "annual_oni_anomaly"})
    tb_annual["annual_nino_classification"] = tb_annual["annual_nino_classification"].copy_metadata(
        tb["nino_classification"]
    )

    tb_annual = tb_annual.drop(columns={"month", "nino_classification"})

    tb_annual = tb_annual.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_annual], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
