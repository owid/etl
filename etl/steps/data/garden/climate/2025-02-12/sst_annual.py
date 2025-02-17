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

    tb_annual = tb_annual.rename(columns={"oni_anomaly": "annual_oni_anomaly"})
    tb_annual = tb_annual.drop(columns={"nino_classification", "month"})
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
