"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("sst")

    # Read table from meadow dataset.
    tb = ds_meadow.read("sst")

    #
    # Process data.
    #

    # Fill NA values with a number that won't affect classification
    tb["oni_anomaly"] = tb["oni_anomaly"].fillna(0)

    # Classify NINO3.4 and NINO4 anomaly values
    tb["nino_classification"] = tb["oni_anomaly"].copy()
    tb.loc[tb["oni_anomaly"] > 0.5, "nino_classification"] = 1
    tb.loc[tb["oni_anomaly"] < -0.5, "nino_classification"] = 2
    tb.loc[(tb["oni_anomaly"] >= -0.5) & (tb["oni_anomaly"] <= 0.5), "nino_classification"] = 0
    tb["nino_classification"] = tb["nino_classification"].astype(int)

    for col in ["nino_classification"]:
        tb[col].metadata.origins = tb["nino3_4_anomaly"].metadata.origins

    tb = tb.drop(columns={"nino4_anomaly", "nino3_4_anomaly"})

    tb = tb.format(["country", "year", "month"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
