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

    tb["nino_classification"] = tb.apply(classify_nino_anomaly, axis=1)
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


def classify_nino_anomaly(row):
    # Fill NA values with a number that won't affect classification
    row = row.fillna(0)
    # Classify NINO3.4 and NINO4 anomaly values
    if row["oni_anomaly"] > 0.5:
        return 1  # "El Niño"
    elif row["oni_anomaly"] < -0.5:
        return 2  # "La Niña"
    else:
        return 0  # "Neutral"
