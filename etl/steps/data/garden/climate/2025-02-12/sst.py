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
    tb
    month_map = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }
    tb["month"] = tb["month"].map(month_map)
    tb["nino_classification"].metadata.origins = tb["nino3_4_anomaly"].metadata.origins

    tb = tb.drop(
        columns={
            "nino4_anomaly",
            "nino3_4_anomaly",
        }
    )

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
    # Classify NINO3.4 and NINO4 anomaly values
    if row["nino3_4_anomaly"] >= 0.5:
        return "El Niño"
    elif row["nino4_anomaly"] <= -0.5:
        return "La Niña"
    else:
        return "Neutral"
