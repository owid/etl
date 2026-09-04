"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of the ICD-10 codes covered by the dataset.
ICD10_MAPPING = {
    "C18-C21": "Colon and rectum cancer",
    "C50": "Breast cancer",
    "C53": "Cervical cancer",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("eurostat_cancer.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Keep the relevant columns (the SDMX-CSV export also carries the dataflow id, last-update stamp, frequency,
    # unit, observation flags and confidentiality status, which are constant or not needed).
    tb = tb[["sex", "icd10", "geo", "TIME_PERIOD", "OBS_VALUE"]]

    # Replace the ICD-10 codes with the corresponding cancer names.
    tb["icd10"] = tb["icd10"].replace(ICD10_MAPPING)
    tb = tb.rename(columns={"geo": "country", "TIME_PERIOD": "year", "OBS_VALUE": "pct_of_population"})

    # Improve table format.
    tb = tb.format(["country", "year", "icd10", "sex"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
