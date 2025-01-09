"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
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
    tb = tb[["sex", "icd10", "geo", "TIME_PERIOD", "OBS_VALUE"]]
    # Dictionary mapping ICD-10 codes to their descriptions
    icd10_mapping = {
        "C18-C21": "Colon and rectum cancer",
        "C50": "Breast cancer",
        "C53": "Cervical cancer",
    }

    # Replace the ICD-10 codes in the 'icd10' column with their corresponding descriptions
    tb["icd10"] = tb["icd10"].replace(icd10_mapping)
    tb = tb.rename(columns={"geo": "country", "TIME_PERIOD": "year", "OBS_VALUE": "%_of_population"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "icd10", "sex"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
