"""Load a meadow dataset and create a garden dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("dynabench")

    # Read table from meadow dataset.
    tb = ds_meadow["dynabench"].reset_index()

    #
    # Process data.
    #
    tb["benchmark"] = tb["benchmark"].astype(str)
    # Selecting the best performance for each benchmark per year
    tb = tb.groupby(["benchmark", "year"])["performance"].max().reset_index().copy_metadata(from_table=tb)

    # Set the first year's performance to the baseline of –1 for each benchmark.
    # This is to preserve a baseline for –1 for all benchmarks,
    # even when a second, better performance is recorded in a later year.
    tb = tb.sort_values(by=["benchmark", "year"])
    tb.loc[tb.groupby("benchmark").head(1).index, "performance"] = -1

    mapping = {
        "MNIST": "Handwriting recognition",
        "Switchboard": "Speech recognition",
        "ImageNet": "Image recognition",
        "BBH": "Complex reasoning",
        "GLUE": "Language understanding",
        "SQuAD 1.1": "Reading comprehension",
        "SQuAD 2.0": "Reading comprehension with unanswerable questions",
        "MMLU": "General knowledge tests",
        "HellaSwag": "Predictive reasoning",
        "HumanEval": "Code generation",
        "SuperGLUE": "Nuanced language interpretation",
        "GSK8k": "Math problem-solving",
    }
    tb["assessment_domain"] = tb["benchmark"].map(mapping)

    assert (
        not tb["assessment_domain"].isnull().any()
    ), "There are NaN values in the 'assessment_domain' column. Make sure you've mapped all benchmarks to their respective assessment domains."

    tb = tb.set_index(["benchmark", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
