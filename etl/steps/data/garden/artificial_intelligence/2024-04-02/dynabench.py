"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

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

    mapping = {
        "MNIST": "Handwriting recognition",
        "GLUE": "Broad language understanding",
        "ImageNet": "Image classification",
        "SQuAD 1.1": "Text-based question answering",
        "SQuAD 2.0": "Reading comprehension",
        "BBH": "Creative reasoning and problem solving",
        "Switchboard": "Conversational language processing",
        "MMLU": "Language based knowledge tests",
        "HellaSwag": "Contextual reasoning and prediction",
        "HumanEval": "Programming and code generation",
        "SuperGLUE": "Nuanced language interpretation",
        "GSK8k": "Mathematical reasoning and logic",
    }
    tb["assessment_domain"] = tb["benchmark"].map(mapping)

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
