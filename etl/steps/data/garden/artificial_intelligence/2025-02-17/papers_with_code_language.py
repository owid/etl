"""Load a meadow dataset and create a garden dataset."""

from shared import load_and_process_dataset

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    file_indicator = "language"  # Change this based on your dataset: math, language, coding
    column_to_process = ["performance_language_average"]  # Change or expand this list based on your dataset
    perform_merge = False  # Set to True if you need to merge two processed tables

    load_and_process_dataset(file_indicator, column_to_process, paths, perform_merge)
