import owid.catalog.processing as pr
from shared import load_and_process_dataset

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    file_indicator = "math"  # Change this based on your dataset: math, language, coding
    column_to_process = ["performance_math"]  # Change or expand this list based on your dataset
    perform_merge = False  # Set to True if you need to merge two processed tables

    load_and_process_dataset(file_indicator, column_to_process, dest_dir, paths, perform_merge)
