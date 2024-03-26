from shared import load_and_process_dataset

from etl.helpers import PathFinder

# Get paths and naming conventions for the current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    file_indicator = "coding"
    # Pass both columns to process and merge their results.
    columns_to_process = ["performance_code_any_interview", "performance_code_any_competition"]
    perform_merge = True  # Indicate that we want to merge the processed tables.

    load_and_process_dataset(file_indicator, columns_to_process, dest_dir, paths, perform_merge)
