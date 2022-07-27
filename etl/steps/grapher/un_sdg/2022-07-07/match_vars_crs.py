from etl.chart_revision_suggester import ChartRevisionSuggester
from etl import match_variables_from_two_versions_of_a_dataset
from pathlib import Path

VERSION = Path(__file__).parent.stem
FNAME = Path(__file__).stem
NAMESPACE = Path(__file__).parent.parent.stem

# VERSION = "2022-07-07"
# FNAME = "un_sdg"
# NAMESPACE = "un_sdg"

NEW_DATASET_NAME = f"{NAMESPACE}__{VERSION.replace('-', '_')}"
OLD_DATASET_NAME = (
    "United Nations Sustainable Development Goals - United Nations (2022-04)"
)


match_variables_from_two_versions_of_a_dataset.main(
    old_dataset_name=OLD_DATASET_NAME,
    new_dataset_name=NEW_DATASET_NAME,
    output_file=Path(__file__).parent.resolve() / "output/variable_replacements.json",
)


suggester = ChartRevisionSuggester(
    Path(__file__).parent.resolve(), VERSION, NEW_DATASET_NAME
)
suggester.suggest()
