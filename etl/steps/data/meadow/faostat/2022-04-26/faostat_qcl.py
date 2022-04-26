"""FAOstat: Crops and livestock products.

"""

from .shared import generate_dataset

NAMESPACE = "faostat"
DATASET_SHORT_NAME = f"{NAMESPACE}_qcl"


def run(dest_dir: str) -> None:
    generate_dataset(
        dest_dir,
        namespace=NAMESPACE,
        dataset_short_name=DATASET_SHORT_NAME,
    )
