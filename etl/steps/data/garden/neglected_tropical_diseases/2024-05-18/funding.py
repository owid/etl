"""
Load a meadow dataset and create a garden dataset.
"""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("funding.start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("funding")

    # Read table from meadow dataset.
    tb = ds_meadow["funding"].reset_index()
    # Aggregate the funding amounts so that we have three tables, one summed by disease, one summed by product and one summed by disease*product
    tb_disease = tb.groupby(["disease", "year"], observed=True)["amount__usd"].sum()
    tb_product = tb.groupby(["product", "year"], observed=True)["amount__usd"].sum()
    tb_disease_product = tb.groupby(["disease", "product", "year"], observed=True)["amount__usd"].sum()
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_disease, tb_product, tb_disease_product],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
