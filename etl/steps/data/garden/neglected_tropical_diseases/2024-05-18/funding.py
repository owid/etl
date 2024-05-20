"""
Load a meadow dataset and create a garden dataset.
"""

from typing import List

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
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

    # The funding for each disease
    tb_disease = format_table(tb=tb, group=["disease", "year"], index_col=["disease"], short_name="funding_disease")
    # The funding for each product
    tb_product = format_table(tb=tb, group=["product", "year"], index_col=["product"], short_name="funding_product")
    # The funding for each disease*product
    tb_disease_product = format_table(
        tb=tb,
        group=["disease", "product", "year"],
        index_col=["disease", "product"],
        short_name="funding_disease_product",
    )
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


def format_table(tb: Table, group: List[str], index_col: List[str], short_name: str) -> Table:
    """
    Formatting original table so that we can have total funding by disease, product and disease*product
    """
    tb = tb.groupby(group, observed=True)["amount__usd"].sum().reset_index()
    tb["country"] = "World"
    tb = tb.set_index(["country", "year"] + index_col, verify_integrity=False)
    # tb = tb.pivot(index="year", columns=pivot_col, values="amount__usd")
    tb.metadata.short_name = short_name

    return tb
