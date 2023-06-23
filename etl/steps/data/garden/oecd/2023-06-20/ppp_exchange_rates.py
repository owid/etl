"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ppp_exchange_rates.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("ppp_exchange_rates"))

    # Read table from meadow dataset.
    tb = ds_meadow["ppp_exchange_rates"]
    #
    # Process data.
    #
    log.info("ppp_exchange_rates.harmonize_countries")
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    pivot_table = tb.pivot(
        index=["measure", "country", "year", "unit"], columns="transaction", values="value"
    ).reset_index()

    tb_garden = Table(pivot_table, short_name="ppp_exchange_rates")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ppp_exchange_rates.end")
