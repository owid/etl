"""Load a meadow dataset and create the World Values Survey - Trust garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("wvs_trust.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("wvs_trust")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["wvs_trust"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    log.info("wvs_trust.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    # Keep selected variables (the rest is kept in the snapshot/meadow for anaylisis)
    vars_to_keep = [
        "trust",
        "trust_first_not_at_all",
        "trust_personally_not_at_all",
        "take_advantage",
        "confidence_government",
    ]
    tb_garden = tb_garden[vars_to_keep]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wvs_trust.end")
