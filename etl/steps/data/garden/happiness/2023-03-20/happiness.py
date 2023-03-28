"""Load a meadow dataset and create a garden dataset."""

from datetime import datetime

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("happiness.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("happiness")
    backport: Dataset = paths.load_dependency("dataset_235_world_happiness_report__2022")
    # Read table from meadow dataset.
    tb_meadow = ds_meadow["happiness"]
    dt = datetime.strptime(paths.version, "%Y-%m-%d")
    # The report give values the average of the three previous years, which we report as the middle year - consistently 2 years prior to the publication year
    tb_meadow["year"] = dt.year - 2

    tb_backport = backport["dataset_235_world_happiness_report__2022"].reset_index()
    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)
    df = df[["year", "country", "ladder_score", "standard_error_of_ladder_score", "upperwhisker", "lowerwhisker"]]

    df_backport = pd.DataFrame(tb_backport)
    df_backport = df_backport[
        ["year", "entity_name", "life_satisfaction_in_cantril_ladder__world_happiness_report_2022"]
    ]
    df_backport = df_backport.rename(
        columns={
            "entity_name": "country",
            "life_satisfaction_in_cantril_ladder__world_happiness_report_2022": "ladder_score",
        }
    )
    #
    # Process data.
    #
    log.info("happiness.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    df_combined = pd.concat([df, df_backport])
    df_combined["year"] = df_combined["year"].astype("uint32")
    df_combined = df_combined.sort_values("year")
    df_combined = df_combined.reset_index(drop=True)
    # Create a new table with the processed data.
    tb_garden = Table(df_combined, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("happiness.end")
