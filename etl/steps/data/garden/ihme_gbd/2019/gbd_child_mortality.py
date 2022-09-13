import pandas as pd
from gbd_tools import prepare_garden, tidy_countries
from owid.catalog import Dataset
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

# naming conventions
N = Names(__file__)
log = get_logger()


def run(dest_dir: str) -> None:

    dataset = N.short_name

    log.info(f"{dataset}.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / f"meadow/ihme_gbd/2019/{dataset}")

    tb_meadow = ds_meadow[f"{dataset}"]

    df = pd.DataFrame(tb_meadow)

    # exclude entities we don't want and harmonize ones we do
    country_mapping_path = N.directory / "gbd.countries.json"
    excluded_countries_path = N.directory / "gbd.excluded_countries.json"

    df = tidy_countries(country_mapping_path, excluded_countries_path, df)
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    # underscore column names and add units column
    tb_garden = prepare_garden(df)

    tb_garden.metadata = tb_meadow.metadata
    for col in tb_garden.columns:
        tb_garden[col].metadata = tb_meadow[col].metadata
    metadata_path = N.directory / f"{dataset}.meta.yml"
    ds_garden.metadata.update_from_yaml(metadata_path)
    tb_garden.update_metadata_from_yaml(metadata_path, f"{dataset}")
    tb_garden = tb_garden.set_index(["measure", "cause", "metric"])

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info(f"{dataset}.end")
