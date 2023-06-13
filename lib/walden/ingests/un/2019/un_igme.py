import os
import tempfile
from pathlib import Path

import click
import pandas as pd
from owid.repack import repack_frame

from owid.walden import add_to_catalog
from owid.walden.catalog import Dataset

LOCAL_FILE = "/Users/fionaspooner/Documents/OWID/repos/notebooks/FionaSpooner/un_igme_youth_mortality/data/input/Child Mortality Estimation.csv"


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    metadata = Dataset.from_yaml(Path(__file__).parent / "un_igme.meta.yml")

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset = pd.read_csv(LOCAL_FILE)
        # consolidate data
        dataset = repack_frame(dataset)
        data_file = os.path.join(temp_dir, f"data.{metadata.file_extension}")
        dataset = dataset.reset_index()
        dataset.to_feather(data_file)
        add_to_catalog(metadata, data_file, upload=upload)


if __name__ == "__main__":
    main()
