from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    ds_garden = cast(Dataset, paths.load_dependency("ggdc_maddison"))
    tb = ds_garden["maddison_gdp"]

    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
