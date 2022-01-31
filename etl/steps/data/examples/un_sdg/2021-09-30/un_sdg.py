from zipfile import ZipFile
import pandas as pd
from owid.catalog import Dataset
from owid import walden, catalog
from etl.steps.data.converters import convert_walden_metadata


def run(dest_dir: str) -> None:
    # Load dataset from `walden://un_sdg/2021-09-30/un_sdg`
    raw_dataset = walden.Catalog().find_one("un_sdg", "2021-09-30", "un_sdg")

    # Filter data and replace columns
    zip_file = ZipFile(raw_dataset.ensure_downloaded())
    print(f"Available files: {zip_file.namelist()}")

    df = pd.read_csv(zip_file.open("un-sdg-2021-10.csv"))

    gf = df[
        (df["[Education level]"] == "PRIMAR") & (df["[Type of skill]"] == "SKILL_MATH")
    ].dropna(how="all", axis=1)
    gf = gf.reset_index(drop=True)

    math_skills = catalog.Table(gf)
    math_skills.metadata = catalog.TableMeta(
        short_name="math_skills",
    )

    # Create dataset
    print(f"Saving to {dest_dir}")
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(raw_dataset)

    ds.add(math_skills)
    ds.save()
