"""
This code generates the garden step with the MPI dataset for both harmonized over time and
current margin estimates for the variables MPI, share of MPI poor and intensity of poverty.
"""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

MEADOW_VERSION = "2022-12-13"

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("multidimensional_poverty_index.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / f"meadow/ophi/{MEADOW_VERSION}/multidimensional_poverty_index")
    tb_meadow = ds_meadow["multidimensional_poverty_index"]

    df = pd.DataFrame(tb_meadow)

    # %% [markdown]
    # ### Note on `year`
    #
    # The way `year` is formatted – as a string variable often spanning two calendar years – won't work with our schema. We have to map the data to a single (integer) year.
    #
    # For now, arbitrarily, I take the first year in these cases and convert to integer.

    # %%
    # First year = first 4 characters of the year string
    df["year"] = df["year"].str[:4].astype(int)

    # %% [markdown]
    # ## Multi-dimesional poverty measures
    #
    # At least initially, we will be primarily concerned with the three measures that relate to overall multi-dimensional poverty:
    # - `Headcount ratio`: the share of population in multidimensional poverty
    # - `Intensity`: a measure of the average depth of poverty (of the poor only – NB, not like the World Bank's poverty gap index)
    # - `MPI`: the product of `Headcount ratio` and `Intensity`.
    #
    # These are multi-dimensional poverty measures – a weighted aggregation across many individual indicators.
    # Here I prepare this data as I would for uploading to OWID grapher and visualize it – including both `hot` and `cme` data in the same file.

    # %%
    # Prep data for garden

    # Modify variable names
    df = df.replace({"M0": "mpi", "H": "share", "A": "intensity"})

    # filter for main multi-dimensional pov measures
    df = df[df["measure"].isin(["mpi", "share", "intensity"])].reset_index(drop=True)

    # pivot to wide format
    df = df.pivot_table(index=["country", "year"], columns=["flav", "measure", "area_lab"], values="b").reset_index()

    # collapse multi-level index into single column names
    df.columns = [" ".join(col).strip().replace(" ", "_") for col in df.columns.values]

    # Format column names, making it all lowercase
    df.columns = df.columns.str.lower()  # type: ignore

    log.info("multidimensional_poverty_index.harmonize_countries")
    df = harmonize_countries(df)

    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir)

    tb_garden = Table(df)

    # update metadata from yaml file
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "multidimensional_poverty_index")

    # For now the variable descriptions are stored as a list of strings, this transforms them into a single string
    for col in tb_garden.columns:
        if isinstance(tb_garden[col].metadata.description, list):
            tb_garden[col].metadata.description = "\n".join(tb_garden[col].metadata.description)

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("multidimensional_poverty_index.end")


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df
