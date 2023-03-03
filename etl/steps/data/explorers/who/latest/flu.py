"""Load a garden dataset and create an explorers dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Load in both the fluid and flunet datasets and merge them on country and date
    - Check that all dates match (they should)
    """
    #
    # Load inputs.
    #
    # Load garden dataset.
    flunet_garden: Dataset = paths.load_dependency("flunet")
    fluid_garden: Dataset = paths.load_dependency("fluid")

    # Read table from garden dataset.
    tb_flunet = flunet_garden["flunet"]
    tb_fluid = fluid_garden["fluid"]

    tb_flu = pd.DataFrame(pd.merge(tb_fluid, tb_flunet, on=["country", "date"], how="outer"))
    tb_flu = create_zero_filled_strain_columns(tb_flu)
    tb_flu = remove_sparse_timeseries(df=tb_flu, cols=tb_flu.columns.drop(["country", "date"]), min_data_points=10)
    # tb_flu.dropna(axis = 1, how="all")
    assert tb_flu[["country", "date"]].duplicated().sum() == 0
    tb_flu = Table(tb_flu, short_name="flu")

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb_flu], default_metadata=flunet_garden.metadata, formats=["csv"])
    ds_explorer.save()


def create_zero_filled_strain_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    For the stacked bar charts in the grapher to work I think I need to fill the NAs with zeros. I'll keep the original columns too as I think adding 0s into line charts would look weird and be misleading
    """
    strain_columns = [
        "ah1n12009combined",
        "ah1combined",
        "ah3combined",
        "ah5combined",
        "ah7n9combined",
        "a_no_subtypecombined",
        "byamcombined",
        "bnotdeterminedcombined",
        "bviccombined",
        "inf_acombined",
        "inf_bcombined",
        "ah1n12009sentinel",
        "ah1sentinel",
        "ah3sentinel",
        "ah5sentinel",
        "ah7n9sentinel",
        "byamsentinel",
        "bnotdeterminedsentinel",
        "bvicsentinel",
        "inf_asentinel",
        "inf_bsentinel",
        "ah1n12009nonsentinel",
        "ah1n12009notdefined",
        "ah1notdefined",
        "ah1nonsentinel",
        "ah3nonsentinel",
        "ah3notdefined",
        "ah5nonsentinel",
        "ah5notdefined",
        "ah7n9nonsentinel",
        "ah7n9notdefined",
        "a_no_subtypenotdefined",
        "a_no_subtypesentinel",
        "byamnonsentinel",
        "byamnotdefined",
        "bnotdeterminednonsentinel",
        "bnotdeterminednotdefined",
        "bvicnonsentinel",
        "bvicnotdefined",
    ]
    # Add these columns if we need to, for now stick to the above for file size reasons

    strain_columns_zfilled = [s + "_zfilled" for s in strain_columns]
    df[strain_columns_zfilled] = df[strain_columns].fillna(0)
    return df


def remove_sparse_timeseries(df: pd.DataFrame, cols: list[str], min_data_points: int) -> pd.DataFrame:
    """
    For each country go through each column, if there are {min_data_points} or fewer values which are non-zero and non-NA then the whole time-series is set to NA.
    For example if min_data_points = 5, then any country-column with fewer than 5 datapoints will be set to NA.
    """
    countries = df["country"].drop_duplicates()

    for country in countries:
        for col in cols:
            df[col] = df[col].astype(np.float32)
            if df.loc[(df["country"] == country), col].fillna(0).astype(bool).sum() <= min_data_points:
                df.loc[(df["country"] == country), col] = np.NaN

    return df
