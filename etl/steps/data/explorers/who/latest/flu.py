"""Load a garden dataset and create an explorers dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

MIN_DATA_POINTS = 50


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
    tb_flu = remove_sparse_timeseries(df=tb_flu, min_data_points=MIN_DATA_POINTS)
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


def remove_sparse_timeseries(df: pd.DataFrame, min_data_points: int) -> pd.DataFrame:
    """
    For each country identify if they have < {min_data_points} confirmed flu cases.

    If they do then set all their values for flu cases to NA, we don't want to show super sparse countries

    For each of the ari/sari/ili columns we apply the same rule, if it has less than {min_data_points} then we set it to NA for that country

    """
    countries = df["country"].drop_duplicates()
    cols = df.columns.drop(["country", "date"])
    # all columns that have been zerofilled so they can be used in stacked bar charts
    z_filled_cols = [col for col in cols if col.endswith("zfilled")]
    # all columns that contain values on confirmed flu cases
    all_flunet_cols = cols[(cols.str.contains("sentinel|notdefined|combined"))]
    # all columns that will be used in line charts but not stacked bar charts
    fluid_cols = cols[(cols.str.contains("ili|ari"))]
    for country in countries:
        if all(df.loc[(df["country"] == country), z_filled_cols].fillna(0).astype(bool).sum() <= min_data_points):
            df.loc[(df["country"] == country), all_flunet_cols] = np.NaN
        for col in fluid_cols:
            df[col] = df[col].astype(np.float32)
            if df.loc[(df["country"] == country), col].fillna(0).astype(bool).sum() <= min_data_points:
                df.loc[(df["country"] == country), col] = np.NaN

    return df
