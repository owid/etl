# # Regional wellbeing (OECD 2016)

# +
import pandas as pd
from owid.catalog import Dataset, Table
from owid.walden import Catalog as WaldenCatalog

from etl.steps.data.converters import convert_walden_metadata

# -

# ## Load the data

walden_ds = WaldenCatalog().find_one("oecd", "2016-06-01", "regional_wellbeing")
df = pd.read_csv(walden_ds.local_path)

# ## Drop useless columns

# some columns are empty
df.dropna(how="all", axis=1, inplace=True)

# some columns have only a single value for the whole table -- not informative
for col in df.columns:
    if len(df[col].unique()) == 1 and col != "Time":
        df.drop(col, axis=1, inplace=True)

# ## Standardise names

df = df.rename(
    columns={
        "REG_ID": "region_code",
        "Regions": "region_name",
        "IND": "ind_code",
        "Indicator": "ind_name",
        "Time": "year",
        "Unit Code": "unit_code",
        "Unit": "unit_name",
        "Value": "value",
    }
)

df["ind_code"] = df.ind_code.str.lower()
df["unit_code"] = df.unit_code.str.lower()

# ## Sanity checks

# #### One unit per indicator

assert len(df[["ind_code", "unit_code"]].drop_duplicates()) == len(df.ind_code.unique())

# #### One name per indicator

assert len(df[["ind_code", "ind_name"]].drop_duplicates()) == len(df.ind_code.unique())

# #### One name per unit

assert len(df[["unit_code", "unit_name"]].drop_duplicates()) == len(df.unit_code.unique())

# #### One name per region

assert len(df[["region_code", "region_name"]].drop_duplicates()) == len(df.region_code.unique())

# ## Capture important metadata pre-pivot

# name of each indicator
ind = df[["ind_code", "ind_name"]].drop_duplicates().set_index("ind_code").ind_name

# unit short_name of each indicator
ind_unit_code = df[["ind_code", "unit_code"]].drop_duplicates().set_index("ind_code").unit_code

# unit title for each indicator
ind_unit_name = df[["ind_code", "unit_name"]].drop_duplicates().set_index("ind_code").unit_name

regions = df[["region_code", "region_name"]].drop_duplicates().set_index("region_code").region_name

# ## Strip back and pivot

df = df.drop(["region_name", "ind_name", "unit_name", "unit_code"], axis=1)

df.head()

df = df.pivot(index=["region_code", "year"], columns="ind_code", values="value")

df.head()

# ## Add metadata

t = Table(df)

for ind_code in t.columns:
    meta = t[ind_code].metadata
    meta.short_name = ind_code
    meta.title = ind.loc[ind_code]
    short_unit = ind_unit_code[ind_code]
    meta.short_unit = short_unit if not pd.isnull(short_unit) else None
    meta.unit = ind_unit_name[ind_code] if not pd.isnull(ind_unit_name[ind_code]) else None

t.head()

t.metadata.short_name = "regional_wellbeing"

# ## Regions

regions = Table(regions)
regions.metadata.short_name = "regions"


# ## Metadata checks

for col in t.columns:
    assert t[col].metadata.short_name
    assert t[col].metadata.title
    assert t[col].metadata.short_unit or col == "unem_ra"
    assert t[col].metadata.unit or col == "unem_ra"

# ## Save the dataset


def run(dest_dir: str) -> None:
    ds_meta = convert_walden_metadata(walden_ds)
    ds = Dataset.create_empty(dest_dir, ds_meta)
    ds.add(t)
    ds.add(regions)
    ds.save()
