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
    if len(df[col].unique()) == 1:
        df.drop(col, axis=1, inplace=True)

df.head()

# ## Standardise names

df.columns = [
    "region_code",
    "region_name",
    "ind_code",
    "ind_name",
    "unit_code",
    "unit_name",
    "value",
]

df.head()

df["ind_code"] = df.ind_code.apply(lambda s: s.lower())

# ## Sanity checks

# #### One unit per indicator

assert len(df[["ind_code", "unit_code"]].drop_duplicates()) == len(df.ind_code.unique())

# #### One name per indicator

assert len(df[["ind_code", "ind_name"]].drop_duplicates()) == len(df.ind_code.unique())

# #### One name per unit

assert len(df[["unit_code", "unit_name"]].drop_duplicates()) == len(
    df.unit_code.unique()
)

# #### One name per region

assert len(df[["region_code", "region_name"]].drop_duplicates()) == len(
    df.region_code.unique()
)

# ## Capture important metadata pre-pivot

ind = df[["ind_code", "ind_name"]].drop_duplicates().set_index("ind_code").ind_name

ind_unit_code = (
    df[["ind_code", "unit_code"]].drop_duplicates().set_index("ind_code").unit_code
)

ind_unit_name = (
    df[["ind_code", "unit_name"]].drop_duplicates().set_index("ind_code").unit_name
)

regions = (
    df[["region_code", "region_name"]]
    .drop_duplicates()
    .set_index("region_code")
    .region_name
)

# ## Strip back and pivot

df = df.drop(["region_name", "ind_name", "unit_name", "unit_code"], axis=1)

df.head()

df = df.pivot("region_code", "ind_code", "value")

df.head()

# ## Add metadata

t = Table(df)

for ind_code in t.columns:
    meta = t[ind_code].metadata
    meta.short_name = ind_code.lower()
    meta.title = ind.loc[ind_code]
    short_unit = ind_unit_code[ind_code]
    meta.short_unit = short_unit.lower() if not pd.isnull(short_unit) else None
    meta.unit = ind_unit_name[ind_code]

t.head()

t.metadata.short_name = "regional_wellbeing"

# ## Regions

regions = Table(regions)
regions.metadata.short_name = "regions"


# ## Save the dataset


def run(dest_dir: str) -> None:
    ds_meta = convert_walden_metadata(walden_ds)
    ds = Dataset.create_empty(dest_dir, ds_meta)
    ds.add(t)
    ds.add(regions)
