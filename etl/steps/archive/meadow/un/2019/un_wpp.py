#!/usr/bin/env python
# pyright: reportUnusedExpression=false
# coding: utf-8
# %% [markdown]
# # UN World Population Prospects (2019)

# %%
import tempfile
import zipfile

import pandas as pd
from owid import walden
from owid.catalog import Dataset, Table

from etl.steps.data import converters

# %% [markdown]
# ## Find walden file

# %%
walden_ds = walden.Catalog().find_one("wpp", "2019", "standard_projections")
walden_ds


# %% [markdown]
# ## Unzip the data

# %%
temp_dir = tempfile.mkdtemp()

zipfile.ZipFile(walden_ds.local_path).extractall(temp_dir)

# %%
# !ls {temp_dir}/WPP2019

# %% [markdown]
# ## Total population

# %%
df = pd.read_csv(f"{temp_dir}/WPP2019/WPP2019_TotalPopulationBySex.csv")


# %%
df.head()


# %%
df.columns = [
    "loc_id",
    "location",
    "var_id",
    "variant",
    "year",
    "mid_period",
    "population_male",
    "population_female",
    "population_total",
    "population_density",
]


# %%
t1 = Table(df[["loc_id", "location"]].drop_duplicates().set_index("loc_id"))
t1.metadata.short_name = "location_codes"


# %%
t2 = Table(df[["var_id", "variant"]].drop_duplicates().set_index("var_id"))
t2.metadata.short_name = "variant_codes"


# %%
df.drop(columns=["loc_id", "var_id"], inplace=True)


# %%
for col in ["location", "variant"]:
    df[col] = df[col].astype("category")


# %%
df.set_index(["variant", "location", "year"], inplace=True)


# %%
df


# %%
df.index.levels[0]  # type: ignore


# %%
t3 = Table(df)
t3.metadata.short_name = "total_population"

# %% [markdown]
# ## Fertility by age

# %%
df = pd.read_csv(f"{temp_dir}/WPP2019/WPP2019_Fertility_by_Age.csv")


# %%
df.head()


# %%
df.drop(columns=["LocID", "VarID", "MidPeriod", "AgeGrpStart", "AgeGrpSpan"], inplace=True)


# %%
df.columns = [
    "location",
    "variant",
    "year_range",
    "age_group",
    "asfr",
    "pasfr",
    "births",
]


# %%
df.head()


# %%
for col in ["location", "variant", "year_range", "age_group"]:
    df[col] = df[col].astype("category")


# %%
df.set_index(["variant", "location", "year_range", "age_group"], inplace=True)


# %%
t4 = Table(df)
t4.metadata.short_name = "fertility_by_age"

# %% [markdown]
# ## Population by age and sex

# %%
df = pd.read_csv(f"{temp_dir}/WPP2019/WPP2019_PopulationByAgeSex_Medium.csv")


# %%
df.head()


# %%
df.drop(columns=["LocID", "VarID", "MidPeriod", "AgeGrpStart", "AgeGrpSpan"], inplace=True)


# %%
df.columns = [
    "location",
    "variant",
    "year",
    "age_group",
    "population_male",
    "population_female",
    "population_total",
]


# %%
df.head()


# %%
for col in ["location", "variant", "age_group"]:
    df[col] = df[col].astype("category")


# %%
df.set_index(["variant", "location", "year", "age_group"], inplace=True)


# %%
df.head()


# %%
t5 = Table(df)
t5.metadata.short_name = "population_by_age_sex"


# %% [markdown]
# ## Save the dataset to disk

# %%
def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = converters.convert_walden_metadata(walden_ds)
    ds.metadata.namespace = "un"
    ds.metadata.short_name = "un_wpp"
    ds.add(t1)
    ds.add(t2)
    ds.add(t3)
    ds.add(t4)
    ds.add(t5)
    ds.save()
