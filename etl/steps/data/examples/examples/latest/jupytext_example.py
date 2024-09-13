# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: 'Python 3.9.13 (''.venv'': uv)'
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Jupytext example
#
# Install jupytext to your Jupyter Lab environment https://github.com/mwouts/jupytext, then right click the
# file and open with `Jupytext Notebook`.

# %% [markdown]
# There's also a [VS Code extension](https://marketplace.visualstudio.com/items?itemName=congyiwu.vscode-jupytext&ssr=false#overview).

# %% [markdown]
# ## Get data

# %%
from owid.catalog import Dataset, Table, utils

from etl.paths import DATA_DIR

dataset = Dataset(DATA_DIR / "garden/ggdc/2020-10-01/ggdc_maddison")
table = dataset["maddison_gdp"]


# %% [markdown]
# ## Clean data

# %%
table = table.dropna(subset=["gdp"]).query("year >= 2020")

# %% [markdown]
# ## Create dataset in the `run` function using module-level variables


# %%
def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "jupytext_example"
    ds.metadata.namespace = "examples"

    # use module-level variables
    t = Table(table.reset_index(drop=True))
    t.metadata.short_name = "jupytext_example"

    ds.add(utils.underscore_table(t))
    ds.save()
