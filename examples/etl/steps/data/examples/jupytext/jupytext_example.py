# %%[markdown]

# # Jupytext example

# Install jupytext (or its extension for PyCharm) or see this directly in
# [Visual Studio Code](https://code.visualstudio.com/docs/python/jupyter-support-py)

# %% Get data
import pandas as pd
from owid.catalog import Dataset, Table

df = pd.DataFrame({
    'a': [1, 2, 3]
})

# %% Clean it up

df = df[df.a > 1]

# %% Create dataset in the `run` function using module-level variables

def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = 'jupytext_example'

    # use module-level variables
    t = Table(df.reset_index(drop=True))
    t.metadata.short_name = 'jupytext_example'

    ds.add(t)
    ds.save()
