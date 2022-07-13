# %%[markdown]

# # VS Code cells example

# If you open this file in VS Code, you can [run individual cells](https://code.visualstudio.com/docs/python/jupyter-support-py) separated by `# %%`. However, working
# in the interactive window is not as pleasant as working in a notebook, so we'd recommend installing
# [this VS Code extension](https://marketplace.visualstudio.com/items?itemName=donjayamanne.vscode-jupytext)
# and running it as a notebook instead.


# %% Get data
import pandas as pd
from owid.catalog import Dataset, Table

df = pd.DataFrame({"a": [1, 2, 3]})

# %% Clean it up

df = df[df.a > 1]

# %% Create dataset in the `run` function using module-level variables


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "vs_code_cells_example"
    ds.metadata.namespace = "examples"

    # use module-level variables
    t = Table(df.reset_index(drop=True))
    t.metadata.short_name = "vs_code_cells_example"

    ds.add(t)
    ds.save()
