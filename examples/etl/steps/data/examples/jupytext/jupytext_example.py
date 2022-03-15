# # Jupytext example
#
# Install jupytext to your Jupyter Lab environment https://github.com/mwouts/jupytext, then right click the
# file and open with `Jupytext Notebook`.

# There's also a [VS Code extension](https://marketplace.visualstudio.com/items?itemName=donjayamanne.vscode-jupytext),
# but it's unfortunately not usable [due to this bug](https://github.com/notebookPowerTools/vscode-jupytext/issues/9).

# ## Get data

# +
import pandas as pd
from owid.catalog import Dataset, Table

df = pd.DataFrame({
    'a': [1, 2, 3]
})
# -

# ## Clean data

df = df[df.a > 1]


# ## Create dataset in the `run` function using module-level variables

def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = 'jupytext_example'

    # use module-level variables
    t = Table(df.reset_index(drop=True))
    t.metadata.short_name = 'jupytext_example'

    ds.add(t)
    ds.save()


