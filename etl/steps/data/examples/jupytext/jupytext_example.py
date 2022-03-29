# # Jupytext example
#
# Install jupytext to your Jupyter Lab environment https://github.com/mwouts/jupytext, then right click the
# file and open with `Jupytext Notebook`.

# There's also a [VS Code extension](https://marketplace.visualstudio.com/items?itemName=donjayamanne.vscode-jupytext),
# but it's unfortunately not usable [due to this bug](https://github.com/notebookPowerTools/vscode-jupytext/issues/9).

# ## Get data

# +
import pandas as pd
from owid.walden import Catalog
from owid.catalog import Dataset, Table, utils

walden_ds = Catalog().find_one("wb", "2021-07-01", "wb_income")
local_path = walden_ds.ensure_downloaded()
df = pd.read_excel(local_path)

# -

# ## Clean data

df = df.dropna(subset=["Income group"])


# ## Create dataset in the `run` function using module-level variables


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "jupytext_example"

    # use module-level variables
    t = Table(df.reset_index(drop=True))
    t.metadata.short_name = "jupytext_example"

    ds.add(utils.underscore_table(t))
    ds.save()
