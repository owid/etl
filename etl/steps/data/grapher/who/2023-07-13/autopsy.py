"""Load a garden dataset and create a grapher dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset, grapher_checks
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("autopsy"))
    # Load Paratz paper dataset
    snap = cast(Snapshot, paths.load_dependency("paratz.csv"))
    paratz_df = pd.read_csv(snap.path)
    tb_paratz = Table(paratz_df, short_name="paratz")

    # Read table from garden dataset.
    tb_who = ds_garden["autopsy"].reset_index()
    assert all(tb_who["sex"].drop_duplicates() == "ALL")
    tb_who = tb_who.drop(columns="sex")
    tb_who = tb_who.rename(columns={"value": "autopsy_rate"})
    # Removing overlapping countries from Paratz as we prefer WHO as a source.
    tb_paratz = tb_paratz[~tb_paratz["country"].isin(tb_who["country"])]
    tb = Table(pd.concat([tb_paratz, tb_who]), short_name=paths.short_name)

    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb])

    ds_grapher.update_metadata(paths.metadata_path)
    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
