"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    snap = cast(Snapshot, paths.load_dependency("epoch_llms.csv"))
    df = pd.read_csv(snap.path)
    df["training_computation_petaflop"] = df["Approx Compute (FLOP)"] / 1e15
    df.drop("Approx Compute (FLOP)", axis=1, inplace=True)
    df["MMLU avg"] *= 100
    df["Architecture"] = df["Architecture"].str.replace("Llama", "LLaMA", regex=True)

    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb.set_index(["architecture", "year"], inplace=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
