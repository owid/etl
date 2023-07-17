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
    df["Architecture"] = df.apply(add_asterisks, axis=1)
    df["training_computation_petaflop"] = df["Approx Compute (FLOP)"] / 1e15
    df.drop("Approx Compute (FLOP)", axis=1, inplace=True)
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb.set_index(["architecture", "year"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_asterisks(row):
    if row["Architecture"] == "Gopher":
        if row["MMLU avg"] <= 0.26:  # <= 1 billion
            return "Gopher" + "*"
        elif row["MMLU avg"] <= 0.28:  # <= 10 billion
            return "Gopher" + "**"
        elif row["MMLU avg"] <= 0.30:  # <= 100 billion
            return "Gopher" + "***"
        else:
            return "Gopher" + "****"
    elif row["Architecture"] == "PaLM":
        if row["MMLU avg"] <= 0.26:  # <= 10 billion
            return "PaLM" + "*"
        elif row["MMLU avg"] <= 0.54:  # <= 100 billion
            return "PaLM" + "**"
        elif row["MMLU avg"] <= 0.63:  # <= 1 trillion
            return "PaLM" + "***"
        else:
            return "PaLM" + "****"
    else:
        return row["Architecture"]
