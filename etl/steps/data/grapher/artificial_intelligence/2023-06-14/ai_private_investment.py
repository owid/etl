"""Load a garden dataset and create a grapher dataset."""

from typing import cast

import numpy as np
from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("ai_private_investment"))

    # Read table from garden dataset.
    tb = ds_garden["ai_private_investment"]
    tb.reset_index(inplace=True)

    # For plotting in grapher add focus area to country and then drop focus_area column
    tb["country"] = np.where(tb["focus_area"].notna(), tb["focus_area"], tb["country"])
    tb.drop("focus_area", axis=1, inplace=True)
    tb.set_index(["country", "year"], inplace=True)

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
