"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid.catalog import Dataset

from etl import grapher_helpers as gh
from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("cset"))

    # Read table from garden dataset.
    tb = ds_garden["cset"]
    tb = tb.reset_index()
    tb.set_index(["country", "year", "field"], inplace=True)

    # Expand dimensions into columns.
    expanded_tb = gh.expand_dimensions(tb)

    # Set display name to its `field` for each column
    for col in expanded_tb.columns:
        dim_filters = expanded_tb[col].metadata.additional_info["dimensions"]["filters"]
        assert len(dim_filters) == 1
        expanded_tb[col].metadata.display["name"] = dim_filters[0]["value"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset
    ds_grapher = create_dataset(dest_dir, tables=[expanded_tb], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
