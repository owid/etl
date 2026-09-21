"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("avian_influenza_ah5n1"))

    # Read table from garden dataset.
    tb = ds_garden["avian_influenza_ah5n1"].reset_index()

    #
    # Process data.
    #
    # Yearly
    tb["year"] = tb["date"].dt.year
    tb = tb.groupby(["year", "country"], as_index=False)["avian_cases"].sum()

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    ds_grapher.metadata.short_name = "avian_influenza_ah5n1_year"
    ds_grapher.metadata.title = "Human Cases with Highly Pathogenic Avian Influenza A/H5N1 (WHO, 2023) [YEARLY]"
    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
