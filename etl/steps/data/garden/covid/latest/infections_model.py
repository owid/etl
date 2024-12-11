"""Load a meadow dataset and create a garden dataset."""
from typing import Any, Dict

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("infections_model")

    # Read table from meadow dataset.
    tb = ds_meadow.read("infections_model")

    #
    # Process data.
    #
    # Extract origins
    origins = extract_origins_per_model(tb)

    # Reshape columns
    ## Unpivot
    tb = tb.melt(["country", "date"]).dropna(subset="value")

    ## Split column
    tb[["indicator", "estimate"]] = tb["variable"].str.split("__", expand=True)

    ## Pivot
    tb = tb.pivot(index=["country", "date", "estimate"], columns="indicator", values="value").reset_index()

    # Format
    tb = tb.format(["country", "date", "estimate"])

    # Insert origins
    for name, origin in origins.items():
        tb[f"{name}_infections"].metadata.origins = origin
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def extract_origins_per_model(tb: Table) -> Dict[str, Any]:
    names = [
        "icl",
        "ihme",
        "lshtm",
        "yyg",
    ]
    origins = {}
    for name in names:
        columns = tb.filter(regex=name).columns
        assert len(columns) == 3, f"Unexpected number of columns for {name}"
        origins[name] = tb[columns[0]].metadata.origins
    return origins
