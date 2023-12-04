"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("dna_sequencing.xls")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(
        columns={
            "Date": "year",
            "Cost per Mb": "cost_per_mb",
            "Cost per Genome": "cost_per_genome",
        }
    )

    tb["year"] = tb["year"].dt.year

    # Sort by cost then drop duplicates to keep the lowest cost per genome for each year
    tb = tb.sort_values(by="cost_per_genome")
    tb = tb.drop_duplicates(subset=["year"], keep="first")

    # Add a world row and sort by year
    tb["country"] = "World"
    tb = tb.sort_values(by="year")

    # Convert cost per mb to cost per gb
    tb["cost_per_gb"] = tb.cost_per_mb * 1000
    tb = tb.drop(columns="cost_per_mb")
    tb[["cost_per_gb", "cost_per_genome"]] = tb[["cost_per_gb", "cost_per_genome"]].round(2)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
