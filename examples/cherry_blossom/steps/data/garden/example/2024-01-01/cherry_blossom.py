"""Example garden step for cherry blossom data."""

from pathlib import Path

from owid.catalog import Dataset, Table
import pandas as pd


def run(dest_dir: str) -> None:
    """Process cherry blossom data in garden."""
    # Load meadow dataset
    # In a real scenario, you'd use PathFinder to find the dependency
    meadow_path = Path(dest_dir).parent.parent.parent.parent / "meadow/example/2024-01-01/cherry_blossom"
    ds_meadow = Dataset(meadow_path)

    # Get table
    tb = ds_meadow["cherry_blossom"].reset_index()

    # Add processing: convert bloom_date to day of year
    tb["bloom_date"] = pd.to_datetime(tb["bloom_date"])
    tb["bloom_day_of_year"] = tb["bloom_date"].dt.dayofyear

    # Set index
    tb = tb.set_index(["country", "year"])

    # Create garden dataset
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "cherry_blossom"
    ds.metadata.namespace = "example"
    ds.metadata.version = "2024-01-01"
    ds.metadata.channel = "garden"
    ds.metadata.title = "Cherry Blossom Bloom Dates (Processed)"
    ds.add(tb)
    ds.save()
