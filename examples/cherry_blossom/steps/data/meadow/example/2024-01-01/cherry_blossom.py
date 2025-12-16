"""Example meadow step for cherry blossom data."""

from pathlib import Path

from owid.catalog import Dataset, Table
import pandas as pd


def run(dest_dir: str) -> None:
    """Load raw cherry blossom data and save to meadow."""
    # Create sample data (in real use, this would come from a snapshot)
    data = {
        "year": [2020, 2021, 2022, 2023, 2024],
        "country": ["Japan", "Japan", "Japan", "Japan", "Japan"],
        "bloom_date": ["2020-03-20", "2021-03-18", "2022-03-22", "2023-03-15", "2024-03-19"],
    }
    df = pd.DataFrame(data)

    # Create table with metadata
    tb = Table(df, short_name="cherry_blossom")
    tb = tb.set_index(["country", "year"])

    # Create and save dataset
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "cherry_blossom"
    ds.metadata.namespace = "example"
    ds.metadata.version = "2024-01-01"
    ds.metadata.channel = "meadow"
    ds.metadata.title = "Cherry Blossom Bloom Dates"
    ds.add(tb)
    ds.save()
