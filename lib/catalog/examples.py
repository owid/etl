#!/usr/bin/env python
"""Examples demonstrating how to use the owid-catalog library.

This script shows the main features and APIs available in owid-catalog.
Run with: python examples.py
"""

from owid.catalog import Client, Table
from owid.catalog import processing as pr


def example_client_charts():
    """Example: Fetching data from published charts."""
    print("\n=== Charts API ===")

    client = Client()

    # Fetch chart data as DataFrame
    df = client.charts.get_data("life-expectancy")
    print(f"Chart data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"First few rows:\n{df.head()}")

    # Get chart metadata
    metadata = client.charts.metadata("life-expectancy")
    print(f"\nChart has {len(metadata.get('columns', {}))} columns")

    # Get full chart with config
    chart = client.charts.fetch("life-expectancy")
    print(f"\nChart title: {chart.title}")
    print(f"Chart URL: {chart.url}")


def example_client_search():
    """Example: Searching for charts and pages."""
    print("\n=== Search API ===")

    client = Client()

    # Search for charts
    charts = client.search.charts("gdp per capita", limit=3)
    print(f"Found {charts.total} charts, showing first {len(charts)}:")
    for chart in charts:
        print(f"  - {chart.title} ({chart.slug})")

    # Search for pages/articles
    pages = client.search.pages("climate change", limit=3)
    print(f"\nFound {pages.total} pages, showing first {len(pages)}:")
    for page in pages:
        print(f"  - {page.title}")


def example_client_indicators():
    """Example: Semantic search for indicators."""
    print("\n=== Indicators API ===")

    client = Client()

    # Search for indicators using natural language
    indicators = client.indicators.search("renewable energy", limit=3)
    print(f"Found {indicators.total} indicators, showing first {len(indicators)}:")
    for ind in indicators:
        print(f"  - {ind.title}")
        print(f"    Score: {ind.score:.3f}, Charts: {ind.n_charts}")
        print(f"    Path: {ind.catalog_path}")

    # Load data from an indicator
    if indicators:
        print(f"\nLoading first indicator...")
        table = indicators[0].load()
        print(f"Table shape: {table.shape}")


def example_client_datasets():
    """Example: Querying and loading datasets from catalog."""
    print("\n=== Datasets API ===")

    client = Client()

    # Find datasets by criteria
    results = client.datasets.find(table="population", namespace="un")
    print(f"Found {len(results)} datasets matching criteria:")
    for result in results[:3]:
        print(f"  - {result.path}")

    # Load a specific dataset
    if results:
        table = results[0].load()
        print(f"\nLoaded table shape: {table.shape}")
        print(f"Columns: {list(table.columns)}")

    # Find and load the latest version
    latest = client.datasets.find_latest(table="population", namespace="un")
    print(f"\nLatest population table: {latest.metadata.short_name}")


def example_tables_and_metadata():
    """Example: Working with Tables and metadata."""
    print("\n=== Tables and Metadata ===")

    # Create a table from a DataFrame
    import pandas as pd

    df = pd.DataFrame(
        {
            "country": ["USA", "China", "India"],
            "year": [2020, 2020, 2020],
            "gdp": [21427, 14722, 2622],
        }
    )

    tb = Table(df)
    print(f"Created table with shape: {tb.shape}")

    # Add column metadata by setting properties
    tb["gdp"].metadata.unit = "billion current US$"
    tb["gdp"].metadata.short_unit = "$"
    tb["gdp"].metadata.title = "GDP (current US$)"
    tb["gdp"].metadata.description = "Gross Domestic Product in current US dollars"

    print("\nColumn metadata for 'gdp':")
    print(f"  Unit: {tb['gdp'].metadata.unit}")
    print(f"  Title: {tb['gdp'].metadata.title}")

    # Format table (underscore columns, set index, sort)
    tb = tb.format(["country", "year"], short_name="gdp_table")
    print(f"\nFormatted table:\n{tb}")


def example_processing_functions():
    """Example: Using processing functions that preserve metadata."""
    print("\n=== Processing Functions ===")

    import pandas as pd

    # Create two tables
    tb1 = Table(pd.DataFrame({"country": ["USA", "China"], "pop": [331, 1411]}))
    tb1["pop"].metadata.unit = "million people"

    tb2 = Table(pd.DataFrame({"country": ["USA", "China"], "gdp": [21, 14]}))
    tb2["gdp"].metadata.unit = "trillion USD"

    # Merge tables (metadata is preserved)
    merged = pr.merge(tb1, tb2, on="country")
    print(f"Merged table:\n{merged}")
    print("\nMetadata preserved:")
    print(f"  pop unit: {merged['pop'].metadata.unit}")
    print(f"  gdp unit: {merged['gdp'].metadata.unit}")

    # Concatenate tables
    tb3 = Table(pd.DataFrame({"country": ["India"], "pop": [1393]}))
    tb3["pop"].metadata.unit = "million people"

    combined = pr.concat([tb1, tb3])
    print(f"\nConcatenated table:\n{combined}")


def example_reading_files():
    """Example: Reading files with metadata preservation."""
    print("\n=== Reading Files ===")

    # The processing module provides readers that preserve metadata
    print("Available readers:")
    print("  - pr.read_csv()")
    print("  - pr.read_excel()")
    print("  - pr.read_feather()")
    print("  - pr.read_parquet()")
    print("  - pr.read_rds() (R data files)")
    print("  - pr.read_rda() (R data files)")

    # Example (requires actual files):
    # tb = pr.read_csv("data.csv")
    # tb = pr.read_excel("data.xlsx", sheet_name="Sheet1")


def main():
    """Run all examples."""
    print("=" * 60)
    print("OWID Catalog Library Examples")
    print("=" * 60)

    # Note: Some examples make network requests and may be slow
    print("\nNote: Examples make network requests and may take a moment...")

    # Run examples
    try:
        example_client_charts()
    except Exception as e:
        print(f"Charts example failed: {e}")

    try:
        example_client_search()
    except Exception as e:
        print(f"Search example failed: {e}")

    try:
        example_client_indicators()
    except Exception as e:
        print(f"Indicators example failed: {e}")

    try:
        example_client_datasets()
    except Exception as e:
        print(f"Datasets example failed: {e}")

    try:
        example_tables_and_metadata()
    except Exception as e:
        print(f"Tables example failed: {e}")

    try:
        example_processing_functions()
    except Exception as e:
        print(f"Processing example failed: {e}")

    example_reading_files()

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


# if __name__ == "__main__":
#     main()
