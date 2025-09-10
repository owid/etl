#!/usr/bin/env python3
"""
Example showing how to use RegionAggregator for population-weighted regional aggregates
of internet users data. This demonstrates the proper approach for ETL steps.

Usage in an actual ETL step would look like this:

```python
from etl.helpers import PathFinder
from etl.data_helpers.geo import add_population_to_table

paths = PathFinder(__file__)

def run() -> None:
    # Load your data
    ds_input = paths.load_dataset("some_dataset") 
    tb = ds_input["table_name"].reset_index()
    
    # Add population for weighting
    ds_population = paths.load_dataset("population")
    tb = add_population_to_table(tb, ds_population)
    
    # Method 1: Simple weighted sum approach (recommended for shares/percentages)
    tb['indicator_weighted'] = tb['your_indicator'] * tb['population']
    
    tb = paths.region_aggregator(
        aggregations={
            'indicator_weighted': 'sum',
            'population': 'sum'
        }
    ).add_aggregates(tb)
    
    # Calculate weighted average for regions
    tb['your_indicator_weighted'] = tb['indicator_weighted'] / tb['population']
    
    # Save dataset
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
```
"""

from owid.catalog import Dataset, Table
from etl.paths import DATA_DIR
from etl.data_helpers.geo import add_population_to_table, add_regions_to_table

def demo_population_weighted_aggregation():
    """Demonstrate population-weighted regional aggregation for internet usage data."""
    
    print("=== Population-Weighted Regional Aggregation Demo ===\n")
    
    # Load data
    print("Loading internet usage data...")
    ds_wdi = Dataset(DATA_DIR / 'garden' / 'worldbank_wdi' / '2025-09-08' / 'wdi')
    tb = ds_wdi['wdi'].reset_index()
    
    # Filter for recent internet data
    tb_internet = tb[tb['it_net_user_zs'].notna() & (tb['year'] == 2023)][
        ['country', 'year', 'it_net_user_zs']
    ].copy()
    
    print(f"Working with {len(tb_internet)} countries for year 2023")
    
    # Convert to Table for proper metadata handling
    tb_internet = Table(tb_internet).copy_metadata(ds_wdi['wdi'])
    
    # Add population data for weighting
    print("Adding population data...")
    ds_population = Dataset(DATA_DIR / 'garden' / 'demography' / '2024-07-15' / 'population')
    tb_with_pop = add_population_to_table(tb_internet, ds_population, warn_on_missing_countries=False)
    
    # Remove countries without population data
    tb_clean = tb_with_pop.dropna(subset=['population']).reset_index(drop=True)
    print(f"Countries with both internet and population data: {len(tb_clean)}")
    
    # Create weighted values (this is the key step for population weighting)
    tb_clean['internet_usage_weighted'] = tb_clean['it_net_user_zs'] * tb_clean['population']
    
    # Load region datasets
    ds_regions = Dataset(DATA_DIR / 'garden' / 'regions' / '2023-01-01' / 'regions')
    ds_income_groups = Dataset(DATA_DIR / 'garden' / 'wb' / '2025-07-01' / 'income_groups')
    
    # Add regional aggregates using the weighted approach
    print("Calculating regional aggregates...")
    tb_with_regions = add_regions_to_table(
        tb=tb_clean,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions={
            "World": {},
            "Europe": {},
            "Asia": {},
            "Africa": {},
            "North America": {},
            "South America": {},
            "Oceania": {},
            "High-income countries": {},
            "Upper-middle-income countries": {},
            "Lower-middle-income countries": {},
            "Low-income countries": {}
        },
        aggregations={
            'internet_usage_weighted': 'sum',  # Sum of weighted values
            'population': 'sum',                # Sum of populations
            'it_net_user_zs': 'mean'           # Simple mean for comparison
        },
        min_num_values_per_year=1
    )
    
    # Calculate population-weighted averages for regional aggregates
    tb_final = tb_with_regions.reset_index()
    
    # Identify regional rows
    regions = [
        "World", "Europe", "Asia", "Africa", "North America", "South America", "Oceania",
        "High-income countries", "Upper-middle-income countries", 
        "Lower-middle-income countries", "Low-income countries"
    ]
    
    region_mask = tb_final['country'].isin(regions)
    
    # For regions: calculate weighted average
    tb_final.loc[region_mask, 'internet_usage_pop_weighted'] = (
        tb_final.loc[region_mask, 'internet_usage_weighted'] / 
        tb_final.loc[region_mask, 'population']
    )
    
    # For countries: use original value
    tb_final.loc[~region_mask, 'internet_usage_pop_weighted'] = (
        tb_final.loc[~region_mask, 'it_net_user_zs']
    )
    
    # Display results
    print(f"\nInternet Usage in 2023 - Regional Comparison:")
    print(f"{'Region':<30} {'Simple Mean':<12} {'Pop-Weighted':<12} {'Difference':<12} {'Total Pop (M)'}")
    print("-" * 85)
    
    regional_data = tb_final[tb_final['country'].isin(regions)].sort_values('internet_usage_pop_weighted', ascending=False)
    
    for _, row in regional_data.iterrows():
        simple = row['it_net_user_zs']
        weighted = row['internet_usage_pop_weighted'] 
        diff = weighted - simple
        pop_millions = row['population'] / 1_000_000
        
        print(f"{row['country']:<30} {simple:>8.1f}%    {weighted:>8.1f}%    {diff:>+7.1f}pp    {pop_millions:>8.0f}M")
    
    # Show specific example
    print(f"\nDetailed Example - Asia (why population weighting matters):")
    print(f"{'Country':<20} {'Internet %':<12} {'Population':<15} {'Contribution'}")
    print("-" * 65)
    
    # Get some Asian countries to illustrate
    asian_countries = ['China', 'India', 'Indonesia', 'Pakistan', 'Bangladesh', 'Japan', 'Philippines', 'Vietnam']
    asia_data = tb_final[
        (tb_final['country'].isin(asian_countries)) & 
        (tb_final['it_net_user_zs'].notna())
    ].sort_values('population', ascending=False)
    
    total_asian_pop = asia_data['population'].sum()
    
    for _, row in asia_data.head(6).iterrows():
        pop_millions = row['population'] / 1_000_000
        contribution = row['population'] / total_asian_pop * 100
        print(f"{row['country']:<20} {row['it_net_user_zs']:>8.1f}%    {pop_millions:>10.1f}M    {contribution:>8.1f}%")
    
    print(f"\nThis shows why population weighting is important:")
    print(f"- China and India have low internet penetration but massive populations")
    print(f"- Their influence in the regional average should reflect their population size")
    print(f"- Simple mean would overweight small, high-penetration countries")
    
    return tb_final

def show_pathfinder_example():
    """Show how this would be used in a real ETL step with PathFinder."""
    
    example_code = '''
# In a real ETL step (e.g., etl/steps/data/garden/my_namespace/version/step.py):

from etl.helpers import PathFinder
from etl.data_helpers.geo import add_population_to_table

paths = PathFinder(__file__)

def run() -> None:
    """Main function for the ETL step."""
    
    # Load input data
    ds_input = paths.load_dataset("input_dataset")
    tb = ds_input["table_name"].reset_index()
    
    # Add population for weighting (if not already present)
    ds_population = paths.load_dataset("population")
    tb = add_population_to_table(tb, ds_population)
    
    # Create weighted column for your indicator of interest
    tb['your_indicator_weighted'] = tb['your_indicator'] * tb['population']
    
    # Use PathFinder's region_aggregator (cleanest approach)
    tb = paths.region_aggregator(
        aggregations={
            'your_indicator_weighted': 'sum',
            'population': 'sum',
            'your_indicator': 'mean'  # Keep simple mean for comparison
        }
    ).add_aggregates(tb)
    
    # Calculate population-weighted averages for regional aggregates
    regions = ["World", "Europe", "Asia", etc...]  # Define your regions
    region_mask = tb['country'].isin(regions)
    
    tb.loc[region_mask, 'your_indicator_pop_weighted'] = (
        tb.loc[region_mask, 'your_indicator_weighted'] / 
        tb.loc[region_mask, 'population']
    )
    
    # Format and save
    tb = tb.format(['country', 'year'])
    ds_output = paths.create_dataset(tables=[tb])
    ds_output.save()
'''
    
    print("\n" + "="*80)
    print("HOW TO USE IN A REAL ETL STEP:")
    print("="*80)
    print(example_code)

if __name__ == "__main__":
    # Run the demo
    result_table = demo_population_weighted_aggregation()
    
    # Show how to use this in practice
    show_pathfinder_example()
    
    print("\n" + "="*80)
    print("KEY TAKEAWAYS:")
    print("="*80)
    print("1. For share/percentage data, use population-weighted aggregation")
    print("2. Create weighted values: indicator * population")  
    print("3. Aggregate: sum(weighted_values) / sum(population)")
    print("4. Use paths.region_aggregator() in ETL steps for best practice")
    print("5. Population weighting gives more representative regional averages")