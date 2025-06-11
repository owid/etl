"""Combine multiple literacy datasets into a single comprehensive dataset."""

import pandas as pd
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """
    Load and combine multiple literacy datasets.
    
    Combines:
    - literacy_1451_1800: Historical literacy from Buringh and van Zanden (1451-1800)
    - literacy_1900_1950: Progress of literacy in various countries (1900-1950)
    - literacy_1950: World illiteracy at mid-century
    - literacy_rates: Modern literacy rates from World Bank/CIA (2018)
    - education_sdgs: UNESCO SDG education data
    """
    
    # Load all input datasets
    ds_1451_1800 = paths.load_dataset("education", version="2025-06-09", namespace="garden")
    ds_1900_1950 = paths.load_dataset("education", version="2025-06-10", namespace="garden")
    ds_1950 = paths.load_dataset("education", version="2025-06-10", namespace="garden")
    ds_rates = paths.load_dataset("education", version="2018-04-18", namespace="garden")
    ds_unesco = paths.load_dataset("unesco", version="2025-05-01", namespace="garden")
    
    # Read tables
    tb_1451_1800 = ds_1451_1800.read("literacy_1451_1800")
    tb_1900_1950 = ds_1900_1950.read("literacy_1900_1950")
    tb_1950 = ds_1950.read("literacy_1950")
    tb_rates = ds_rates.read("literacy_rates")
    tb_unesco = ds_unesco.read("education_sdgs")
    
    # Initialize list to store standardized tables
    tables = []
    
    # Process 1451-1800 dataset
    if not tb_1451_1800.empty:
        tb_1451_1800_std = standardize_table(
            tb_1451_1800, 
            source="Buringh and van Zanden (2009)",
            period="1451-1800"
        )
        tables.append(tb_1451_1800_std)
    
    # Process 1900-1950 dataset
    if not tb_1900_1950.empty:
        tb_1900_1950_std = standardize_table(
            tb_1900_1950,
            source="UNESCO (1953)",
            period="1900-1950"
        )
        tables.append(tb_1900_1950_std)
    
    # Process 1950 dataset
    if not tb_1950.empty:
        tb_1950_std = standardize_table(
            tb_1950,
            source="UNESCO (1957)",
            period="1950"
        )
        tables.append(tb_1950_std)
    
    # Process modern rates dataset
    if not tb_rates.empty:
        tb_rates_std = standardize_table(
            tb_rates,
            source="World Bank, CIA World Factbook",
            period="1976-2018"
        )
        tables.append(tb_rates_std)
    
    # Process UNESCO SDG dataset (literacy-related indicators)
    if not tb_unesco.empty:
        # Filter for literacy-related indicators
        literacy_indicators = [
            "literacy_rate_adult_total",
            "literacy_rate_adult_female", 
            "literacy_rate_adult_male",
            "literacy_rate_youth_total",
            "literacy_rate_youth_female",
            "literacy_rate_youth_male"
        ]
        
        unesco_literacy = tb_unesco[tb_unesco.index.get_level_values('indicator').isin(literacy_indicators)]
        if not unesco_literacy.empty:
            tb_unesco_std = standardize_unesco_table(
                unesco_literacy,
                source="UNESCO SDG",
                period="2000-2022"
            )
            tables.append(tb_unesco_std)
    
    # Combine all tables
    if tables:
        tb_combined = pd.concat(tables, ignore_index=True)
        
        # Sort by country and year
        tb_combined = tb_combined.sort_values(['country', 'year'])
        
        # Reset index
        tb_combined = tb_combined.reset_index(drop=True)
    else:
        # Create empty table with expected structure
        tb_combined = pd.DataFrame(columns=[
            'country', 'year', 'literacy_rate', 'illiteracy_rate', 
            'age_group', 'sex', 'source', 'period'
        ])
    
    # Create new dataset
    ds_meadow = paths.create_dataset(
        tables=[tb_combined.set_index(['country', 'year'], verify_integrity=False)],
        default_metadata=ds_1451_1800.metadata
    )
    
    # Save dataset
    ds_meadow.save()


def standardize_table(tb, source, period):
    """Standardize a literacy table to common format."""
    tb_std = tb.copy()
    
    # Ensure we have the required columns
    required_cols = ['country', 'year']
    if not all(col in tb_std.columns for col in required_cols):
        if 'country' in tb_std.index.names:
            tb_std = tb_std.reset_index()
    
    # Initialize standard columns
    tb_std['source'] = source
    tb_std['period'] = period
    
    # Handle different column structures
    if 'literacy_rate' not in tb_std.columns:
        # Check for other literacy rate columns
        literacy_cols = [col for col in tb_std.columns if 'literacy' in col.lower() and 'rate' in col.lower()]
        if literacy_cols:
            tb_std['literacy_rate'] = tb_std[literacy_cols[0]]
        elif 'literate' in tb_std.columns:
            tb_std['literacy_rate'] = tb_std['literate']
    
    if 'illiteracy_rate' not in tb_std.columns:
        # Check for other illiteracy rate columns
        illiteracy_cols = [col for col in tb_std.columns if 'illiteracy' in col.lower() and 'rate' in col.lower()]
        if illiteracy_cols:
            tb_std['illiteracy_rate'] = tb_std[illiteracy_cols[0]]
        elif 'illiteracy_est' in tb_std.columns:
            tb_std['illiteracy_rate'] = tb_std['illiteracy_est']
        elif 'literacy_est' in tb_std.columns:
            tb_std['illiteracy_rate'] = 100 - tb_std['literacy_est']
        elif 'literacy_rate' in tb_std.columns:
            tb_std['illiteracy_rate'] = 100 - tb_std['literacy_rate']
    
    # Calculate missing literacy rate if we have illiteracy rate
    if 'literacy_rate' not in tb_std.columns and 'illiteracy_rate' in tb_std.columns:
        tb_std['literacy_rate'] = 100 - tb_std['illiteracy_rate']
    
    # Calculate missing illiteracy rate if we have literacy rate
    if 'illiteracy_rate' not in tb_std.columns and 'literacy_rate' in tb_std.columns:
        tb_std['illiteracy_rate'] = 100 - tb_std['literacy_rate']
    
    # Handle age group
    if 'age' in tb_std.columns:
        tb_std['age_group'] = tb_std['age']
    elif 'age_group' not in tb_std.columns:
        tb_std['age_group'] = 'All ages'
    
    # Handle sex
    if 'sex' not in tb_std.columns:
        tb_std['sex'] = 'Both sexes'
    
    # Select and order columns
    final_cols = ['country', 'year', 'literacy_rate', 'illiteracy_rate', 'age_group', 'sex', 'source', 'period']
    available_cols = [col for col in final_cols if col in tb_std.columns]
    
    return tb_std[available_cols]


def standardize_unesco_table(tb, source, period):
    """Standardize UNESCO SDG literacy table to common format."""
    tb_std = tb.reset_index()
    
    # Initialize result list
    result_rows = []
    
    # Process each indicator
    for idx, row in tb_std.iterrows():
        country = row['country']
        year = row['year']
        indicator = row['indicator']
        value = row['value']
        
        # Skip if value is missing
        if pd.isna(value):
            continue
        
        # Determine age group and sex from indicator
        if 'adult' in indicator:
            age_group = '15+'
        elif 'youth' in indicator:
            age_group = '15-24'
        else:
            age_group = 'All ages'
        
        if 'female' in indicator:
            sex = 'Female'
        elif 'male' in indicator:
            sex = 'Male'
        else:
            sex = 'Both sexes'
        
        # Create standardized row
        result_rows.append({
            'country': country,
            'year': year,
            'literacy_rate': value,
            'illiteracy_rate': 100 - value,
            'age_group': age_group,
            'sex': sex,
            'source': source,
            'period': period
        })
    
    return pd.DataFrame(result_rows)