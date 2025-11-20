"""
Test examples to verify the detect-outdated-practices extension.
Open this file in VS Code to see the warnings.
"""

# ========================================
# PATTERN 1: dest_dir usage
# ========================================

# Example 1: Function parameter with comma
def create_dataset(dest_dir, table_name):
    pass

# Example 2: Function parameter with closing parenthesis
def save_data(dest_dir):
    pass

# Example 3: Type annotation
def process_files(dest_dir: str):
    pass

# Example 4: Multiple parameters
def complex_function(source_file, dest_dir, output_format):
    pass

# Example 5: String literal (should also be detected)
config = {"path": "dest_dir"}

# Example 6: Type annotation with Path
from pathlib import Path
def handle_path(dest_dir: Path):
    pass

# Example 7: Variable usage with comma
my_var = dest_dir, other_var

# Example 8: In function call
create_dataset(dest_dir=path, table_name="test")

# This should NOT be detected (not followed by :,) or whitespace):
# dest_directory = "/path/to/dir"  # Won't match because it's dest_directory, not dest_dir

# ========================================
# PATTERN 2: geo.harmonize_countries
# ========================================

from etl.data_helpers import geo

# Example 1: Basic usage with countries_file
tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)

# Example 2: Without assignment
geo.harmonize_countries(df, countries_file="mapping.json")

# Example 3: With excluded countries
result = geo.harmonize_countries(
    tb,
    countries_file=paths.country_mapping_path,
    excluded_countries=["World"]
)

# ========================================
# PATTERN 3: paths.load_dependency
# ========================================

from etl.helpers import PathFinder
paths = PathFinder(__file__)

# Example 1: Loading a dataset
ds = paths.load_dependency("namespace/version/dataset_name")

# Example 2: Direct usage
tb = paths.load_dependency("un/2023/population")["table_name"]

# Example 3: Multiple dependencies
ds1 = paths.load_dependency("source1/version/name1")
ds2 = paths.load_dependency("source2/version/name2")
