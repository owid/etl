# Add Validation

Can you add data validation to <% if $ARGUMENTS %>the ETL step `$ARGUMENTS`<% else %>this ETL step<% endif %> based on the validation patterns and best practices outlined in `.claude/docs/validation.md`?

<% if $ARGUMENTS %>
For the step: `etl/steps/data/garden/$ARGUMENTS.py`

You need to:
1. Create a **new separate file**: `etl/steps/data/garden/$ARGUMENTS_validation.py` (in the same directory)
2. Add validation code to this new file (NOT to the main step file)
3. Add the validation step to the appropriate DAG file
<% endif %>

Please follow these requirements:

## Step-by-Step Instructions

### 1. Create the Validation File

**IMPORTANT**: Create a **NEW FILE** - do NOT modify the main ETL step file!

- **File location**: Same directory as the main step file
- **File name**: `[short_name]_validation.py` (e.g., if main step is `cherry_blossom.py`, create `cherry_blossom_validation.py`)
- **File contents**: Follow the template in `.claude/docs/validation.md` under "Validation Step File"

### 2. Write Validation Code in the New File

The validation file should:
- Start with standard imports and setup:
  ```python
  from pathlib import Path
  from structlog import get_logger
  from owid.catalog import Table
  from etl.helpers import PathFinder

  log = get_logger()
  paths = PathFinder(__file__)
  ```
- Have a `run()` function that:
  - Loads the dataset using `paths.load_dataset("[short_name]")`
  - Loads metadata from the `.meta.yml` file
  - Runs validation functions on the data
  - Logs results
- Include validation functions appropriate for the dataset:
  - Basic data integrity checks
  - Value range validation
  - Temporal consistency checks
  - Domain-specific validation
  - For OWID datasets: regional and income group aggregation validation

**Read `.claude/docs/validation.md` for complete validation function examples**

### 3. Add the Validation Step to the DAG

- Find the appropriate DAG file in `dag/` directory (search for the main step to find which DAG file it's in)
- Add ONE line for the validation step:
  ```yaml
  data://garden/[namespace]/[version]/[short_name]_validation:
    - data://garden/[namespace]/[version]/[short_name]
  ```
- **Note**: The validation file itself is automatically included in the checksum because of the `[short_name]_validation.py` naming pattern

### 4. Test the Validation

- Run the validation step: `etlr [namespace]/[version]/[short_name]_validation`
- Verify it passes or shows appropriate validation errors
- Test that it re-runs when you change the validation file

## Benefits of This Approach

- **Performance**: Validation only runs when data OR validation code changes, not on unrelated code changes
- **Modularity**: Keeps ETL and validation logic separate and easier to maintain
- **Reusability**: Validation functions can be shared and tested independently
- **Automatic Checksum**: The naming convention ensures validation file is included in checksums automatically

Focus on creating validation functions that are tailored to the specific data type and domain of this ETL step.