---
name: owid-metadata-generation
description: Use when creating or enriching metadata for OWID ETL datasets - generates comprehensive YAML metadata from dataset inspection, data exploration, and web research following OWID metadata standards
---

# OWID Metadata Generation

## Overview

Generate comprehensive, publication-ready metadata for OWID ETL datasets by systematically analyzing data, exploring sources, and following strict OWID metadata standards.

**Core principle:** Metadata must be accurate, complete, and useful for public consumption. Every field must be justified by evidence from the data or sources.

## When to Use

**Always use for:**
- New garden/grapher datasets needing metadata
- Updating existing metadata for dataset refreshes
- Enriching incomplete metadata files
- Creating metadata after data transformations

**Don't use for:**
- Snapshot metadata (different process)
- Quick fixes to single fields (just edit directly)
- Metadata that requires domain expert knowledge beyond what's available

## Metadata Standards

### Required Fields (Garden/Grapher)

**Variable-level (each indicator MUST have):**
- `title`: Short, clear indicator name
- `unit`: Full unit name (plural, lowercase, e.g., "tonnes per hectare")
- `short_unit`: Symbol representation (e.g., "t/ha", "%")
- `description_short`: One-sentence description
- `processing_level`: "minor" or "major" based on transformations
- `license`: Depends on processing_level and origin licenses

**Recommended fields:**
- `description_key`: Bullet points of key information
- `description_from_producer`: Producer's definition if available
- `description_processing`: Processing steps if major transformations
- `presentation.attribution_short`: Short producer citation
- `presentation.topic_tags`: Relevant topic pages (1-3)
- `presentation.title_public`: Public-facing title if needed
- `display.name`: Short legend name for charts
- `display.numDecimalPlaces`: Decimal precision for charts

**Dataset-level:**
- `update_period_days`: Expected update frequency (0, 30, 90, 365)

### Field Guidelines

#### Titles (`title`, `presentation.title_public`, `display.name`)

```yaml
# GOOD
title: Number of neutron star mergers in the Milky Way
presentation:
  title_public: Neutron star mergers in the Milky Way
display:
  name: Neutron star mergers

# BAD - includes metadata in title
title: Number of neutron star mergers (NASA, 2023)

# BAD - too long for display name
display:
  name: Number of neutron star mergers in the Milky Way
```

**Rules:**
- Must start with capital letter
- Must NOT end with period
- Must NOT mention producer, year, or other metadata
- `title`: Can be technical for large datasets (searchable)
- `title_public`: Must be human-readable for public
- `display.name`: Must be very short for chart legends

#### Units (`unit`, `short_unit`)

```yaml
# GOOD
unit: tonnes per hectare
short_unit: t/ha

unit: kilowatts per person
short_unit: kWh/person

unit: "%"
short_unit: "%"

# BAD
unit: tonnes/hectare  # Use "per" not "/"
unit: kilowatts per capita  # Use "person" not "capita"
short_unit: pct  # Use "%" for percentages
```

**Rules:**
- `unit`: Lowercase, plural, use "per" not "/", metric when applicable
  - **Exception**: "Index" is commonly capitalized (either "index" or "Index" acceptable)
- `short_unit`: Follow SI capitalization, no spaces typically, empty if no units
- Empty if dimensionless

**Decimal Precision Guidelines:**
- 0 decimals: Whole numbers (population counts, number of countries)
- 1 decimal: Percentages, rates, simple ratios
- 2 decimals: Most economic indicators, per capita values
- 3 decimals: High-precision indices, when comparing small changes
- **Consistency matters**: Use same precision for related variables in a table

#### Descriptions

**`description_short`** - One sentence:
```yaml
# GOOD
description_short: |-
  The number of people living in extreme poverty, defined as living on less than $2.15 per day.

# BAD - mentions units (redundant)
description_short: |-
  The number of people (in millions) living in extreme poverty.

# BAD - too long, multiple concepts
description_short: |-
  The number of people living in extreme poverty. Extreme poverty is defined using the international poverty line. This has changed over time.
```

**`description_key`** - Bullet points:
```yaml
# GOOD
description_key:
  - Extreme poverty is measured using the International Poverty Line of $2.15 per day in 2017 international dollars.
  - This metric uses household survey data adjusted for purchasing power parity (PPP).
  - The data is produced by the World Bank's Poverty and Inequality Platform (PIP).

# BAD - too technical, no expansion of acronyms
description_key:
  - Uses IPL of $2.15/day (2017 PPP).
  - From WB PIP.
```

**`description_from_producer`** - Direct quote:
```yaml
# GOOD - exact producer text
description_from_producer: |-
  Poverty headcount ratio at $2.15 a day is the percentage of the population living on less than $2.15 a day at 2017 international prices.

# BAD - paraphrased
description_from_producer: |-
  This shows how many people are poor based on the $2.15 threshold.
```

**`description_processing`** - OWID transformations:
```yaml
# GOOD - describes major processing
description_processing: |-
  We calculated regional aggregates by summing country-level data weighted by population.

# BAD - describes common operations
description_processing: |-
  We harmonized country names and removed missing values.
```

**Rules:**
- Start with capital, end with period (all descriptions)
- `description_short`: One short paragraph for subtitles
- `description_key`: Multiple bullet points, expand acronyms, public-friendly
- `description_from_producer`: Only if producer provides clear definition
- `description_processing`: Only for major transformations (required if applicable)

#### Processing Level

```yaml
# MINOR - simple operations only
processing_level: minor
# Allowed: rename entities, multiply by constant, drop missing values

# MAJOR - any other operations
processing_level: major
# Examples: aggregations, per-capita calculations, indicator combinations
```

**Impacts license:**
- `minor` → use most restrictive origin license
- `major` → use "CC BY 4.0"

#### Topic Tags

```yaml
# GOOD - 1-3 relevant topics, most relevant first
presentation:
  topic_tags:
    - Energy
    - Climate Change

# BAD - too many tags
presentation:
  topic_tags:
    - Energy
    - Climate Change
    - Economic Growth
    - Technology
    - Environment
```

**Rules:**
- Must use existing topic tags (check Admin or datasette)
- First tag = primary topic (used in citations)
- 1-3 tags maximum
- Most relevant first

## Workflow

### Step 1: Load and Inspect Dataset

```python
from owid.catalog import Dataset
from etl import paths

# Load the dataset
ds = Dataset(paths.DATA_DIR / "garden/namespace/version/dataset_name")

# Inspect structure
print(f"Tables: {list(ds.table_names)}")
for table_name in ds.table_names:
    tb = ds[table_name]
    print(f"\nTable: {table_name}")
    print(f"Columns: {list(tb.columns)}")
    print(f"Index: {tb.index.names}")
    print(f"\nSample data:")
    print(tb.head())

    # Check value ranges
    for col in tb.select_dtypes(include=['number']).columns:
        print(f"{col}: min={tb[col].min()}, max={tb[col].max()}")
```

**What to extract:**
- Variable names (columns)
- Data ranges and patterns
- Countries/entities present
- Time periods covered
- Existing metadata from origins

### Step 2: Research Sources

**Check dataset origins:**
```python
# Extract origin metadata
for table_name in ds.table_names:
    tb = ds[table_name]
    if hasattr(tb, 'metadata') and hasattr(tb.metadata, 'origins'):
        for origin in tb.metadata.origins:
            print(f"Producer: {origin.producer}")
            print(f"Title: {origin.title}")
            print(f"URL: {origin.url_main}")
            print(f"Description: {origin.description}")
```

**Web research:**
- Visit origin URLs to understand methodology
- Search for "<producer> <dataset> methodology"
- Look for data dictionaries, codebooks, technical documentation
- Find official definitions of indicators
- Check for licensing information

**Document sources for:**
- Indicator definitions (`description_from_producer`)
- Methodology details (`description_key`)
- Acronym expansions
- Appropriate topic tags

### Step 3: Generate Metadata Structure

**Export skeleton:**
```bash
# IMPORTANT: Always use --show or --output to avoid overwriting existing metadata!

# Option 1: Output to file
.venv/bin/etl metadata-export data/garden/namespace/version/dataset_name --output /tmp/dataset_name.meta.yml

# Option 2: Print to stdout (use for inspection)
.venv/bin/etl metadata-export data/garden/namespace/version/dataset_name --show

# NEVER run without --show or --output - it will overwrite the original metadata file!
```

This creates a YAML file with all variable names pre-filled.

### Step 4: Fill Metadata Systematically

**For each variable, determine in order:**

1. **Title** - from column name or origin docs
2. **Units** - inspect data and origin docs
3. **Description short** - one-sentence summary
4. **Description key** - research-backed bullet points
5. **Description from producer** - extract from source if exists
6. **Processing level** - analyze transformations in garden step
7. **Description processing** - document major transformations
8. **Display settings** - determine appropriate decimal places
9. **Topic tags** - match to OWID topic pages

**Use YAML features for efficiency:**

```yaml
definitions:
  common:
    unit: "%"
    short_unit: "%"
    presentation:
      topic_tags:
        - Energy
    display:
      numDecimalPlaces: 1
  notes:
    # Define reusable text snippets
    data_source: |-
      This is based on archival sources and previous research.

tables:
  my_table:
    # Add table-level metadata
    title: Energy Statistics
    description: |-
      Comprehensive energy data covering renewable and fossil fuel sources.

    variables:
      renewable_share:
        title: Share of electricity from renewables
        description_short: |-
          The percentage of electricity generation from renewable sources. {definitions.notes.data_source}
        # Inherits unit, short_unit, topic_tags, numDecimalPlaces from common
```

**For datasets with dimensions (e.g., age groups, sexes):**

```yaml
# Use Jinja templates for dynamic text
macros: |-
  <% macro format_sex(sex) %>
    <%- if sex == "Both" -%>
    all individuals
    <%- elif sex == "Male" -%>
    males
    <%- elif sex == "Female" -%>
    females
    <%- endif -%>
  <% endmacro %>

definitions:
  notes:
    common_note: |-
      Data compiled from multiple historical sources.

tables:
  my_table:
    variables:
      deaths:
        description_short: |-
          The number of deaths from << cause >> among << format_sex(sex) >>. {definitions.notes.common_note}
```

**For repeated grapher notes (e.g., data construction methods):**

```yaml
definitions:
  common:
    presentation:
      grapher_config:
        note: The value for Belgium is constructed from the values of four major cities.

tables:
  my_table:
    variables:
      indicator1:
        # Inherits the grapher note from common
        title: My indicator
```

### Step 5: Validate Metadata

**Check completeness:**
```bash
# Run the grapher step to validate
.venv/bin/etlr data://grapher/namespace/version/dataset_name --grapher --dry-run
```

**Manual checks:**
- [ ] All required fields present
- [ ] No placeholder text ("TODO", "FIXME")
- [ ] Units consistent across related variables
- [ ] Descriptions start with capital, end with period
- [ ] No metadata fields mentioned in titles/descriptions
- [ ] Topic tags exist and spelled correctly
- [ ] Processing level matches actual transformations
- [ ] Decimal places appropriate for data ranges

**Test in Admin:**
```bash
# Upload to staging and preview data pages
ENV_FILE=.env.myname .venv/bin/etlr data://grapher/namespace/version/dataset_name --grapher
# Visit staging-site-myname/admin/datapage-preview/[indicator_id]
```

### Step 6: Iterate Based on Preview

**Common issues:**
- Descriptions too technical → simplify, expand acronyms
- Titles too long for charts → adjust `display.name`
- Wrong decimal precision → adjust `numDecimalPlaces`
- Missing context → expand `description_key`

**Use INSTANT mode for rapid iteration:**
```bash
ENV_FILE=.env.myname INSTANT=1 .venv/bin/etlr data://grapher/namespace/version/dataset_name --grapher --watch
```

## Templates

### Simple Dataset (Few Variables, One Origin)

```yaml
dataset:
  update_period_days: 365

definitions:
  common:
    unit: people
    short_unit: ""
    processing_level: minor
    presentation:
      topic_tags:
        - Global Health
      attribution_short: WHO
    display:
      numDecimalPlaces: 0

tables:
  main:
    variables:
      total_cases:
        title: Total reported cases
        description_short: |-
          The cumulative number of reported cases.
        description_key:
          - Data is collected through national surveillance systems.
          - Cases are laboratory-confirmed using PCR testing.
          - Reporting delays may affect recent data.

      new_cases:
        title: New reported cases
        description_short: |-
          The number of newly reported cases.
        description_key:
          - Calculated as the daily change in total reported cases.
          - May include retrospective adjustments.
```

### Complex Dataset (Multiple Dimensions)

```yaml
dataset:
  update_period_days: 90

definitions:
  common:
    processing_level: major
    license: CC BY 4.0
    presentation:
      topic_tags:
        - Energy
      attribution_short: IEA

macros: |-
  <% macro fuel_description(fuel_type) %>
    <%- if fuel_type == "coal" -%>
    Coal is a fossil fuel formed from ancient plant matter.
    <%- elif fuel_type == "oil" -%>
    Oil is a liquid fossil fuel formed from ancient marine organisms.
    <%- elif fuel_type == "gas" -%>
    Natural gas is a gaseous fossil fuel, primarily methane.
    <%- endif -%>
  <% endmacro %>

tables:
  energy_consumption:
    variables:
      consumption:
        title: << fuel_type.title() >> consumption
        unit: terawatt-hours
        short_unit: TWh
        description_short: |-
          The total primary energy consumption from << fuel_type >>.
        description_key:
          - << fuel_description(fuel_type) >>
          - Primary energy is measured using the substitution method.
          - Data includes both energy and non-energy uses.
        display:
          name: << fuel_type.title() >>
          numDecimalPlaces: 2
```

### Dataset with Multiple Tables

```yaml
dataset:
  update_period_days: 365
  title: Global Disease Burden
  description: |-
    Comprehensive data on disease prevalence, incidence, and mortality across multiple causes.

definitions:
  common:
    presentation:
      topic_tags:
        - Global Health
      attribution_short: IHME

tables:
  prevalence:
    title: Disease prevalence
    variables:
      rate:
        title: Prevalence of << cause >>
        unit: cases per 100,000 people
        short_unit: per 100k
        description_short: |-
          The number of people living with << cause >> per 100,000 population.
        processing_level: major
        license: CC BY 4.0

  mortality:
    title: Disease mortality
    variables:
      deaths:
        title: Deaths from << cause >>
        unit: deaths
        short_unit: ""
        description_short: |-
          The number of deaths attributed to << cause >>.
        processing_level: minor
```

## Using Existing Tools

### etl metadata-upgrade (GPT-assisted)

**When to use:**
- Large datasets with many similar variables
- Initial metadata generation
- Enriching `description_key` fields

**Workflow:**
```bash
# Generate initial metadata with GPT
.venv/bin/etl metadata-upgrade --path-to-file etl/steps/data/garden/namespace/version/dataset.meta.yml --model gpt-4

# Review and refine GPT output
# GPT provides starting point - you must verify and improve
```

**Limitations:**
- Estimates cost before running (can be expensive)
- May hallucinate details not in sources
- Requires manual verification of all fields
- Best for `description_key` and `description_short`

**Best practices:**
- Use for initial draft only
- Always verify against source documentation
- Check that descriptions are public-friendly
- Ensure acronyms are expanded
- Validate units and processing levels manually

### Live Reloading for Rapid Development

```bash
# Set up fast iteration environment
# In .env.myname:
# DEBUG=1
# PREFER_DOWNLOAD=1

# Use INSTANT mode with watch
ENV_FILE=.env.myname INSTANT=1 .venv/bin/etlr data://grapher/namespace/version/dataset --grapher --watch --only

# Edit YAML, save, refresh data page preview immediately
```

## Common Patterns

### Handling "Per Capita" Variables

```yaml
# Original variable
population:
  title: Total population
  processing_level: minor

# Derived per capita variable
gdp_per_capita:
  title: GDP per capita
  unit: international-$ per person
  short_unit: $
  processing_level: major  # Division is major processing
  license: CC BY 4.0
  description_processing: |-
    GDP per capita is calculated by dividing total GDP by population.
```

### Handling Age-Standardized Variables

```yaml
# Use title_variant for disambiguation
mortality_rate:
  title: Mortality rate
  presentation:
    title_variant: Age-standardized
  description_key:
    - Age-standardized rates account for differences in population age structure.
    - This allows fair comparison between countries and over time.
    - Standardization uses the WHO World Standard Population.
```

### Handling Multiple Units (Absolute + Share)

```yaml
renewable_energy_absolute:
  title: Renewable energy generation
  unit: terawatt-hours
  short_unit: TWh
  display:
    name: Renewable energy

renewable_energy_share:
  title: Share of renewable energy
  unit: "%"
  short_unit: "%"
  display:
    name: Renewable share
```

## Quality Checklist

Before considering metadata complete:

- [ ] Every variable has title, unit, short_unit, description_short
- [ ] All descriptions start with capital letter, end with period
- [ ] Units are lowercase, plural (except % and Index)
- [ ] Short units follow SI conventions
- [ ] No metadata fields mentioned in descriptions or titles
- [ ] Processing level accurately reflects transformations
- [ ] License matches processing level
- [ ] Topic tags exist and spelled correctly (check datasette/Admin)
- [ ] Description_key bullet points expand all acronyms
- [ ] Description_key is public-friendly (layperson can understand)
- [ ] Description_from_producer is exact producer text (if used)
- [ ] Description_processing documents major transformations (if applicable)
- [ ] Display.name is short enough for chart legends
- [ ] Decimal places appropriate and consistent for related variables
- [ ] Dataset.update_period_days set correctly
- [ ] Table titles and descriptions provided (if multiple tables or complex dataset)
- [ ] Dynamic YAML used for repeated text ({definitions.notes...})
- [ ] Grapher notes included for data construction methods (if applicable)
- [ ] No TODO or placeholder text remains
- [ ] Metadata previewed in Admin/staging

## Red Flags - Stop and Review

- Copying metadata from other variables without adapting
- Leaving GPT-generated content without verification
- Using technical jargon without explanation
- Skipping web research "to save time"
- Guessing at producer definitions
- Assuming units without checking data
- Not testing metadata in data page preview
- Including producer names in titles/descriptions
- Using "etc." or vague language
- Not expanding acronyms in public-facing fields
- Inconsistent decimal precision across related variables
- Missing table titles/descriptions for complex datasets
- Not using dynamic YAML for repeated text
- Ignoring important methodological notes from source papers

**If you encounter these, pause and do proper research.**

## Domain-Specific Research

When metadata requires deep source understanding:

**Read the source paper/documentation:**
1. Check snapshot metadata for `url_main` links
2. Look for methodology sections, data dictionaries, codebooks
3. Identify key concepts that need explanation in `description_key`
4. Extract important caveats or limitations for grapher notes
5. Find producer's definitions for `description_from_producer`

**Examples of domain-specific notes to capture:**
- Data construction methods (e.g., "Belgium constructed from four major cities")
- Historical events affecting data (e.g., "Paris rent controls until 1948")
- Quality adjustments (e.g., "adjusted for housing amenities")
- Methodological choices (e.g., "uses substitution method for energy")

**When to ask the user:**
- You can't access the source documentation
- Technical details require domain expertise
- Ambiguous data construction methods
- Conflicting information between sources

## Getting Help

**Questions about:**
- Specific field meanings → Check [metadata reference](https://docs.owid.io/projects/etl/architecture/metadata/reference/)
- YAML syntax → Check [structuring YAML guide](https://docs.owid.io/projects/etl/architecture/metadata/structuring-yaml/)
- Topic tags → Query datasette or check Admin
- License determination → Ask in team discussion
- Domain-specific content → Consult domain expert or ask user

**Resources:**
- Metadata reference: `docs/architecture/metadata/reference/index.md`
- Example datasets: Browse `etl/steps/data/garden/` for well-documented examples
- OWID discussions: https://github.com/owid/etl/discussions/categories/metadata

## Final Rule

```
Metadata is for the public, not just us.
Every field should help someone understand the data.
If you wouldn't show it on the website, rewrite it.
```

Metadata is as important as the data itself. Invest the time to do it right.
