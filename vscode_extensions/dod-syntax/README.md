# DoD Syntax

A VS Code extension for syntax highlighting of Definition of Data (DoD) references in YAML and Python files with intelligent pre-loading and batch database fetching.

## Features

- **Syntax Highlighting**: Automatically highlights text blocks with the pattern `[title](#dod:key)` with an underline
- **Intelligent Autocomplete**: Type `#dod` or `#dod:` to get a dropdown of all available DoD names with filtering
- **Multi-Language Support**: Works in YAML files and Python strings (both raw and regular)  
- **Intelligent Pre-loading**: Scans files on open and pre-fetches all DoD definitions for immediate hover access
- **Batch Database Fetching**: Efficiently fetches multiple DoD definitions in a single database query
- **Smart Caching**: Caches database results for 5 minutes to improve performance (configurable)
- **Hover Details**: Shows detailed information including definition content, last updated by, and update date
- **Blue Color**: Uses VS Code's standard blue color (`#0078d4`) for consistency

## How It Works

1. **DoD Names Pre-loading**: On activation, loads all available DoD names from database for autocomplete
2. **File Scanning**: When you open a YAML or Python file, automatically scans for all DoD references
3. **Batch Pre-loading**: All unique DoD keys are fetched from the database in a single batch request
4. **Immediate Hover**: Hover over any DoD reference to see the definition instantly (no loading time!)
5. **Smart Autocomplete**: Type `#dod` to trigger intelligent completion with filtering
6. **Smart Caching**: Results are cached to avoid redundant database calls

## Usage

The extension automatically activates when you open YAML or Python files. It provides:

### 🔍 Syntax Highlighting
1. Scans files for DoD patterns matching `[title](#dod:key)` format
2. Pre-loads all DoD definitions from the database
3. Applies underline styling to DoD references  
4. Shows instant hover information with full definitions

### ⚡ Intelligent Autocomplete
1. Type `#dod` to trigger autocomplete dropdown
2. Type `#dod:` followed by text to filter available DoDs
3. Select a DoD from the dropdown to insert `[Title](#dod:name)` format
4. DoD names are automatically converted to readable titles

### YAML Files
```yaml
description_short: The [Gini index](#dod:gini) measures inequality on a scale from 0 to 100.
footnote: This metric is [age-standardized](#dod:age_standardized) for comparability.
```

### Python Files
DoD references inside strings (both raw and regular) are highlighted:
```python
# Raw string - highlighted
description = r"The [Gini index](#dod:gini) measures inequality."

# Regular string - highlighted  
description = "The [Gini index](#dod:gini) measures inequality."

# Not in string - ignored
# [Gini index](#dod:gini) - this comment is ignored
```

## 🚀 Autocomplete Examples

### Basic Usage
1. **Type `#dod`** → Dropdown appears with all DoD names
2. **Type `#dod:age`** → Filtered to DoDs containing "age" (e.g., `age_standardized`)
3. **Select from dropdown** → Inserts `[Age Standardized](#dod:age_standardized)`

### Smart Filtering
- `#dod:gini` → Shows `gini` DoD
- `#dod:standard` → Shows `age_standardized`, `age-specific-fertility-rate`, etc.
- `#dod:covid` → Shows all COVID-related DoDs

### Title Generation
DoD names are automatically converted to readable titles:
- `age_standardized` → `Age Standardized`
- `gross-domestic-product` → `Gross Domestic Product` 
- `hiv_aids` → `Hiv Aids`

## Configuration

The extension includes a top-level configuration variable:

```typescript
const ENABLE_CACHE = true;  // Set to false to disable caching and always fetch fresh DoDs
```

## Directory Structure

```
dod-syntax/
├── src/                 # TypeScript source code
│   ├── extension.ts     # Main extension logic
│   └── test/           # Extension tests
├── scripts/            # Python backend scripts  
│   └── fetch_dod.py   # Batch DoD fetching script
├── tests/             # Test files and debug scripts
├── install/           # Built extension packages (.vsix)
├── dist/              # Compiled JavaScript  
└── out/               # Test compilation output
```

## Development

### Building
```bash
npm run package         # Build the extension
npm run compile        # Compile TypeScript
npm run watch          # Watch for changes
```

### Packaging  
```bash
npx vsce package --out install/dod-syntax-x.x.x.vsix
```

## Database Integration

The extension connects to the OWID production database to fetch DOD definitions:

- **Batch Processing**: Multiple DoDs fetched in single SQL query using `IN` clause
- **Production Database**: Always connects to production for consistency
- **Error Handling**: Graceful fallback when database is unavailable
- **Caching**: 5-minute cache prevents repeated database calls

## Performance Optimizations

1. **Batch Fetching**: Single database query for all DoDs in a file
2. **Pre-loading**: Definitions loaded when file opens, not on hover
3. **Smart Caching**: Only fetch uncached DoDs
4. **Virtual Environment**: Uses project's Python environment for database access