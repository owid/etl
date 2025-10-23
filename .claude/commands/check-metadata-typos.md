---
description: Check .meta.yml files for spelling typos using codespell
---

Check `.meta.yml` files for spelling typos using comprehensive spell checking.

**First, ask the user which scope they want to check:**
1. **Current step only** - Check only the `.meta.yml` file(s) in the current working directory/step
2. **All ETL metadata** - Check all active `.meta.yml` files in `etl/steps/data/garden/` (automatically excludes ~1,979 archived steps)

Once the user specifies the scope, proceed with the typo check using the codespell-based approach.

**Note:** Archived steps (defined in `dag/archive/*.yml`) are automatically excluded from checking as they are no longer actively maintained.

---

## Implementation Strategy

### 1. Ensure codespell is installed
Check if codespell is available in the virtual environment. If not, install it:
```bash
.venv/bin/pip show codespell || uv add --dev codespell
```

### 2. Exclude archived steps
**IMPORTANT:** Do not check archived steps as they are no longer in use.

Archived steps are defined in `dag/archive/*.yml` files (~1,979 deprecated steps).

To exclude them, extract their paths and pass to codespell's `--skip` flag:
```bash
# Extract archived garden step paths
ARCHIVED=$(grep -h "data://garden/" dag/archive/*.yml 2>/dev/null |
           grep -o "data://garden/[^:]*" |
           sed 's|data://||' |
           sed 's|$|.meta.yml|' |
           tr '\n' ',' |
           sed 's/,$//')
```

### 3. Run codespell with ignore list and exclusions
Use the existing `.codespell-ignore.txt` file to filter out domain-specific terms:
```bash
# For current directory (option 1)
.venv/bin/codespell . \
  --ignore-words=.codespell-ignore.txt

# For all garden metadata (option 2)
.venv/bin/codespell etl/steps/data/garden/**/*.meta.yml \
  --ignore-words=.codespell-ignore.txt \
  --skip="$ARCHIVED"
```

Note: Excluding archived steps reduces the scope by ~1,979 files and focuses on actively maintained metadata.

### 4. Parse and present results
Extract typos from codespell output and present them in a structured format:
- Group by typo type (e.g., all instances of "seperate" → "separate")
- Show file paths (as clickable links when possible)
- Show line numbers
- Show suggested corrections

**Example output format:**
```
Found 15 typos across 8 files:

Most common:
- "inmigrant" → "immigrant" (5 occurrences in 2 files)
- "seperate" → "separate" (3 occurrences in 1 file)
- "accomodation" → "accommodation" (2 occurrences in 1 file)

Detailed list:
[file.meta.yml:123] inmigrant → immigrant
[file.meta.yml:456] seperate → separate
...
```

### 5. Offer to fix typos
After presenting results, ask the user:
- **Fix all automatically?** - Apply all suggested fixes
- **Review each typo?** - Go through typos one by one for confirmation
- **Cancel** - Exit without making changes

### 6. Apply fixes (if user confirms)
For automatic fixes:
```bash
# Use sed or Python script to replace typos in files
# Example: sed -i '' 's/seperate/separate/g' file.meta.yml
```

For reviewed fixes, confirm each change before applying.

### 7. Verify fixes
After applying fixes, re-run codespell to verify all typos were corrected:
```bash
.venv/bin/codespell <path> --ignore-words=.codespell-ignore.txt
```

Should return 0 results.

### 8. Clean up
**IMPORTANT:** Delete any temporary files created during the check:
- Any `/tmp/` files created for analysis
- Any temporary Python scripts
- Any temporary report files

The only persistent files should be:
- The `.codespell-ignore.txt` whitelist (if it doesn't exist, create it)
- Modified `.meta.yml` files (if fixes were applied)

**Do NOT create new persistent files in the repo like:**
- ❌ `TYPO_CHECK_REPORT.md`
- ❌ `scripts/analyze_typos.py`
- ❌ `scripts/advanced_spell_checker.py`

All analysis logic should be embedded in this command execution, not saved as separate files.

---

## Domain-Specific Terms (Whitelist)

If `.codespell-ignore.txt` doesn't exist, create it with these terms:

```
# Codespell ignore list for OWID ETL
# Domain-specific terms that are NOT typos

# Agriculture and crops
tung
fallow

# Technical abbreviations
fpr
eto
ags
ons
ned
neet
aer
wil
ahd
mis

# Chemical elements and minerals
nd
ore

# Academic/technical terms
theses
demog
ane
ans
oint
mor
sive
fot
belows

# Proper names
reste
```

---

## Error Handling

- If codespell is not installed and installation fails, explain to the user how to install it manually
- If no `.meta.yml` files are found in the specified scope, inform the user
- If codespell finds no typos, congratulate the user on clean metadata!
- If file modification fails, report which files couldn't be updated

---

## Notes

- Always use American English spelling (e.g., "combating" not "combatting")
- Technical field names (like variable names with underscores) are typically safe to ignore
- Acronyms in ALL CAPS that are fewer than 6 characters are likely legitimate
- When in doubt about a flagged word, ask the user before fixing
