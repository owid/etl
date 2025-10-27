---
description: Check .meta.yml and snapshot .dvc files for spelling typos using codespell
---

Check metadata files for spelling typos using comprehensive spell checking.

**First, ask the user which scope they want to check:**

1. **Current step only** - Ask the user to specify the step path (e.g., `etl/steps/data/garden/energy/2025-06-27/electricity_mix`)
2. **All ETL metadata** - Check all active `.meta.yml` files in `etl/steps/data/garden/` (automatically excludes ~1,979 archived steps)
3. **Snapshot metadata** - Check all snapshot `.dvc` files in `snapshots/` (~7,913 files)
4. **All metadata** - Check both ETL and snapshot metadata files

Once the user specifies the scope, proceed with the typo check using the codespell-based approach.

**Note:** Archived steps and snapshots (defined in `dag/archive/*.yml`) are automatically excluded from checking as they are no longer actively maintained.

---

## Implementation Strategy

### 0. Check codespell installation

**IMPORTANT:** Check if codespell is installed before attempting to use it. Since codespell is now a dev dependency in the project, it should already be installed, but verify first to avoid reinstalling unnecessarily.

```bash
# Check if codespell is installed
if ! .venv/bin/codespell --version &> /dev/null; then
    echo "codespell not found, installing..."
    uv add --dev codespell
else
    echo "codespell is already installed"
fi
```

If codespell is not installed and `uv add --dev codespell` fails, explain to the user how to install it manually.

### 1. Exclude archived steps and snapshots

**IMPORTANT:** Do not check archived steps and snapshots as they are no longer in use.

Archived steps and snapshots are defined in `dag/archive/*.yml` files:
- ~1,979 deprecated garden steps
- ~736 deprecated snapshots

To exclude them, extract their paths and pass to codespell's `--skip` flag:

```bash
# Extract archived garden step paths
ARCHIVED_STEPS=$(grep -h "data://garden/" dag/archive/*.yml 2>/dev/null |
           grep -o "data://garden/[^:]*" |
           sed 's|data://||' |
           sed 's|$|.meta.yml|' |
           tr '\n' ',' |
           sed 's/,$//')

# Extract archived snapshot paths
ARCHIVED_SNAPSHOTS=$(grep -rh "snapshot://" dag/archive/*.yml 2>/dev/null |
           grep -o "snapshot://[^:]*" |
           sed 's|snapshot://|snapshots/|' |
           sed 's|$|.dvc|' |
           sort -u |
           tr '\n' ',' |
           sed 's/,$//')

# Combine both for use in --skip flag
ARCHIVED="${ARCHIVED_STEPS},${ARCHIVED_SNAPSHOTS}"
```

### 2. Run codespell with ignore list and exclusions

Use the existing `.codespell-ignore.txt` file to filter out domain-specific terms:

**For option 1 (current step only):**

1. Ask the user to provide the step path (e.g., `etl/steps/data/garden/energy/2025-06-27/electricity_mix`)
2. Construct the full path to the metadata file: `<step_path>/*.meta.yml`
3. Run codespell on that specific path:

```bash
# For specific step (option 1)
STEP_PATH="<user_provided_path>"  # e.g., etl/steps/data/garden/energy/2025-06-27/electricity_mix
.venv/bin/codespell "${STEP_PATH}"/*.meta.yml \
  --ignore-words=.codespell-ignore.txt
```

**For option 2 (all garden metadata):**

```bash
# For all garden metadata (option 2)
.venv/bin/codespell etl/steps/data/garden/**/*.meta.yml \
  --ignore-words=.codespell-ignore.txt \
  --skip="$ARCHIVED"
```

Note: Excluding archived steps reduces the scope by ~1,979 files and focuses on actively maintained metadata. Archived snapshots are also excluded when checking snapshot metadata.

**For option 3 (snapshot metadata):**

```bash
# For all snapshot metadata (option 3)
# Extract archived snapshots
ARCHIVED_SNAPSHOTS=$(grep -rh "snapshot://" dag/archive/*.yml 2>/dev/null |
           grep -o "snapshot://[^:]*" |
           sed 's|snapshot://|snapshots/|' |
           sed 's|$|.dvc|' |
           sort -u |
           tr '\n' ',' |
           sed 's/,$//')

.venv/bin/codespell snapshots/**/*.dvc \
  --ignore-words=.codespell-ignore.txt \
  --skip="$ARCHIVED_SNAPSHOTS"
```

Note: Snapshot `.dvc` files contain metadata in the `meta.source.description` and `meta.source.published_by` fields. ~736 archived snapshots are excluded.

**For option 4 (all metadata):**

```bash
# For all metadata - ETL and snapshots (option 4)
.venv/bin/codespell etl/steps/data/garden/**/*.meta.yml snapshots/**/*.dvc \
  --ignore-words=.codespell-ignore.txt \
  --skip="$ARCHIVED"
```

### 3. Parse and present results

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

### 4. Offer to fix typos

After presenting results, ask the user:

- **Fix all automatically?** - Apply all suggested fixes
- **Review each typo?** - Go through typos one by one for confirmation
- **Cancel** - Exit without making changes

### 5. Apply fixes (if user confirms)

For automatic fixes:

```bash
# Use sed or Python script to replace typos in files
# Example: sed -i '' 's/seperate/separate/g' file.meta.yml
```

For reviewed fixes, confirm each change before applying.

### 6. Verify fixes

After applying fixes, re-run codespell to verify all typos were corrected:

```bash
.venv/bin/codespell <path> --ignore-words=.codespell-ignore.txt
```

Should return 0 results.

### 7. Clean up

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

## Error Handling

- Check if codespell is installed first (see step 0). If not installed and `uv add --dev codespell` fails, explain to the user how to install it manually with `uv sync` or check their Python environment
- If no `.meta.yml` or `.dvc` files are found in the specified scope, inform the user
- If codespell finds no typos, congratulate the user on clean metadata!
- If file modification fails, report which files couldn't be updated

---

## Notes

- Always use American English spelling (e.g., "combating" not "combatting")
- Technical field names (like variable names with underscores) are typically safe to ignore
- **Acronyms in ALL CAPS should be ignored** - they are almost always legitimate acronyms (e.g., TE, INE, DIEA)
- **URLs and domain names should be ignored** - codespell may flag parts of URLs (e.g., "ine.es", "corona.fo") but these are correct
- When in doubt about a flagged word, ask the user before fixing
