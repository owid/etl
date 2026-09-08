---
name: check-metadata-typos
description: Check .meta.yml and snapshot .dvc files for spelling typos using codespell. Use when user mentions typos, spelling errors, metadata quality, or wants to check metadata files for mistakes.
metadata:
  internal: true
---

# Check Metadata Typos

Check metadata files for spelling typos using comprehensive spell checking.

## Scope Options

Ask the user which scope they want to check:

1. **Current step only** - Ask the user to specify the step path (e.g., `etl/steps/data/garden/energy/2025-06-27/electricity_mix`)
2. **All ETL metadata** - Check all active `.meta.yml` files in `etl/steps/data/{garden,meadow,grapher}/` (automatically excludes ~3,570 archived steps)
3. **Snapshot metadata** - Check all snapshot `.dvc` files in `snapshots/` (~7,915 files)
4. **All metadata** - Check both ETL steps and snapshot metadata files

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
- ~3,570 deprecated steps (garden, meadow, grapher)
- ~736 deprecated snapshots

To exclude them, extract their paths and create a list of active files:

```bash
# Extract archived step paths to a file
for step_type in garden meadow grapher; do
  grep -h "data://${step_type}/" dag/archive/*.yml 2>/dev/null | \
    grep -o "data://${step_type}/[^:]*" | \
    sed 's|data://|etl/steps/data/|' | \
    sed 's|$|.meta.yml|'
done > /tmp/archived_files.txt

# Extract archived snapshots
grep -rh "snapshot://" dag/archive/*.yml 2>/dev/null | \
  grep -o "snapshot://[^:]*" | \
  sed 's|snapshot://|snapshots/|' | \
  sed 's|$|.dvc|' | \
  sort -u >> /tmp/archived_files.txt

# Create list of all metadata files
find etl/steps/data/garden -name "*.meta.yml" > /tmp/all_meta_files.txt
find etl/steps/data/meadow -name "*.meta.yml" >> /tmp/all_meta_files.txt
find etl/steps/data/grapher -name "*.meta.yml" >> /tmp/all_meta_files.txt
find snapshots -name "*.dvc" >> /tmp/all_meta_files.txt

# Filter out archived files
grep -vFf /tmp/archived_files.txt /tmp/all_meta_files.txt > /tmp/active_meta_files.txt

echo "Total files to check: $(wc -l < /tmp/active_meta_files.txt)"
```

### 2. Run codespell with ignore list and exclusions

Use the existing `.codespell-ignore.txt` file to filter out domain-specific terms.

**Whichever option the user picked, write that scope's file list to `/tmp/codespell_targets.txt` and check it.** Steps 5 and 6 reuse that one file, so the fix and the verification cannot act on a wider scope than the check did. Never point a later step at a different list.

**For option 1 (current step only):**

Ask the user for the path, then normalize it. This scope has to accept three shapes, because `/create-snapshot` step 5 calls it on a single new snapshot file:

- an ETL step directory (`etl/steps/data/garden/energy/2025-06-27/electricity_mix`) → every `.meta.yml` in it
- a snapshot stem with no extension (`snapshots/who/2026-04-22/mortality`) → the matching `.dvc`
- a single file, `.meta.yml` or `.dvc` → itself

```bash
# For specific step or single file (option 1)
TARGET="<user_provided_path>"

.venv/bin/python - "$TARGET" > /tmp/codespell_targets.txt <<'PY'
import glob, os, sys

target = sys.argv[1].rstrip("/")
if os.path.isdir(target):
    paths = sorted(glob.glob(f"{target}/*.meta.yml")) + sorted(glob.glob(f"{target}/*.dvc"))
elif os.path.isfile(target):
    paths = [target]
else:
    # A snapshot stem: snapshots/<namespace>/<version>/<short_name>, extension unknown.
    paths = sorted(glob.glob(f"{target}.*.dvc")) or sorted(glob.glob(f"{target}.dvc"))
print("\n".join(paths))
PY

cat /tmp/codespell_targets.txt | xargs .venv/bin/codespell \
  --ignore-words=.codespell-ignore.txt
```

Two ways this scope silently checks nothing, both of which must be treated as an error rather than a clean result:

- **A `.dvc` target matched by a `*.meta.yml`-only pattern.** A snapshot path has no `.meta.yml` in it, so a `.meta.yml`-only search returns an empty list and codespell reports no typos on a file it never opened. The normalizer above exists for exactly this case.
- **An empty list from a shell glob.** Don't build the list with `ls <path>/*.meta.yml`: under zsh a glob matching nothing aborts the command before the redirect, so no file is written and a wider list left over from an earlier run gets reused.

**Always report the number of files in `/tmp/codespell_targets.txt` before running codespell.** If it is 0, say the path matched nothing and ask for a corrected one — never fall through to a wider scope, and never report "no typos found".

**For option 2 (all ETL metadata - garden, meadow, grapher):**

```bash
# For all ETL step metadata (option 2)
find etl/steps/data/garden -name "*.meta.yml" > /tmp/all_step_files.txt
find etl/steps/data/meadow -name "*.meta.yml" >> /tmp/all_step_files.txt
find etl/steps/data/grapher -name "*.meta.yml" >> /tmp/all_step_files.txt
grep -vFf /tmp/archived_files.txt /tmp/all_step_files.txt > /tmp/codespell_targets.txt

cat /tmp/codespell_targets.txt | xargs .venv/bin/codespell \
  --ignore-words=.codespell-ignore.txt
```

Note: Excluding archived steps reduces the scope by ~3,570 files and focuses on actively maintained metadata.

**For option 3 (snapshot metadata):**

```bash
# For all snapshot metadata (option 3)
find snapshots -name "*.dvc" > /tmp/all_snapshot_files.txt
grep -vFf /tmp/archived_files.txt /tmp/all_snapshot_files.txt > /tmp/codespell_targets.txt

cat /tmp/codespell_targets.txt | xargs .venv/bin/codespell \
  --ignore-words=.codespell-ignore.txt
```

Note: the prose worth checking in a snapshot `.dvc` lives under `meta.origin` — `description`, `description_snapshot`, `title`, `title_snapshot`, `citation_full`, `attribution`. Older files instead use the deprecated `meta.source.description` / `meta.source.published_by`; both shapes are still in the repo (~2,000 files on `origin`, ~6,000 on `source`). codespell reads the whole file either way, so no filtering is needed — but report hits by their real field path, and don't "fix" a typo by migrating a file from `source` to `origin` (that's a separate change, out of scope here). ~736 archived snapshots are excluded.

**For option 4 (all metadata):**

```bash
# For all metadata - ETL and snapshots (option 4)
# Use the active_meta_files.txt created in step 1
cp /tmp/active_meta_files.txt /tmp/codespell_targets.txt

cat /tmp/codespell_targets.txt | xargs .venv/bin/codespell \
  --ignore-words=.codespell-ignore.txt
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

For automatic fixes, let codespell apply its own corrections — it rewrites only the words it flagged, at the positions it flagged them. Run it over `/tmp/codespell_targets.txt`, the list step 2 wrote for the scope the user chose:

```bash
cat /tmp/codespell_targets.txt | xargs .venv/bin/codespell \
  --ignore-words=.codespell-ignore.txt --write-changes
```

**Never widen the scope here.** `--write-changes` rewrites every file it is given, so pointing this at `/tmp/active_meta_files.txt` (the option-4 list) after the user asked for one step would silently edit thousands of unrelated files. The list must be the one that produced the reported typos — if `/tmp/codespell_targets.txt` is missing or you are unsure it matches, re-run step 2 for the chosen option before fixing anything.

For reviewed fixes, apply each one with the `Edit` tool, matching enough surrounding words to be unambiguous.

**Never fix with an unanchored `sed` substitution.** `sed -i 's/word/word/g' file` rewrites *every* occurrence in the file, including the ones you decided to ignore — inside URLs, domain names, and ALL-CAPS acronyms (see Notes). It also silently rewrites a word codespell never flagged on that line. `sed -i ''` is BSD-only on top of that, so it breaks on Linux (CI, cloud sandbox) where the same command needs `sed -i`.

### 6. Verify fixes

After applying fixes, re-run codespell over the same scope to verify all typos were corrected:

```bash
cat /tmp/codespell_targets.txt | xargs .venv/bin/codespell \
  --ignore-words=.codespell-ignore.txt
```

Should return 0 results. If the user chose to review typos one by one and deliberately skipped some, expect exactly those to remain — say which, rather than reporting the check as failed.

### 7. Clean up

**IMPORTANT:** Delete any temporary files created during the check:

```bash
rm -f /tmp/archived_files.txt /tmp/all_meta_files.txt /tmp/active_meta_files.txt \
      /tmp/all_step_files.txt /tmp/active_step_files.txt \
      /tmp/all_snapshot_files.txt /tmp/active_snapshot_files.txt \
      /tmp/codespell_output.txt

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
