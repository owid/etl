# Compare Previous Version Extension

This VSCode extension compares files in date-versioned folders (`YYYY-MM-DD`) with their previous version. It has two parts:

- **Compare Previous Version** (`Cmd+Shift+D`): diff the open file against its previous version.
- **Changes vs Previous Versions** (its own icon in the Activity Bar): review a whole branch, such as a dataset update, file by file against the previous versions, with the noise of every update hidden, and the reviewed files synced with GitHub's "Viewed" state.

## How the previous version is found

For a file like `etl/steps/data/garden/faostat/2026-11-23/faostat_qcl.py`, the extension looks at the other date folders in `etl/steps/data/garden/faostat/`, and takes the most recent one older than `2026-11-23` that has a file with the same name (e.g. `2026-02-25/faostat_qcl.py`).
So each file finds its own previous version, even when the steps of one update come from different older folders.

## Compare Previous Version

1. Open a file that has a date version in its path (e.g., `snapshots/biodiversity/2025-04-07/cherry_blossom.py`).
2. Press `Ctrl+Shift+D` (or `Cmd+Shift+D` on Mac), or use Command Palette > "Compare Previous Version".
3. The extension opens a diff view against the previous version. Press the shortcut again to go back to the file.

## Changes vs Previous Versions

A dataset update copies each step folder to a new version folder, so git (and GitHub) show the new files as entirely new.
This view lists the files changed in the current branch (committed or not) since it left `origin/master`, and puts each in one group:

| Group | What it means | Clicking it opens |
|-------|---------------|-------------------|
| Meaningful changes | A new-version file that differs from its previous version beyond noise. | The diff against the previous version, with the noise lines hidden. Right-click > "Open Diff Including Noise" shows everything. |
| In previous version, missing in new version | A file of an updated step (same name before the first dot) that is in the previous folder but not in the new one, such as a dropped `.countries.json` or `.corrections.yml`. | The old file. |
| New files, with no previous version | A file added in a version folder with no older counterpart. | The file. |
| DAG | The active DAG files (`dag/*.yml`). | The diff against the branch's merge base. |
| Other changed files | Other files without a date folder (scripts, docs), and dated files edited in place. | The diff against the branch's merge base. |
| Only noise changes | Differs from its previous version, but only in noise. | The full diff, to check it. |
| Identical to previous version | Byte-identical copies. | The diff (no changes). |
| DAG archive | `dag/archive/*.yml`, which `etl archive-dag` generates (collapsed). | The diff against the branch's merge base. |

The buttons at the top of the view step to the previous or next file to review (the first five groups), toggle hiding the noise, and rescan.

### Reviewed checkboxes

Each file has a "reviewed" checkbox, and each group has one that ticks or unticks all its files (it shows as ticked when all of them are).

Without a pull request, the checkboxes are kept per workspace in VS Code, and a file unticks itself when its content (or its previous version's) changes.

When the branch has an open pull request, and the GitHub CLI (`gh`) is installed and logged in, the checkboxes are synced with GitHub's "Viewed" state on that pull request, for your own GitHub user:

- Ticking a file marks it as viewed on GitHub, and unticking it unmarks it. Changes are sent in batches (at most 20 files every 4 seconds), so ticking a large group takes a few seconds; the status bar shows the progress.
- Every refresh reads the "Viewed" state of all the pull request's files, and GitHub's state wins: a file viewed or unviewed in the browser shows up here, and so does GitHub's own reset after a new push changes a file ("Viewed" becomes dismissed, which counts as not reviewed).
- If GitHub rejects a change, the file keeps the state you gave it here, a warning offers to retry, and the change is sent again at the next refresh. The message at the top of the view counts the changes that have not reached GitHub yet.
- A file that was ticked here but never reached GitHub (ticked before the pull request existed, or while `gh` was not working) is not unticked silently: the next refresh asks whether to mark those files as viewed on GitHub or to untick them here. Unticks made while GitHub was unreachable are not kept: GitHub's state wins for those.
- Files that are not in the pull request (not pushed yet, or the old files of "In previous version, missing in new version") keep the local state only.
- GitHub only knows the pushed content, so editing a file locally does not untick it while it is viewed on GitHub.

If there is no open pull request, or `gh` is missing or not logged in, the view says so at the top (and once in a notification), and the checkboxes stay local.
`owidVersionDiff.githubSync` turns the sync off, and `owidVersionDiff.ghPath` sets where `gh` is (by default it is looked up on the PATH, then in `/opt/homebrew/bin` and `/usr/local/bin`).

### What counts as noise

A line is hidden only when it and its counterpart become equal after rewriting the parts below; any other difference on the line keeps it visible.
Unchanged lines are never affected.

| Rule (`owidVersionDiff.noiseRules`) | Files | What is rewritten |
|-------------------------------------|-------|-------------------|
| `version-dates` | All | Any old or new version date of the compared files, as a whole token, e.g. `data://garden/faostat/2026-02-25/...` vs `.../2026-11-23/...`. A date glued to other text (`2026-02-20_analysis`) or a date that is not a version of the branch is not touched. |
| `dvc-outs` | `.dvc` | The `md5`, `size` and `nfiles` values under `outs:` (not `path`). |
| `dvc-date-accessed` | `.dvc` | The value of `date_accessed`. |
| `dvc-date-published` | `.dvc` | The value of `date_published`. |
| `dvc-citation-dates` | `.dvc` | Years, and the date right after "accessed", in `attribution` and `citation_full` (one-line or block values). |

To see a kind of change that is hidden by default (for example, to check every `date_published`), remove its rule from `owidVersionDiff.noiseRules`.
`owidVersionDiff.baseRef` (default `origin/master`) sets what the branch is compared with.

### Command line

The same classification runs in a terminal, which is handy outside VS Code or for an agent reviewing a PR (`dist/` is built by `make vsce-compile EXT=compare-previous-version`, and also ships inside the installed extension, in `~/.vscode/extensions/owid.compare-previous-version-<version>/dist/cli.js`):

```bash
node vscode_extensions/compare-previous-version/dist/cli.js [repo dir] [--base origin/master] [--hunks] [--noise] [--all] [--json]
```

`--hunks` prints the meaningful hunks, `--noise` the line pairs treated as noise, `--all` also lists identical files, and `--json` everything as JSON.

## Installation

From the repository root, `make vsce-compile EXT=compare-previous-version INSTALL=1` (or `make vsce-sync`), then reload the window.
