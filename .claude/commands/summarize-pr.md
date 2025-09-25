# Summarize PR Changes After Step Duplication

**Usage:** `/summarize-pr <pr_url>`

**Description:** Analyzes a GitHub PR to find the commit range after "Duplicate old steps" and provides a summary of the total changes across that range, not individual commits.

**Example:** `/summarize-pr https://github.com/owid/etl/pull/4729`

## What this command does:

1. **Fetches PR commits** - Gets all commits from the PR to identify the duplication point
2. **Finds duplication point** - Identifies the "Duplicate old steps" commit
3. **Analyzes commit range** - Uses GitHub's compare API to get the total diff between duplication point and final commit
4. **Provides readable diff link** - Generates a GitHub PR files view URL for the commit range

## Key Requirements:

- **Analyze the RANGE, not individual commits** - Use GitHub's compare API to get the total diff from duplication commit to final commit
- **Use correct API format** - Use shorter commit hashes (7-8 chars) and three dots: `gh api repos/owid/etl/compare/3a34eaf3d...6ee8c4666` (NOT full 40-char hashes or two dots)
- **Focus on net changes** - What files were modified and what the overall impact was
- **Provide cherry-pick command** - Give the exact git command to apply these changes

## Output should include:

- Commit range being analyzed (from duplication point to end)
- Total files modified with net change summary
- Clean GitHub PR files URL for reviewing changes (e.g., `https://github.com/owid/etl/pull/4729/files/3a34eaf3dc536af62aa29f0993c4648cdf84018c..6ee8c4666adb33318938f3e21067c055cec05fd6`)
- Git command to cherry-pick the commit range
- Recommendation on safety of applying changes

## Use cases:

- Reviewing ETL dataset updates to understand what changed after initial duplication
- Deciding which specific commits to cherry-pick from a squashed PR
- Getting a clean view of post-duplication changes without the noise of the initial setup