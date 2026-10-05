#!/bin/bash
#
#  commit-snapshots.sh
#
#  Sourced by the scripts/update-*.sh jobs. Provides commit_and_push_snapshots, which commits
#  a job's refreshed .dvc files and pushes them to master.
#
#  Usage: commit_and_push_snapshots "<commit message>" <file>...
#

# Runs in a subshell so its strict mode doesn't leak into the calling script.
commit_and_push_snapshots() (
    set -euo pipefail

    local message="$1"
    shift
    local files=("$@")

    git add -- "${files[@]}"
    if git diff --cached --quiet -- "${files[@]}"; then
        echo "No changes in ${files[*]}; nothing to commit."
        exit 0
    fi

    # --no-verify: the pre-commit hook is for people, and it is what made git re-read its
    # temporary index from .git after the hook ran. On 2026-10-03 a deploy deleted that file
    # mid-commit, and git committed an empty tree.
    git commit -q --no-verify -m "$message" -- "${files[@]}"

    # Last check before master and the production ETL run: the commit may only change our files.
    local unexpected
    unexpected=$(comm -23 \
        <(git diff --name-only HEAD~1 HEAD | sort) \
        <(printf '%s\n' "${files[@]}" | sort))
    if [ -n "$unexpected" ]; then
        echo "Refusing to push: the commit changes files this job doesn't own:" >&2
        echo "$unexpected" | sed 's/^/  /' >&2
        git reset -q HEAD~1
        exit 1
    fi

    git push -q origin master
)
