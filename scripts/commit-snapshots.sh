#!/bin/bash
#
#  commit-snapshots.sh
#
#  Sourced by the scripts/update-*.sh jobs. Provides commit_and_push_snapshots, which pushes
#  a job's refreshed .dvc files to master as one commit.
#
#  The commit is built with plumbing on top of a fresh origin/master, in a private index, and
#  pushed by hash. It never touches the checkout's own index, HEAD or local master, which
#  `Build ETL` resets on every deploy anyway. That rules out the failures the old
#  `git add; git commit || true; git push || true` had in this shared checkout:
#
#  - An empty commit. `git commit -- <paths>` runs the pre-commit hook and then re-reads its
#    temporary index from .git. On 2026-10-03 a deploy deleted that file mid-commit, and git
#    read it as empty and committed a tree with no files. No hook runs here, and the index is
#    outside .git.
#  - A failed update reported as success. A rejected push used to leave a commit only on local
#    master, and the job still passed. Now the push fails the job, and Buildkite's retry
#    rebuilds the commit on the new origin/master.
#
#  Usage: commit_and_push_snapshots "<commit message>" <file>...
#

# Runs in a subshell so its strict mode and cleanup trap don't leak into the calling script.
commit_and_push_snapshots() (
    set -euo pipefail

    local message="$1"
    shift
    local files=("$@")

    git fetch -q origin master

    local index
    index=$(mktemp)
    trap 'rm -f "$index" "$index.lock"' EXIT

    GIT_INDEX_FILE="$index" git read-tree origin/master
    GIT_INDEX_FILE="$index" git update-index --add -- "${files[@]}"
    local tree
    tree=$(GIT_INDEX_FILE="$index" git write-tree)

    if [ "$tree" = "$(git rev-parse 'origin/master^{tree}')" ]; then
        echo "No changes in ${files[*]}; nothing to commit."
        exit 0
    fi

    local commit
    commit=$(git commit-tree "$tree" -p origin/master -m "$message")

    # The construction above can only change our own files. Check it anyway: this is the last
    # point before a bad commit reaches master and the production ETL run.
    local unexpected
    unexpected=$(comm -23 \
        <(git diff --name-only origin/master "$commit" | sort) \
        <(printf '%s\n' "${files[@]}" | sort))
    if [ -n "$unexpected" ]; then
        echo "Refusing to push $commit: it changes files this job doesn't own:" >&2
        echo "$unexpected" | sed 's/^/  /' >&2
        exit 1
    fi

    git push -q origin "${commit}:refs/heads/master"
    echo "Pushed $commit to master."
)
