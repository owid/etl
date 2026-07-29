"""Tests for the branching logic of `etl pr`.

The work branch must start from the latest `origin/<base_branch>`, not from a stale local base
branch (https://github.com/owid/etl/issues/6552).
"""

from pathlib import Path

import click
import pytest
from git import Repo

from apps.pr.cli import branch_out, branch_out_worktree


def _commit_file(repo: Repo, name: str, content: str, message: str):
    path = Path(repo.working_tree_dir) / name  # ty: ignore
    path.write_text(content)
    repo.index.add([str(path)])
    return repo.index.commit(message)


def _set_identity(repo: Repo) -> None:
    with repo.config_writer() as config:
        config.set_value("user", "name", "Test User")
        config.set_value("user", "email", "test@example.com")


@pytest.fixture
def stale_local(tmp_path):
    """A clone whose local 'master' is one commit behind 'origin/master' (already fetched)."""
    remote_path = tmp_path / "remote.git"
    Repo.init(remote_path, bare=True, initial_branch="master")

    # Seed the remote with an initial commit on master.
    seed = Repo.init(tmp_path / "seed", initial_branch="master")
    _set_identity(seed)
    _commit_file(seed, "a.txt", "1", "initial commit")
    seed.create_remote("origin", str(remote_path))
    seed.remotes.origin.push("master")

    # The local clone whose master will become stale.
    local = Repo.clone_from(str(remote_path), tmp_path / "local", branch="master")
    _set_identity(local)

    # Advance the remote's master by one commit, and fetch (branch_out assumes init_repo fetched).
    _commit_file(seed, "b.txt", "2", "remote-only commit")
    seed.remotes.origin.push("master")
    local.remotes.origin.fetch()

    assert local.commit("master") != local.commit("origin/master")
    return local


def test_branch_out_starts_from_remote_and_fast_forwards_base(stale_local):
    remote_tip = stale_local.commit("origin/master")
    branch_out(stale_local, "master", "work-branch")
    assert stale_local.active_branch.name == "work-branch"
    assert stale_local.head.commit == remote_tip
    # The local base branch was fast-forwarded along the way.
    assert stale_local.commit("master") == remote_tip
    # No upstream tracking: a plain `git push` on the work branch must not target origin/master.
    assert stale_local.active_branch.tracking_branch() is None


def test_branch_out_excludes_unpushed_local_commits(stale_local):
    # Make the local base branch strictly ahead of the remote (unpushed commit on top of its tip).
    stale_local.git.merge("--ff-only", "origin/master")
    local_tip = _commit_file(stale_local, "local.txt", "3", "unpushed local commit")
    remote_tip = stale_local.commit("origin/master")
    branch_out(stale_local, "master", "work-branch")
    # The work branch starts exactly at the remote tip, without the unpushed commit.
    assert stale_local.head.commit == remote_tip
    # The unpushed commit stays on the local base branch, untouched.
    assert stale_local.commit("master") == local_tip


def test_branch_out_no_update_base_keeps_stale_start_point(stale_local):
    stale_tip = stale_local.commit("master")
    branch_out(stale_local, "master", "work-branch", update_base=False)
    assert stale_local.active_branch.name == "work-branch"
    assert stale_local.head.commit == stale_tip


def test_branch_out_fails_on_diverged_base(stale_local):
    _commit_file(stale_local, "local.txt", "3", "local-only commit")
    with pytest.raises(click.ClickException, match="diverged"):
        branch_out(stale_local, "master", "work-branch")


def test_branch_out_worktree_starts_from_remote(stale_local, tmp_path):
    remote_tip = stale_local.commit("origin/master")
    stale_tip = stale_local.commit("master")
    worktree_path = tmp_path / "worktree"
    branch_out_worktree(stale_local, "master", "work-branch", worktree_path)
    worktree = Repo(worktree_path)
    assert worktree.active_branch.name == "work-branch"
    assert worktree.head.commit == remote_tip
    # The local base branch is left untouched (it may be checked out in another worktree).
    assert stale_local.commit("master") == stale_tip
    # No upstream tracking: a plain `git push` on the work branch must not target origin/master.
    assert worktree.active_branch.tracking_branch() is None


def test_branch_out_worktree_no_update_base_keeps_stale_start_point(stale_local, tmp_path):
    stale_tip = stale_local.commit("master")
    worktree_path = tmp_path / "worktree"
    branch_out_worktree(stale_local, "master", "work-branch", worktree_path, update_base=False)
    assert Repo(worktree_path).head.commit == stale_tip
