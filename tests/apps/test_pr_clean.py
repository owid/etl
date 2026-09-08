"""Tests for the worktree-cleaning logic of `etl pr-clean`.

A dirty worktree must be skipped before anything is copied or modified: a failed
`git worktree remove` after the salvage copies would leave the worktree stripped of its
--share-data symlinks, and a later re-run would duplicate the salvaged workbench/ai dirs
under suffixed names.
"""

import shutil
from pathlib import Path

import pytest
from git import Repo

from apps.pr.cli import clean_branch, worktree_blocking_changes


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
def repo_with_worktree(tmp_path):
    """A main repo plus a worktree checked out on branch 'feature'.

    The .gitignore uses directory patterns (trailing slash), which match real dirs but NOT
    symlinks — so a real workbench/ dir is ignored (and never blocks `git worktree remove`),
    while a --share-data style symlink shows up as untracked, exercising the filter in
    worktree_blocking_changes.
    """
    repo = Repo.init(tmp_path / "main", initial_branch="master")
    _set_identity(repo)
    gitignore = Path(repo.working_tree_dir) / ".gitignore"  # ty: ignore
    gitignore.write_text("data/\nworkbench/\nai/\n")
    repo.index.add([str(gitignore)])
    _commit_file(repo, "a.txt", "1", "initial commit")
    repo.git.branch("feature")
    worktree_path = tmp_path / "wt"
    repo.git.worktree("add", str(worktree_path), "feature")
    return repo, worktree_path


def _add_share_data_symlinks(worktree_path: Path, target: Path) -> None:
    target.mkdir(exist_ok=True)
    for name in ("data", "workbench", "ai"):
        (worktree_path / name).symlink_to(target)


def test_blocking_changes_clean_worktree(repo_with_worktree):
    _, worktree_path = repo_with_worktree
    assert worktree_blocking_changes(worktree_path) == []


def test_blocking_changes_modified_tracked_file(repo_with_worktree):
    _, worktree_path = repo_with_worktree
    (worktree_path / "a.txt").write_text("edited")
    blocking = worktree_blocking_changes(worktree_path)
    assert len(blocking) == 1
    assert "a.txt" in blocking[0]


def test_blocking_changes_untracked_file(repo_with_worktree):
    _, worktree_path = repo_with_worktree
    (worktree_path / "untracked.txt").write_text("new")
    blocking = worktree_blocking_changes(worktree_path)
    assert len(blocking) == 1
    assert "untracked.txt" in blocking[0]


def test_blocking_changes_excludes_share_data_symlinks(repo_with_worktree, tmp_path):
    _, worktree_path = repo_with_worktree
    _add_share_data_symlinks(worktree_path, tmp_path / "shared")
    # The symlinks are untracked (the dir-only ignore patterns don't match them), yet excluded.
    assert "?? data" in Repo(worktree_path).git.status("--porcelain")
    assert worktree_blocking_changes(worktree_path) == []
    # A real untracked file alongside them still counts.
    (worktree_path / "untracked.txt").write_text("new")
    blocking = worktree_blocking_changes(worktree_path)
    assert len(blocking) == 1
    assert "untracked.txt" in blocking[0]


def test_blocking_changes_missing_worktree_dir(repo_with_worktree):
    """A registered worktree whose directory was manually deleted must not crash the pre-scan."""
    _, worktree_path = repo_with_worktree
    shutil.rmtree(worktree_path)
    assert worktree_blocking_changes(worktree_path) == []


def test_blocking_changes_unreadable_worktree_counts_as_blocking(repo_with_worktree):
    _, worktree_path = repo_with_worktree
    (worktree_path / ".git").write_text("gitdir: /nonexistent/gitdir")
    blocking = worktree_blocking_changes(worktree_path)
    assert len(blocking) == 1
    assert "unreadable worktree" in blocking[0]


def test_clean_branch_cleans_manually_deleted_worktree(repo_with_worktree, tmp_path):
    """`git worktree remove` on a prunable (deleted) worktree succeeds, so cleaning completes."""
    repo, worktree_path = repo_with_worktree
    main_path = Path(repo.working_tree_dir).resolve()  # ty: ignore
    shutil.rmtree(worktree_path)

    clean_branch(
        repo=repo,
        branch="feature",
        worktree_path=worktree_path,
        main_project_dir=tmp_path / "claude_projects",
        main_worktree_path=main_path,
    )

    assert "feature" not in [b.name for b in repo.branches]
    assert str(worktree_path) not in repo.git.worktree("list", "--porcelain")


def test_clean_branch_skips_dirty_worktree_without_mutating(repo_with_worktree, tmp_path):
    repo, worktree_path = repo_with_worktree
    main_path = Path(repo.working_tree_dir).resolve()  # ty: ignore
    _add_share_data_symlinks(worktree_path, tmp_path / "shared")
    workbench = worktree_path / "workbench_real"
    (worktree_path / "a.txt").write_text("uncommitted edit")

    clean_branch(
        repo=repo,
        branch="feature",
        worktree_path=worktree_path,
        main_project_dir=tmp_path / "claude_projects",
        main_worktree_path=main_path,
    )

    # Nothing was touched: worktree and branch still exist, symlinks intact, nothing salvaged.
    assert worktree_path.exists()
    assert "feature" in [b.name for b in repo.branches]
    assert (worktree_path / "data").is_symlink()
    assert not (main_path / "workbench" / "feature").exists()
    assert not workbench.exists()  # sanity: the dirty test never created a real workbench


def test_clean_branch_removes_clean_worktree_and_salvages_scratch_dirs(repo_with_worktree, tmp_path):
    repo, worktree_path = repo_with_worktree
    main_path = Path(repo.working_tree_dir).resolve()  # ty: ignore
    (worktree_path / "data").symlink_to(tmp_path / "shared")
    workbench = worktree_path / "workbench"
    workbench.mkdir()
    (workbench / "notes.txt").write_text("keep me")

    clean_branch(
        repo=repo,
        branch="feature",
        worktree_path=worktree_path,
        main_project_dir=tmp_path / "claude_projects",
        main_worktree_path=main_path,
    )

    assert not worktree_path.exists()
    assert "feature" not in [b.name for b in repo.branches]
    assert (main_path / "workbench" / "feature" / "notes.txt").read_text() == "keep me"
