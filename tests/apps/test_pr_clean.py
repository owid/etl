"""Tests for the worktree-cleaning logic of `etl pr-clean`.

A dirty worktree must be skipped before anything is copied or modified: a failed
`git worktree remove` after the salvage copies would leave the worktree stripped of its
--share-data symlinks, and a later re-run would duplicate the salvaged workbench/ai dirs
under suffixed names.
"""

import re
import shutil
from pathlib import Path

import pytest
from git import GitCommandError, Repo

from apps.pr.cli import (
    _CLEAN_ALL,
    _CLEAN_ALL_CLOSED,
    _CLEAN_ALL_MERGED,
    clean_branch,
    fetch_pr_state,
    format_clean_plan,
    plan_clean,
    remove_worktree,
    resolve_branches_to_clean,
    worktree_blocking_changes,
)


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


def test_resolve_branches_to_clean():
    cleanable = {"merged-a": "merged", "merged-b": "merged", "closed-a": "closed"}
    assert resolve_branches_to_clean(_CLEAN_ALL, cleanable) == ["closed-a", "merged-a", "merged-b"]
    assert resolve_branches_to_clean(_CLEAN_ALL_MERGED, cleanable) == ["merged-a", "merged-b"]
    assert resolve_branches_to_clean(_CLEAN_ALL_CLOSED, cleanable) == ["closed-a"]
    assert resolve_branches_to_clean("merged-a", cleanable) == ["merged-a"]


def test_resolve_branches_to_clean_empty_bucket():
    cleanable = {"merged-a": "merged"}
    assert resolve_branches_to_clean(_CLEAN_ALL_CLOSED, cleanable) == []


def test_resolve_branches_to_clean_no_sentinel_collision():
    """A real branch literally named like a sentinel marker must be treated as that one branch."""
    cleanable = {"__all_merged__": "closed", "other-merged": "merged"}
    assert resolve_branches_to_clean("__all_merged__", cleanable) == ["__all_merged__"]


class _FakeResponse:
    def __init__(self, status_code: int, prs: list | None = None):
        self.status_code = status_code
        self._prs = prs or []

    def json(self):
        return self._prs


def _fake_github(monkeypatch, by_name: _FakeResponse, by_commit: _FakeResponse | None = None) -> list[str]:
    """Answer the PR lookup by branch name with `by_name`, and by commit with `by_commit`; return the URLs asked."""
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if "/commits/" in url:
            assert by_commit is not None, "The lookup by commit should not have been made."
            return by_commit
        return by_name

    monkeypatch.setattr("apps.pr.cli.requests.get", fake_get)
    return calls


MERGED_PR = {"state": "closed", "merged_at": "2026-11-10T00:00:00Z"}
CLOSED_PR = {"state": "closed", "merged_at": None}
OPEN_PR = {"state": "open", "merged_at": None}


def test_fetch_pr_state_by_branch_name(monkeypatch):
    calls = _fake_github(monkeypatch, by_name=_FakeResponse(200, [MERGED_PR]))
    assert fetch_pr_state("feature", tip_sha="abc") == "merged"
    assert len(calls) == 1


def test_fetch_pr_state_by_last_commit_when_no_pr_has_the_name(monkeypatch):
    """A branch with no PR of its own, whose last commit is part of a merged PR, counts as merged."""
    _fake_github(monkeypatch, by_name=_FakeResponse(200, []), by_commit=_FakeResponse(200, [MERGED_PR]))
    assert fetch_pr_state("tmp-copy", tip_sha="abc") == "merged"


def test_fetch_pr_state_by_last_commit_keeps_branches_of_open_or_closed_prs(monkeypatch):
    _fake_github(monkeypatch, by_name=_FakeResponse(200, []), by_commit=_FakeResponse(200, [OPEN_PR]))
    assert fetch_pr_state("tmp-copy", tip_sha="abc") == "open"
    # A last commit that is only in unmerged PRs is not enough to call the branch cleanable.
    _fake_github(monkeypatch, by_name=_FakeResponse(200, []), by_commit=_FakeResponse(200, [CLOSED_PR]))
    assert fetch_pr_state("tmp-copy", tip_sha="abc") is None


def test_fetch_pr_state_unpushed_last_commit(monkeypatch):
    """GitHub answers 422 for a commit it doesn't have, which means no PR contains it."""
    _fake_github(monkeypatch, by_name=_FakeResponse(200, []), by_commit=_FakeResponse(422))
    assert fetch_pr_state("local-only", tip_sha="abc") is None


def test_fetch_pr_state_failed_name_lookup_skips_commit_lookup(monkeypatch):
    _fake_github(monkeypatch, by_name=_FakeResponse(500))
    assert fetch_pr_state("feature", tip_sha="abc") is None


def test_plan_clean_lists_what_clean_branch_then_does(repo_with_worktree, tmp_path, monkeypatch):
    repo, worktree_path = repo_with_worktree
    main_path = Path(repo.working_tree_dir).resolve()  # ty: ignore
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    sessions = tmp_path / "home" / ".claude" / "projects" / re.sub(r"[^a-zA-Z0-9]", "-", str(worktree_path))
    sessions.mkdir(parents=True)
    (sessions / "session.jsonl").write_text("{}")
    _add_share_data_symlinks(worktree_path, tmp_path / "shared")
    (worktree_path / "workbench").unlink()
    (worktree_path / "workbench").mkdir()
    (worktree_path / "workbench" / "notes.txt").write_text("keep me")

    to_clean, to_skip = plan_clean(["feature"], {"feature": worktree_path}, {"feature": False}, main_path)

    # ai/ is a --share-data symlink, so there's nothing to copy from it.
    assert to_skip == {}
    assert to_clean == {
        "feature": [
            "copy its Claude sessions to the main repo, so they stay resumable",
            f"copy its workbench/ folder to {main_path / 'workbench' / 'feature'}",
            f"remove the worktree {worktree_path}",
            "delete the local branch 'feature'",
        ]
    }

    clean_branch(
        repo=repo,
        branch="feature",
        worktree_path=worktree_path,
        main_project_dir=tmp_path / "claude_projects",
        main_worktree_path=main_path,
    )
    assert (tmp_path / "claude_projects" / "session.jsonl").exists()
    assert (main_path / "workbench" / "feature" / "notes.txt").read_text() == "keep me"
    assert not worktree_path.exists()
    assert "feature" not in [b.name for b in repo.branches]


def test_plan_clean_skips_dirty_worktrees_and_the_main_repo_branch(tmp_path):
    main_path = tmp_path / "main"
    worktrees = {"dirty": tmp_path / "wt-dirty", "in-main": main_path}

    to_clean, to_skip = plan_clean(["dirty", "in-main", "no-worktree"], worktrees, {"dirty": True}, main_path)

    assert to_clean == {"no-worktree": ["delete the local branch 'no-worktree'"]}
    assert to_skip == {
        "dirty": f"its worktree {tmp_path / 'wt-dirty'} has uncommitted changes",
        "in-main": "it's checked out in the main repo",
    }


def test_plan_clean_worktree_folder_already_gone(tmp_path):
    gone = tmp_path / "wt-gone"
    to_clean, _ = plan_clean(["feature"], {"feature": gone}, {}, tmp_path / "main")
    assert to_clean["feature"] == [
        f"unregister the worktree {gone}, whose folder is already gone",
        "delete the local branch 'feature'",
    ]


def test_format_clean_plan():
    text = format_clean_plan(
        {"a": ["remove the worktree /wt/a", "delete the local branch 'a'"]},
        {"b": "its worktree /wt/b has uncommitted changes"},
        {"a": "merged", "b": "closed"},
    )
    assert text == (
        "pr-clean will clean 1 branch(es):\n"
        "\n"
        "  a  [merged]\n"
        "    - remove the worktree /wt/a\n"
        "    - delete the local branch 'a'\n"
        "\n"
        "It will skip 1 branch(es):\n"
        "  b  [closed]: its worktree /wt/b has uncommitted changes"
    )


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


def _trash_dir(repo: Repo) -> Path:
    return Path(repo.git_dir) / "worktree-trash"


def test_remove_worktree_renames_then_deletes(repo_with_worktree):
    repo, worktree_path = repo_with_worktree
    # A leftover from an interrupted earlier run is cleaned up too.
    (_trash_dir(repo) / "stale").mkdir(parents=True)

    remove_worktree(repo, worktree_path)

    assert not worktree_path.exists()
    assert str(worktree_path) not in repo.git.worktree("list", "--porcelain")
    assert not _trash_dir(repo).exists()


def test_remove_worktree_keeps_symlink_targets(repo_with_worktree, tmp_path):
    repo, worktree_path = repo_with_worktree
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "keep.txt").write_text("shared")
    (worktree_path / "data").symlink_to(shared)

    remove_worktree(repo, worktree_path)

    assert not worktree_path.exists()
    assert (shared / "keep.txt").read_text() == "shared"


@pytest.mark.parametrize("setup", ["dirty", "locked", "submodule", "replaced"])
def test_remove_worktree_leaves_refused_worktree_in_place(repo_with_worktree, setup):
    """Where `git worktree remove` would refuse, it is still git that decides, so nothing is renamed."""
    repo, worktree_path = repo_with_worktree
    if setup == "replaced":
        # The worktree was deleted by hand and a separate, clean repo now sits at its path.
        shutil.rmtree(worktree_path)
        other = Repo.init(worktree_path, initial_branch="master")
        _set_identity(other)
        _commit_file(other, "a.txt", "unrelated", "unrelated commit")
        assert worktree_blocking_changes(worktree_path) == []
    elif setup == "dirty":
        (worktree_path / "a.txt").write_text("uncommitted edit")
    elif setup == "locked":
        repo.git.worktree("lock", str(worktree_path))
    else:
        # A checked-out submodule, committed, so `git status` is clean but git still refuses.
        sub = Repo.init(worktree_path / "sub")
        _set_identity(sub)
        _commit_file(sub, "s.txt", "sub", "submodule commit")
        worktree = Repo(worktree_path)
        worktree.git.add("sub")
        worktree.git.commit("-m", "add submodule")
        assert worktree_blocking_changes(worktree_path) == []

    with pytest.raises(GitCommandError):
        remove_worktree(repo, worktree_path)

    assert (worktree_path / "a.txt").exists()
    assert str(worktree_path) in repo.git.worktree("list", "--porcelain")
