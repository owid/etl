import json

from apps.owidbot.grapher import make_screenshots_line, read_screenshots_status

BRANCH = "some-branch"
SHA = "0123456789abcdef0123456789abcdef01234567"
COMPARE_URL = f"https://github.com/owid/site-screenshots/compare/{BRANCH}"


def _status(**overrides) -> dict:
    return {
        "status": "ok",
        "branch": BRANCH,
        "grapherCommit": SHA,
        "finishedAt": "2026-08-21T08:00:00+00:00",
    } | overrides


def test_no_status_file_leaves_the_link_alone():
    # An ops host that predates the status file, or a run that hasn't happened
    assert make_screenshots_line(None, branch=BRANCH, head_sha=SHA) == COMPARE_URL


def test_successful_run_is_marked():
    # No changedPages: an ops host that predates the field, so the run is all we know
    assert make_screenshots_line(_status(), branch=BRANCH, head_sha=SHA) == f"✅ {COMPARE_URL}"


def test_a_run_that_found_nothing_says_so():
    line = make_screenshots_line(_status(changedPages=[]), branch=BRANCH, head_sha=SHA)
    assert line == f"✅ no changes: {COMPARE_URL}"


def test_changed_pages_are_worth_a_look():
    # The case the plain tick used to hide: this branch repaints the site
    status = _status(changedPages=["energy", "homepage", "life-expectancy-page"])
    line = make_screenshots_line(status, branch=BRANCH, head_sha=SHA)
    assert line == f"⚠️ 3 pages changed: {COMPARE_URL}"


def test_one_changed_page_is_singular():
    line = make_screenshots_line(_status(changedPages=["energy"]), branch=BRANCH, head_sha=SHA)
    assert line == f"⚠️ 1 page changed: {COMPARE_URL}"


def test_failed_run_says_the_link_is_stale():
    line = make_screenshots_line(_status(status="failed"), branch=BRANCH, head_sha=SHA)
    assert line.startswith("❌")
    assert "previous run" in line


def test_status_from_another_branch_is_not_ours():
    # One status file serves every branch: they share the working copy on lxc-manager-1
    line = make_screenshots_line(_status(branch="other-branch"), branch=BRANCH, head_sha=SHA)
    assert line.startswith("⏳")


def test_status_from_an_earlier_commit_is_not_ours():
    line = make_screenshots_line(_status(grapherCommit="f" * 40), branch=BRANCH, head_sha=SHA)
    assert line.startswith("⏳")
    assert SHA[:7] in line


def test_missing_head_sha_falls_back_to_the_branch():
    # The first owidbot run of a build has no head_sha to compare against
    assert make_screenshots_line(_status(), branch=BRANCH, head_sha=None) == f"✅ {COMPARE_URL}"


def test_unreadable_status_is_its_own_state(tmp_path):
    path = tmp_path / "site-screenshots-status.json"
    path.write_text("{ truncated mid-write")
    status = read_screenshots_status(path)
    assert status == {"status": "unreadable"}
    assert make_screenshots_line(status, branch=BRANCH, head_sha=SHA).startswith("⚠️")


def test_absent_file_reads_as_none(tmp_path):
    assert read_screenshots_status(tmp_path / "site-screenshots-status.json") is None


def test_status_written_by_the_ops_script_parses(tmp_path):
    # The shape templates/lxc-manager/site-screenshots writes
    path = tmp_path / "site-screenshots-status.json"
    path.write_text(
        json.dumps(
            {
                "status": "ok",
                "branch": BRANCH,
                "grapherCommit": SHA,
                "finishedAt": "2026-08-21T08:00:00+00:00",
                "changedPages": ["energy"],
            }
        )
    )
    line = make_screenshots_line(read_screenshots_status(path), branch=BRANCH, head_sha=SHA)
    assert line == f"⚠️ 1 page changed: {COMPARE_URL}"


def test_a_failed_run_carries_no_changed_pages(tmp_path):
    # The ops script leaves the field out rather than sending an empty list, which would
    # claim the branch changes nothing
    path = tmp_path / "site-screenshots-status.json"
    path.write_text(
        json.dumps(
            {
                "status": "failed",
                "branch": BRANCH,
                "grapherCommit": SHA,
                "finishedAt": "2026-08-21T08:00:00+00:00",
            }
        )
    )
    assert make_screenshots_line(read_screenshots_status(path), branch=BRANCH, head_sha=SHA).startswith("❌")
