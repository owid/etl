from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def no_real_step_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the tests away from the links and metadata files of the real steps.

    The test datasets reuse the paths of real ones, so without this a test would pick up the files next to those steps.
    A test that needs such files passes its own ``steps_dir``.
    """
    empty_steps_dir = tmp_path / "no-real-steps"
    monkeypatch.setattr("etl.catalog_pages.links.STEP_DIR", empty_steps_dir)
    monkeypatch.setattr("etl.catalog_pages.artifacts.STEP_DIR", empty_steps_dir)
