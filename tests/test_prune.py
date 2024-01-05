import json
from pathlib import Path
from unittest import mock

import pytest

from etl.prune import prune


@pytest.mark.skip(reason="Takes too long to run")
def test_prune(tmp_path: Path) -> None:
    # create dag file
    dag_file = tmp_path / "dag.yml"
    dag_file.write_text("steps:\n  data://garden/owid/latest/covid:")

    index_file = tmp_path / "data/garden/owid/latest/todelete/index.json"
    index_file.parent.mkdir(exist_ok=True, parents=True)
    index_file.write_text(json.dumps({"namespace": "owid", "short_name": "todelete", "description": "test"}))

    meta_file = tmp_path / "data/garden/owid/latest/todelete/todelete.meta.json"
    meta_file.write_text(json.dumps({"short_name": "todelete"}))

    assert index_file.exists()
    assert meta_file.exists()

    with mock.patch("etl.prune.reindex_catalog"):
        prune(
            dag_path=dag_file,
            data_dir=tmp_path / "data",
        )
    assert not index_file.exists()
    assert not meta_file.exists()
