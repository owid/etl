"""Guards for the `update-dataset` skill's `pipeline_overview.py` script.

Nothing under `.claude/` is linted or collected by CI, so these tests pin the DAG logic the
refresher depends on: which steps count as the chain, which as siblings, and which stay outside.
All step URIs are fictional, so the tests never depend on a real step that a future update archives.
"""

import importlib.util
from pathlib import Path

RELATIVE = Path(".claude/skills/update-dataset/scripts/pipeline_overview.py")


def _load_module():
    for directory in [Path(__file__).resolve(), *Path(__file__).resolve().parents]:
        candidate = directory / RELATIVE
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location("pipeline_overview", candidate)
            assert spec and spec.loader
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
    raise AssertionError(f"could not locate {RELATIVE} from {__file__}")


po = _load_module()


def test_step_key():
    assert po.step_key("snapshot://example/2001-01-01/dataset.xlsx") == ("example", "2001-01-01", "dataset")
    assert po.step_key("snapshot://example/2001-01-01/dataset_part.zip") == ("example", "2001-01-01", "dataset_part")
    assert po.step_key("snapshot-private://example/2001-01-01/dataset.csv") == ("example", "2001-01-01", "dataset")
    assert po.step_key("data://garden/example/2001-01-01/dataset") == ("example", "2001-01-01", "dataset")
    assert po.step_key("viz://chart/example/latest/chart") == ("example", "latest", "chart")
    assert po.step_key("data://external/example/latest") is None


def test_upstream_members_cross_version_chain_and_siblings():
    dag = {
        # Old chains mix versions: a 2001-01-03 garden on a 2001-01-02 meadow on a 2001-01-01 snapshot.
        "data://grapher/example/2001-01-03/dataset": {"data://garden/example/2001-01-03/dataset"},
        "data://garden/example/2001-01-03/dataset": {
            "data://meadow/example/2001-01-02/dataset",
            "data://garden/example/2001-01-03/helper",
            "data://garden/elsewhere/2001-01-01/population",
        },
        "data://meadow/example/2001-01-02/dataset": {"snapshot://example/2001-01-01/dataset.csv"},
        # Same-version helper with its own extra snapshot.
        "data://garden/example/2001-01-03/helper": {"snapshot://example/2001-01-03/extra.csv"},
        # A different dataset in the namespace at another version: not part of the chain.
        "data://garden/example/2002-01-01/other": {"data://garden/example/2001-01-03/dataset"},
    }
    seeds = [s for s in po.graph_nodes(dag) if po.step_key(s) == ("example", "2001-01-03", "dataset")]
    members = po.find_upstream_members(dag, seeds, "example", "2001-01-03", "dataset")

    assert members == {
        "data://grapher/example/2001-01-03/dataset",
        "data://garden/example/2001-01-03/dataset",
        "data://meadow/example/2001-01-02/dataset",
        "snapshot://example/2001-01-01/dataset.csv",
        "data://garden/example/2001-01-03/helper",
        "snapshot://example/2001-01-03/extra.csv",
    }
    assert sorted(members, key=po.stage_order)[0].startswith("snapshot://")
