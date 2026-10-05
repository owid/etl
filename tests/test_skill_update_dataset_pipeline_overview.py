"""Guards for the `update-dataset` skill's `pipeline_overview.py` script.

Nothing under `.claude/` is linted or collected by CI, so these tests pin the DAG logic the
refresher depends on: which steps count as the chain, which as siblings, and which stay outside.
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
    assert po.step_key("snapshot://wb/2026-07-01/income_groups.xlsx") == ("wb", "2026-07-01", "income_groups")
    assert po.step_key("snapshot://war/2025-06-13/ucdp_ged.zip") == ("war", "2025-06-13", "ucdp_ged")
    assert po.step_key("data://garden/wb/2026-07-01/income_groups") == ("wb", "2026-07-01", "income_groups")
    assert po.step_key("viz://chart/animal_welfare/latest/banning") == ("animal_welfare", "latest", "banning")
    assert po.step_key("data://external/owid_grapher/latest") is None


def test_upstream_members_cross_version_chain_and_siblings():
    dag = {
        # Old chains mix versions: a 2023-01-18 garden on a 2023-01-10 meadow on a 2023-01-09 snapshot.
        "data://grapher/war/2023-01-18/clodfelter": {"data://garden/war/2023-01-18/clodfelter"},
        "data://garden/war/2023-01-18/clodfelter": {
            "data://meadow/war/2023-01-10/clodfelter",
            "data://garden/war/2023-01-18/helper",
            "data://garden/demography/2024-07-15/population",
        },
        "data://meadow/war/2023-01-10/clodfelter": {"snapshot://war/2023-01-09/clodfelter.csv"},
        # Same-version helper with its own extra snapshot.
        "data://garden/war/2023-01-18/helper": {"snapshot://war/2023-01-18/extra.csv"},
        # A different dataset in the namespace at another version: not part of the chain.
        "data://garden/war/2024-01-01/other": {"data://garden/war/2023-01-18/clodfelter"},
    }
    seeds = [s for s in po.graph_nodes(dag) if po.step_key(s) == ("war", "2023-01-18", "clodfelter")]
    members = po.find_upstream_members(dag, seeds, "war", "2023-01-18", "clodfelter")

    assert members == {
        "data://grapher/war/2023-01-18/clodfelter",
        "data://garden/war/2023-01-18/clodfelter",
        "data://meadow/war/2023-01-10/clodfelter",
        "snapshot://war/2023-01-09/clodfelter.csv",
        "data://garden/war/2023-01-18/helper",
        "snapshot://war/2023-01-18/extra.csv",
    }
    assert sorted(members, key=po.stage_order)[0].startswith("snapshot://")
