from unittest.mock import patch

from etl.io import get_all_changed_catalog_paths


@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_directly_changed_viz_step(mock_load_dag):
    """A branch that only edits a chart recipe should still select that viz step.

    Such files live under etl/steps/viz/, so they are neither data nor snapshot catalog
    paths. Without include_export they're dropped; with include_export their viz:// URI is
    returned even though no data step changed (so dataset_catalog_paths is empty and the DAG
    subgraph is never consulted).
    """
    files_changed = {"etl/steps/viz/chart/un/latest/un_wpp.py": "M"}

    # Default: viz steps are excluded, so a viz-only change selects nothing.
    assert get_all_changed_catalog_paths(files_changed) == []

    # With include_export, the directly-changed viz step is returned by its full URI.
    # load_dag is not even reached here (no data steps), but patch it to keep the test hermetic.
    mock_load_dag.return_value = {}
    assert get_all_changed_catalog_paths(files_changed, include_export=True) == ["viz://chart/un/latest/un_wpp"]


@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_yaml_only_chart_and_export_steps(mock_load_dag):
    """A YAML-only chart (`.config.yml`, no `.py`) and an export:// recipe both resolve to their step URI."""
    mock_load_dag.return_value = {}
    files_changed = {
        "etl/steps/viz/chart/animal_welfare/latest/banning_of_chick_culling.config.yml": "A",
        "etl/steps/export/github/co2_data/latest/owid_co2.py": "M",
    }
    assert get_all_changed_catalog_paths(files_changed, include_export=True) == [
        "viz://chart/animal_welfare/latest/banning_of_chick_culling",
        "export://github/co2_data/latest/owid_co2",
    ]


@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_collection_subconfig(mock_load_dag):
    """A collection sub-config maps to its parent `<short>` explorer step, not a phantom step.

    The democracy explorer is built by `democracy.py` from companion configs like
    `democracy.eiu.config.yml`. Editing only a sub-config must select the real
    `viz://explorer/democracy/latest/democracy` step; naive suffix-stripping would invent a
    nonexistent `...democracy.eiu` step and make the staging deploy fail with "No steps matched".
    Resolution relies on the sibling `democracy.py` recipe existing on disk.
    """
    mock_load_dag.return_value = {}
    files_changed = {"etl/steps/viz/explorer/democracy/latest/democracy.eiu.config.yml": "M"}
    assert get_all_changed_catalog_paths(files_changed, include_export=True) == [
        "viz://explorer/democracy/latest/democracy"
    ]


@patch("etl.io.filter_to_subgraph")
@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_downstream_and_direct_export_deduped(mock_load_dag, mock_filter_to_subgraph):
    """Downstream and directly-changed viz steps are merged and deduped under include_export."""
    files_changed = {
        # A changed data step whose downstream subgraph includes a viz step.
        "etl/steps/data/garden/un/latest/un_wpp.py": "M",
        # A directly-changed viz step that also appears downstream (should not be duplicated).
        "etl/steps/viz/chart/un/latest/un_wpp.py": "M",
    }
    mock_load_dag.return_value = {}
    # Pretend the downstream subgraph contains the data step plus two viz steps.
    mock_filter_to_subgraph.return_value = {
        "data://garden/un/latest/un_wpp": set(),
        "viz://chart/un/latest/un_wpp": set(),
        "viz://explorer/un/latest/un_wpp": set(),
    }

    result = get_all_changed_catalog_paths(files_changed, include_export=True)

    # Data step returned URI-less; both viz steps present exactly once.
    assert result.count("viz://chart/un/latest/un_wpp") == 1
    assert set(result) == {
        "garden/un/latest/un_wpp",
        "viz://chart/un/latest/un_wpp",
        "viz://explorer/un/latest/un_wpp",
    }

    # Without include_export, only the data catalog path is returned.
    result_no_export = get_all_changed_catalog_paths(files_changed)
    assert result_no_export == ["garden/un/latest/un_wpp"]


@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_version_bump_includes_old_version(mock_load_dag):
    """A version bump must also surface the *previous* version's catalog path.

    Only the new version's files changed, so naively `dataset_catalog_paths` would contain just
    the new path. Callers (e.g. datadiff's `--changed`) turn this list into an --include filter, so
    if the old version's path isn't included, it gets filtered out of the REMOTE-catalog fetch and
    the diff tool reports the bump as a brand-new dataset instead of comparing against the old one.
    """
    mock_load_dag.return_value = {
        "data://garden/worldbank_wdi/2026-07-14/wdi": {"data://meadow/worldbank_wdi/2026-07-14/wdi"},
        "data://meadow/worldbank_wdi/2026-07-14/wdi": set(),
        "data://garden/worldbank_wdi/2026-02-27/wdi": {"data://meadow/worldbank_wdi/2026-02-27/wdi"},
        "data://meadow/worldbank_wdi/2026-02-27/wdi": set(),
        # An unrelated dataset that happens to share the short_name in a different namespace —
        # must NOT be pulled in as a sibling version.
        "data://garden/other_namespace/2026-01-01/wdi": set(),
    }
    files_changed = {"etl/steps/data/garden/worldbank_wdi/2026-07-14/wdi.py": "M"}

    result = get_all_changed_catalog_paths(files_changed)

    assert "garden/worldbank_wdi/2026-07-14/wdi" in result
    assert "garden/worldbank_wdi/2026-02-27/wdi" in result
    assert "garden/other_namespace/2026-01-01/wdi" not in result


@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_only_pulls_in_closest_preceding_sibling(mock_load_dag):
    """A dataset with several *independently* active vintages (e.g. WDI, which keeps older
    versions in production for other downstream consumers instead of superseding them in place)
    must only pull in its closest predecessor as a comparison sibling — not every other active
    version.

    Sweeping in all of them would put datasets that aren't part of this change, and usually
    aren't built locally either, in scope for `etl diff`'s --changed comparison, which then
    reports each one as falsely "removed".
    """
    mock_load_dag.return_value = {
        "data://garden/worldbank_wdi/2025-01-24/wdi": set(),
        "data://garden/worldbank_wdi/2025-09-08/wdi": set(),
        "data://garden/worldbank_wdi/2026-01-29/wdi": set(),
        "data://garden/worldbank_wdi/2026-02-27/wdi": set(),
        "data://garden/worldbank_wdi/2026-07-14/wdi": {"data://meadow/worldbank_wdi/2026-07-14/wdi"},
        "data://meadow/worldbank_wdi/2026-07-14/wdi": set(),
    }
    files_changed = {"etl/steps/data/garden/worldbank_wdi/2026-07-14/wdi.py": "M"}

    result = get_all_changed_catalog_paths(files_changed)

    assert "garden/worldbank_wdi/2026-07-14/wdi" in result
    assert "garden/worldbank_wdi/2026-02-27/wdi" in result
    assert "garden/worldbank_wdi/2025-01-24/wdi" not in result
    assert "garden/worldbank_wdi/2025-09-08/wdi" not in result
    assert "garden/worldbank_wdi/2026-01-29/wdi" not in result


@patch("etl.io.get_file_at_merge_base")
@patch("etl.io.load_single_dag_file")
def test_get_dag_dependency_changed_steps_repoint(mock_load_single, mock_merge_base):
    """A dag-only dependency edit (repoint) must select the repointed step — and only it.

    This is the WDI 2026-07 follow-up shape: consumers were repointed from one WDI version to
    another with no change to any file under etl/steps/, so file-based selection saw nothing,
    staging skipped the rebuilds, and six consumers' value changes reached neither chart-diff
    nor datadiff.
    """
    from etl.io import get_dag_dependency_changed_steps

    # Current branch state: consumer repointed to 2026-07-27; bystander untouched.
    mock_load_single.return_value = {
        "data://garden/malnutrition/2024-12-16/malnutrition": {"data://garden/worldbank_wdi/2026-07-27/wdi"},
        "data://garden/technology/2024-12-23/internet": {"data://garden/demography/2024-07-15/population"},
    }
    # Merge-base state of the same dag file, in the nested form dag files actually use.
    mock_merge_base.return_value = """
steps:
  data://garden/malnutrition/2024-12-16/malnutrition:
    - data://garden/worldbank_wdi/2026-07-14/wdi
  data://garden/technology/2024-12-23/internet:
    - data://garden/demography/2024-07-15/population
"""

    changed = get_dag_dependency_changed_steps({"dag/main.yml": {"status": "M", "diff": ""}})
    assert changed == {"data://garden/malnutrition/2024-12-16/malnutrition"}

    # dag/archive is a generated record — never a selection source.
    assert get_dag_dependency_changed_steps({"dag/archive/main.yml": "M"}) == set()
    # Deleted dag files select nothing.
    assert get_dag_dependency_changed_steps({"dag/main.yml": "D"}) == set()


@patch("etl.io.get_file_at_merge_base")
@patch("etl.io.load_single_dag_file")
@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_dag_only_dependency_change(mock_load_dag, mock_load_single, mock_merge_base):
    """A repoint-only branch must select the repointed consumer and its downstream steps,
    even though no file under etl/steps/ changed."""
    mock_load_dag.return_value = {
        "data://garden/worldbank_wdi/2026-07-27/wdi": set(),
        "data://garden/malnutrition/2024-12-16/malnutrition": {"data://garden/worldbank_wdi/2026-07-27/wdi"},
        "data://grapher/malnutrition/2024-12-16/malnutrition": {"data://garden/malnutrition/2024-12-16/malnutrition"},
    }
    mock_load_single.return_value = {
        "data://garden/malnutrition/2024-12-16/malnutrition": {"data://garden/worldbank_wdi/2026-07-27/wdi"},
    }
    mock_merge_base.return_value = """
steps:
  data://garden/malnutrition/2024-12-16/malnutrition:
    - data://garden/worldbank_wdi/2026-07-14/wdi
"""

    result = get_all_changed_catalog_paths({"dag/main.yml": {"status": "M", "diff": ""}})

    assert "garden/malnutrition/2024-12-16/malnutrition" in result
    # Downstream grapher step is swept in via the subgraph expansion.
    assert "grapher/malnutrition/2024-12-16/malnutrition" in result
    # (The repoint target also appears, as upstream deps always do in the subgraph expansion —
    # harmless, since an unchanged dataset diffs as identical.)


@patch("etl.io.get_file_at_merge_base")
@patch("etl.io.load_single_dag_file")
@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_private_dag_only_dependency_change(
    mock_load_dag, mock_load_single, mock_merge_base
):
    """A repoint of a data-private:// consumer must survive into the returned catalog paths.

    The final projection kept only data:// nodes, so a private-only repoint returned an empty
    list — `etl run --modified --private` then reported no modified steps and skipped the
    rebuild, and chart-diff/datadiff filtered the private steps out as spurious.
    """
    mock_load_dag.return_value = {
        "data-private://garden/worldbank_wdi/2026-07-27/wdi": set(),
        "data-private://garden/malnutrition/2024-12-16/malnutrition": {
            "data-private://garden/worldbank_wdi/2026-07-27/wdi"
        },
        "data-private://grapher/malnutrition/2024-12-16/malnutrition": {
            "data-private://garden/malnutrition/2024-12-16/malnutrition"
        },
    }
    mock_load_single.return_value = {
        "data-private://garden/malnutrition/2024-12-16/malnutrition": {
            "data-private://garden/worldbank_wdi/2026-07-27/wdi"
        },
    }
    mock_merge_base.return_value = """
steps:
  data-private://garden/malnutrition/2024-12-16/malnutrition:
    - data-private://garden/worldbank_wdi/2026-07-14/wdi
"""

    result = get_all_changed_catalog_paths({"dag/main.yml": {"status": "M", "diff": ""}})

    # The repointed private consumer and its private downstream step are both returned, URI-less
    # like their public counterparts.
    assert "garden/malnutrition/2024-12-16/malnutrition" in result
    assert "grapher/malnutrition/2024-12-16/malnutrition" in result


@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_skips_deleted_files(mock_load_dag):
    """A deleted or moved-away recipe must not produce a phantom step, whatever shape the status takes."""
    mock_load_dag.return_value = {}
    files_changed = {
        "etl/steps/export/explorers/who/latest/influenza.py": {"status": "D", "diff": ""},
        "etl/steps/data/garden/who/2024-01-01/influenza.py": {"status": "D", "diff": ""},
        "etl/steps/viz/explorer/who/latest/influenza.py": {"status": "A", "diff": ""},
    }
    assert get_all_changed_catalog_paths(files_changed, include_export=True) == ["viz://explorer/who/latest/influenza"]
