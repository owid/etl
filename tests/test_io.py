from unittest.mock import patch

from etl.io import get_all_changed_catalog_paths


@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_directly_changed_export_step(mock_load_dag):
    """A branch that only edits an export recipe should still select that export step.

    Such files live under etl/steps/export/, so they are neither data nor snapshot catalog
    paths. Without include_export they're dropped; with include_export their export:// URI is
    returned even though no data step changed (so dataset_catalog_paths is empty and the DAG
    subgraph is never consulted).
    """
    files_changed = {"etl/steps/export/multidim/un/latest/un_wpp.py": "M"}

    # Default: export steps are excluded, so an export-only change selects nothing.
    assert get_all_changed_catalog_paths(files_changed) == []

    # With include_export, the directly-changed export step is returned by its full URI.
    # load_dag is not even reached here (no data steps), but patch it to keep the test hermetic.
    mock_load_dag.return_value = {}
    assert get_all_changed_catalog_paths(files_changed, include_export=True) == ["export://multidim/un/latest/un_wpp"]


@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_collection_subconfig(mock_load_dag):
    """A collection sub-config maps to its parent `<short>` export step, not a phantom step.

    The democracy explorer is built by `democracy.py` from companion configs like
    `democracy.eiu.config.yml`. Editing only a sub-config must select the real
    `export://explorers/democracy/latest/democracy` step; naive suffix-stripping would invent a
    nonexistent `...democracy.eiu` step and make the staging deploy fail with "No steps matched".
    Resolution relies on the sibling `democracy.py` recipe existing on disk.
    """
    mock_load_dag.return_value = {}
    files_changed = {"etl/steps/export/explorers/democracy/latest/democracy.eiu.config.yml": "M"}
    assert get_all_changed_catalog_paths(files_changed, include_export=True) == [
        "export://explorers/democracy/latest/democracy"
    ]


@patch("etl.io.filter_to_subgraph")
@patch("etl.io.load_dag")
def test_get_all_changed_catalog_paths_downstream_and_direct_export_deduped(mock_load_dag, mock_filter_to_subgraph):
    """Downstream and directly-changed export steps are merged and deduped under include_export."""
    files_changed = {
        # A changed data step whose downstream subgraph includes an export step.
        "etl/steps/data/garden/un/latest/un_wpp.py": "M",
        # A directly-changed export step that also appears downstream (should not be duplicated).
        "etl/steps/export/multidim/un/latest/un_wpp.py": "M",
    }
    mock_load_dag.return_value = {}
    # Pretend the downstream subgraph contains the data step plus two export steps.
    mock_filter_to_subgraph.return_value = {
        "data://garden/un/latest/un_wpp": set(),
        "export://multidim/un/latest/un_wpp": set(),
        "export://explorers/un/latest/un_wpp": set(),
    }

    result = get_all_changed_catalog_paths(files_changed, include_export=True)

    # Data step returned URI-less; both export steps present exactly once.
    assert result.count("export://multidim/un/latest/un_wpp") == 1
    assert set(result) == {
        "garden/un/latest/un_wpp",
        "export://multidim/un/latest/un_wpp",
        "export://explorers/un/latest/un_wpp",
    }

    # Without include_export, only the data catalog path is returned.
    result_no_export = get_all_changed_catalog_paths(files_changed)
    assert result_no_export == ["garden/un/latest/un_wpp"]
