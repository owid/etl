"""Tests for snapshot_command module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.exceptions import ClickException

from etl.snapshot_command import check_for_version_ambiguity


class TestCheckForVersionAmbiguity:
    """Tests for the check_for_version_ambiguity function."""

    @patch("etl.snapshot_command.paths")
    def test_no_ambiguity_with_full_path(self, mock_paths):
        """Full path (namespace/version/short_name) should not check for ambiguity."""
        # Full path with 3 parts should return early without checking
        check_for_version_ambiguity("benchmark_mineral_intelligence/2024-11-29/battery_cell_prices")
        # No exception raised, test passes

    @patch("etl.snapshot_command.paths")
    def test_no_ambiguity_single_version(self, mock_paths):
        """No error when only one version exists."""
        # Mock the SNAPSHOTS_DIR.glob to return a single file
        mock_snapshots_dir = MagicMock()
        mock_paths.SNAPSHOTS_DIR = mock_snapshots_dir

        mock_file = MagicMock()
        mock_file.relative_to.return_value = Path(
            "benchmark_mineral_intelligence/2024-11-29/battery_cell_prices.xlsx.dvc"
        )
        mock_file.stem = "battery_cell_prices.xlsx"
        mock_file.parts = ("benchmark_mineral_intelligence", "2024-11-29", "battery_cell_prices.xlsx.dvc")

        mock_snapshots_dir.glob.return_value = [mock_file]

        # Should not raise an exception
        check_for_version_ambiguity("battery_cell_prices")

    @patch("etl.snapshot_command.paths")
    def test_ambiguity_with_multiple_versions(self, mock_paths):
        """Should raise error when multiple versions exist for the same snapshot."""
        # Mock the SNAPSHOTS_DIR.glob to return multiple files
        mock_snapshots_dir = MagicMock()
        mock_paths.SNAPSHOTS_DIR = mock_snapshots_dir

        # Create two mock DVC files for different versions
        mock_file1 = MagicMock()
        mock_file1.relative_to.return_value = Path(
            "benchmark_mineral_intelligence/2024-11-29/battery_cell_prices.xlsx.dvc"
        )
        mock_file1.stem = "battery_cell_prices.xlsx"
        mock_file1.parts = ("benchmark_mineral_intelligence", "2024-11-29", "battery_cell_prices.xlsx.dvc")

        mock_file2 = MagicMock()
        mock_file2.relative_to.return_value = Path(
            "benchmark_mineral_intelligence/2025-12-02/battery_cell_prices.xlsx.dvc"
        )
        mock_file2.stem = "battery_cell_prices.xlsx"
        mock_file2.parts = ("benchmark_mineral_intelligence", "2025-12-02", "battery_cell_prices.xlsx.dvc")

        mock_snapshots_dir.glob.return_value = [mock_file1, mock_file2]

        # Should raise ClickException
        with pytest.raises(ClickException) as exc_info:
            check_for_version_ambiguity("battery_cell_prices")

        assert "Multiple snapshot versions found" in str(exc_info.value)
        assert "Please specify the full path" in str(exc_info.value)

    @patch("etl.snapshot_command.paths")
    def test_ambiguity_with_extension_in_name(self, mock_paths):
        """Should handle dataset names with file extensions."""
        # Mock the SNAPSHOTS_DIR.glob to return multiple files
        mock_snapshots_dir = MagicMock()
        mock_paths.SNAPSHOTS_DIR = mock_snapshots_dir

        # Create two mock DVC files for different versions
        mock_file1 = MagicMock()
        mock_file1.relative_to.return_value = Path(
            "benchmark_mineral_intelligence/2024-11-29/battery_cell_prices.xlsx.dvc"
        )
        mock_file1.stem = "battery_cell_prices.xlsx"
        mock_file1.parts = ("benchmark_mineral_intelligence", "2024-11-29", "battery_cell_prices.xlsx.dvc")

        mock_file2 = MagicMock()
        mock_file2.relative_to.return_value = Path(
            "benchmark_mineral_intelligence/2025-12-02/battery_cell_prices.xlsx.dvc"
        )
        mock_file2.stem = "battery_cell_prices.xlsx"
        mock_file2.parts = ("benchmark_mineral_intelligence", "2025-12-02", "battery_cell_prices.xlsx.dvc")

        mock_snapshots_dir.glob.return_value = [mock_file1, mock_file2]

        # Should raise ClickException even when extension is included
        with pytest.raises(ClickException) as exc_info:
            check_for_version_ambiguity("battery_cell_prices.xlsx")

        assert "Multiple snapshot versions found" in str(exc_info.value)

    @patch("etl.snapshot_command.paths")
    def test_no_ambiguity_different_snapshots(self, mock_paths):
        """No error when multiple files exist but for different snapshots."""
        # Mock the SNAPSHOTS_DIR.glob to return files for different snapshots
        mock_snapshots_dir = MagicMock()
        mock_paths.SNAPSHOTS_DIR = mock_snapshots_dir

        # Create mock DVC files for different snapshots (not different versions of the same)
        mock_file1 = MagicMock()
        mock_file1.relative_to.return_value = Path(
            "benchmark_mineral_intelligence/2024-11-29/battery_cell_prices.xlsx.dvc"
        )
        mock_file1.stem = "battery_cell_prices.xlsx"
        mock_file1.parts = ("benchmark_mineral_intelligence", "2024-11-29", "battery_cell_prices.xlsx.dvc")

        mock_file2 = MagicMock()
        mock_file2.relative_to.return_value = Path(
            "benchmark_mineral_intelligence/2024-11-29/battery_cell_prices_by_chemistry.xlsx.dvc"
        )
        mock_file2.stem = "battery_cell_prices_by_chemistry.xlsx"
        mock_file2.parts = ("benchmark_mineral_intelligence", "2024-11-29", "battery_cell_prices_by_chemistry.xlsx.dvc")

        mock_snapshots_dir.glob.return_value = [mock_file1, mock_file2]

        # Should not raise an exception (they are different snapshots, same version)
        check_for_version_ambiguity("battery_cell_prices")

    @patch("etl.snapshot_command.paths")
    def test_ambiguity_with_partial_path(self, mock_paths):
        """Should detect ambiguity with partial path (version/short_name)."""
        # Mock the SNAPSHOTS_DIR.glob to return multiple files in different namespaces
        mock_snapshots_dir = MagicMock()
        mock_paths.SNAPSHOTS_DIR = mock_snapshots_dir

        # Create mock DVC files in different namespaces but same version
        mock_file1 = MagicMock()
        mock_file1.relative_to.return_value = Path("namespace1/2024-11-29/dataset.csv.dvc")
        mock_file1.stem = "dataset.csv"
        mock_file1.parts = ("namespace1", "2024-11-29", "dataset.csv.dvc")

        mock_file2 = MagicMock()
        mock_file2.relative_to.return_value = Path("namespace2/2024-11-29/dataset.csv.dvc")
        mock_file2.stem = "dataset.csv"
        mock_file2.parts = ("namespace2", "2024-11-29", "dataset.csv.dvc")

        mock_snapshots_dir.glob.return_value = [mock_file1, mock_file2]

        # This should NOT raise an error because they're in different namespaces
        # (they're different snapshots, not different versions of the same snapshot)
        check_for_version_ambiguity("2024-11-29/dataset")
